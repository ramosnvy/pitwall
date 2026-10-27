using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Threading.Channels;
using Pitwall.Consumer.Pipelines;
using Pitwall.Contracts;
using Pitwall.Processing.Core;

namespace Pitwall.Consumer.Republish;

/// <summary>
/// Parametros comuns aos tres modos do segundo cenario.
/// </summary>
public sealed record RepublishOptions
{
    public required OutputKind Kind { get; init; }

    /// <summary>
    /// Direct: a cada quantas saidas a faixa para e espera todas as
    /// confirmacoes antes de receber a proxima entrada. O mesmo lote de 100 da
    /// confirmacao do consumidor RabbitMQ.
    /// </summary>
    public int ConfirmBatch { get; init; } = 100;

    /// <summary>
    /// Channels e Pipelines: saidas em voo por faixa antes de a etapa de
    /// publicacao esperar. Igual a capacidade das filas internas.
    /// </summary>
    public int MaxInFlight { get; init; } = 10_000;

    /// <summary>Capacidade das filas internas, em eventos.</summary>
    public int Capacity { get; init; } = 10_000;
}

/// <summary>
/// Direct no segundo cenario: recebe, processa e publica na mesma thread. A
/// cada <see cref="RepublishOptions.ConfirmBatch"/> saidas, espera todas as
/// confirmacoes; durante a espera, a faixa nao recebe nada (docs/APROFUNDAMENTO.md
/// secao 8.6). E o grupo de controle da hipotese H5a.
/// </summary>
public sealed class RepublishDirectPipeline : IProcessingPipeline
{
    private readonly OutputStage[] _stages;
    private readonly OutputCallback[] _emit;
    private readonly int[] _sinceWait;
    private readonly ReleaseTracker _tracker;
    private readonly IRepublisher _republisher;
    private readonly LatencyRecorder _latency;
    private readonly RepublishOptions _options;

    public RepublishDirectPipeline(int lanes, RepublishOptions options, ProcessingOptions processing,
        Action<DriverWindowStats> onWindow, LatencyRecorder latency, ReleaseTracker tracker, IRepublisher republisher)
    {
        _options = options;
        _tracker = tracker;
        _republisher = republisher;
        _latency = latency;
        _sinceWait = new int[lanes];
        _stages = new OutputStage[lanes];
        _emit = new OutputCallback[lanes];

        for (var lane = 0; lane < lanes; lane++)
        {
            var index = lane;
            _stages[lane] = new OutputStage(lane, options.Kind, processing, onWindow, tracker);
            _emit[lane] = (id, payload) =>
            {
                _tracker.MarkPublished(index, id);
                _republisher.Publish(index, payload.ToArray(), id);
                _sinceWait[index]++;
            };
        }
    }

    public string Mode => "direct";

    public bool TracksRelease => true;

    public long Released(int lane) => _tracker.Released(lane);

    public ValueTask SubmitAsync(int lane, ReadOnlySpan<byte> payload) =>
        throw new NotSupportedException("O segundo cenario precisa da posicao de entrada.");

    public ValueTask SubmitAsync(int lane, ReadOnlySpan<byte> payload, long position)
    {
        var evt = TelemetryCodec.Read(payload);

        _stages[lane].Process(evt, position, _emit[lane]);
        _latency.Record(lane, evt.PublishedTicks);

        if (_sinceWait[lane] >= _options.ConfirmBatch)
        {
            _sinceWait[lane] = 0;
            return _tracker.WaitInFlightAtMostAsync(lane, 0);
        }

        return ValueTask.CompletedTask;
    }

    public async Task CompleteAsync()
    {
        for (var lane = 0; lane < _stages.Length; lane++)
        {
            _stages[lane].Complete(_emit[lane]);
            await _tracker.WaitInFlightAtMostAsync(lane, 0);
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Channels no segundo cenario: receber, processar e publicar sao etapas
/// ligadas por canais com limite. A espera pela confirmacao fica na etapa de
/// publicacao, que so para com <see cref="RepublishOptions.MaxInFlight"/>
/// saidas em voo; enquanto isso, receber e processar continuam.
/// </summary>
public sealed class RepublishChannelsPipeline : IProcessingPipeline
{
    private readonly Channel<(TelemetryEvent Event, long Position)>[] _input;
    private readonly Channel<(byte[] Payload, long Id)>[] _output;
    private readonly Task[] _processors;
    private readonly Task[] _publishers;
    private readonly ReleaseTracker _tracker;
    private readonly IRepublisher _republisher;
    private readonly LatencyRecorder _latency;
    private readonly RepublishOptions _options;

    public RepublishChannelsPipeline(int lanes, RepublishOptions options, ProcessingOptions processing,
        Action<DriverWindowStats> onWindow, LatencyRecorder latency, ReleaseTracker tracker, IRepublisher republisher)
    {
        _options = options;
        _tracker = tracker;
        _republisher = republisher;
        _latency = latency;
        _input = new Channel<(TelemetryEvent, long)>[lanes];
        _output = new Channel<(byte[], long)>[lanes];
        _processors = new Task[lanes];
        _publishers = new Task[lanes];

        var bounded = new BoundedChannelOptions(options.Capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        };

        for (var lane = 0; lane < lanes; lane++)
        {
            _input[lane] = Channel.CreateBounded<(TelemetryEvent, long)>(bounded);
            _output[lane] = Channel.CreateBounded<(byte[], long)>(bounded);

            var index = lane;
            var stage = new OutputStage(lane, options.Kind, processing, onWindow, tracker);
            _processors[lane] = Task.Run(() => ProcessAsync(index, stage));
            _publishers[lane] = Task.Run(() => PublishAsync(index));
        }
    }

    public string Mode => "channels";

    public bool TracksRelease => true;

    public long Released(int lane) => _tracker.Released(lane);

    public ValueTask SubmitAsync(int lane, ReadOnlySpan<byte> payload) =>
        throw new NotSupportedException("O segundo cenario precisa da posicao de entrada.");

    public ValueTask SubmitAsync(int lane, ReadOnlySpan<byte> payload, long position)
    {
        var item = (TelemetryCodec.Read(payload), position);
        var writer = _input[lane].Writer;

        return writer.TryWrite(item) ? ValueTask.CompletedTask : writer.WriteAsync(item);
    }

    private async Task ProcessAsync(int lane, OutputStage stage)
    {
        // As saidas de um evento sao juntadas aqui e escritas depois, porque o
        // callback e sincrono e a escrita no canal pode precisar esperar.
        var ready = new List<(byte[], long)>(4);
        OutputCallback emit = (id, payload) => ready.Add((payload.ToArray(), id));
        var writer = _output[lane].Writer;

        await foreach (var (evt, position) in _input[lane].Reader.ReadAllAsync())
        {
            stage.Process(evt, position, emit);
            _latency.Record(lane, evt.PublishedTicks);
            await DrainAsync(ready, writer);
        }

        stage.Complete(emit);
        await DrainAsync(ready, writer);
        writer.Complete();
    }

    private static async ValueTask DrainAsync(List<(byte[], long)> ready, ChannelWriter<(byte[], long)> writer)
    {
        foreach (var item in ready)
        {
            if (!writer.TryWrite(item))
            {
                await writer.WriteAsync(item);
            }
        }

        ready.Clear();
    }

    private async Task PublishAsync(int lane)
    {
        await foreach (var (payload, id) in _output[lane].Reader.ReadAllAsync())
        {
            if (_tracker.InFlight(lane) >= _options.MaxInFlight)
            {
                await _tracker.WaitInFlightAtMostAsync(lane, _options.MaxInFlight / 2);
            }

            _tracker.MarkPublished(lane, id);
            _republisher.Publish(lane, payload, id);
        }
    }

    public async Task CompleteAsync()
    {
        foreach (var input in _input)
        {
            input.Writer.Complete();
        }

        await Task.WhenAll(_processors);
        await Task.WhenAll(_publishers);

        for (var lane = 0; lane < _input.Length; lane++)
        {
            await _tracker.WaitInFlightAtMostAsync(lane, 0);
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Pipelines no segundo cenario: a entrada chega como bytes num Pipe (posicao
/// de 8 bytes mais o evento de 46), a etapa de processamento escreve a saida
/// serializada num segundo Pipe, sem criar objeto por mensagem, e a etapa de
/// publicacao le esse Pipe em blocos. A espera pela confirmacao fica na
/// publicacao, como no Channels.
/// </summary>
public sealed class RepublishPipelinesPipeline : IProcessingPipeline
{
    private const int InputRecord = 8 + TelemetryCodec.Size;

    private readonly Pipe[] _input;
    private readonly Pipe[] _output;
    private readonly Task[] _processors;
    private readonly Task[] _publishers;
    private readonly ReleaseTracker _tracker;
    private readonly IRepublisher _republisher;
    private readonly LatencyRecorder _latency;
    private readonly RepublishOptions _options;
    private readonly int _outputRecord;
    private long _incompleteRecords;

    public RepublishPipelinesPipeline(int lanes, RepublishOptions options, ProcessingOptions processing,
        Action<DriverWindowStats> onWindow, LatencyRecorder latency, ReleaseTracker tracker, IRepublisher republisher)
    {
        _options = options;
        _tracker = tracker;
        _republisher = republisher;
        _latency = latency;
        _input = new Pipe[lanes];
        _output = new Pipe[lanes];
        _processors = new Task[lanes];
        _publishers = new Task[lanes];

        var outputSize = options.Kind == OutputKind.Enriquecida ? OutputCodec.EnrichedSize : OutputCodec.SummarySize;
        _outputRecord = 8 + outputSize;

        for (var lane = 0; lane < lanes; lane++)
        {
            // Mesma contrapressao, em eventos, que os canais do modo Channels.
            var inputPause = options.Capacity * InputRecord;
            var outputPause = options.Capacity * _outputRecord;

            _input[lane] = new Pipe(new PipeOptions(
                pauseWriterThreshold: inputPause, resumeWriterThreshold: inputPause / 2, useSynchronizationContext: false));
            _output[lane] = new Pipe(new PipeOptions(
                pauseWriterThreshold: outputPause, resumeWriterThreshold: outputPause / 2, useSynchronizationContext: false));

            var index = lane;
            var stage = new OutputStage(lane, options.Kind, processing, onWindow, tracker);
            _processors[lane] = Task.Run(() => ProcessAsync(index, stage));
            _publishers[lane] = Task.Run(() => PublishAsync(index));
        }
    }

    public string Mode => "pipelines";

    public bool TracksRelease => true;

    public long Released(int lane) => _tracker.Released(lane);

    public long IncompleteRecords => Interlocked.Read(ref _incompleteRecords);

    public ValueTask SubmitAsync(int lane, ReadOnlySpan<byte> payload) =>
        throw new NotSupportedException("O segundo cenario precisa da posicao de entrada.");

    public ValueTask SubmitAsync(int lane, ReadOnlySpan<byte> payload, long position)
    {
        var writer = _input[lane].Writer;
        var span = writer.GetSpan(InputRecord);

        BinaryPrimitives.WriteInt64LittleEndian(span, position);
        payload[..TelemetryCodec.Size].CopyTo(span[8..]);
        writer.Advance(InputRecord);

        var flush = writer.FlushAsync();
        return flush.IsCompletedSuccessfully ? ValueTask.CompletedTask : new ValueTask(flush.AsTask());
    }

    private async Task ProcessAsync(int lane, OutputStage stage)
    {
        var reader = _input[lane].Reader;
        var writer = _output[lane].Writer;

        // A saida vai direto para o Pipe de saida: identificador da pendencia
        // (8 bytes) mais a mensagem, sem array intermediario.
        OutputCallback emit = (id, payload) =>
        {
            var span = writer.GetSpan(8 + payload.Length);
            BinaryPrimitives.WriteInt64LittleEndian(span, id);
            payload.CopyTo(span[8..]);
            writer.Advance(8 + payload.Length);
        };

        while (true)
        {
            var result = await reader.ReadAsync();
            var buffer = result.Buffer;

            while (buffer.Length >= InputRecord)
            {
                var (position, evt) = ReadInput(buffer.Slice(0, InputRecord));
                stage.Process(evt, position, emit);
                _latency.Record(lane, evt.PublishedTicks);
                buffer = buffer.Slice(InputRecord);
            }

            reader.AdvanceTo(buffer.Start, buffer.End);

            // A espera aqui e a contrapressao da etapa de publicacao.
            await writer.FlushAsync();

            if (result.IsCompleted)
            {
                if (!buffer.IsEmpty)
                {
                    Interlocked.Increment(ref _incompleteRecords);
                    Console.Error.WriteLine($"[pipelines] faixa {lane}: {buffer.Length} bytes de registro incompleto ao encerrar");
                }

                break;
            }
        }

        await reader.CompleteAsync();
        stage.Complete(emit);
        await writer.FlushAsync();
        await writer.CompleteAsync();
    }

    private static (long Position, TelemetryEvent Event) ReadInput(ReadOnlySequence<byte> record)
    {
        Span<byte> scratch = stackalloc byte[InputRecord];
        record.CopyTo(scratch);
        return (BinaryPrimitives.ReadInt64LittleEndian(scratch), TelemetryCodec.Read(scratch[8..]));
    }

    private async Task PublishAsync(int lane)
    {
        var reader = _output[lane].Reader;
        var size = _outputRecord - 8;

        while (true)
        {
            var result = await reader.ReadAsync();
            var buffer = result.Buffer;

            while (buffer.Length >= _outputRecord)
            {
                var record = buffer.Slice(0, _outputRecord);
                var id = ReadId(record);

                // O cliente do broker guarda a mensagem ate a confirmacao, entao
                // aqui ela precisa de um array proprio, como nos outros modos.
                var payload = new byte[size];
                record.Slice(8).CopyTo(payload);

                if (_tracker.InFlight(lane) >= _options.MaxInFlight)
                {
                    await _tracker.WaitInFlightAtMostAsync(lane, _options.MaxInFlight / 2);
                }

                _tracker.MarkPublished(lane, id);
                _republisher.Publish(lane, payload, id);
                buffer = buffer.Slice(_outputRecord);
            }

            reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await reader.CompleteAsync();
    }

    private static long ReadId(ReadOnlySequence<byte> record)
    {
        Span<byte> scratch = stackalloc byte[8];
        record.Slice(0, 8).CopyTo(scratch);
        return BinaryPrimitives.ReadInt64LittleEndian(scratch);
    }

    public async Task CompleteAsync()
    {
        foreach (var pipe in _input)
        {
            await pipe.Writer.CompleteAsync();
        }

        await Task.WhenAll(_processors);
        await Task.WhenAll(_publishers);

        for (var lane = 0; lane < _input.Length; lane++)
        {
            await _tracker.WaitInFlightAtMostAsync(lane, 0);
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// O "painel" do segundo cenario: le a saida republicada, mede a latencia de
/// ponta a ponta (do envio original ate aqui, com o mesmo relogio) e confere
/// o resultado. Na saida resumida, o digest e o mesmo ResultDigest do
/// consumidor, entao os dois precisam bater; na enriquecida, o EnrichedDigest.
/// </summary>
public sealed class PainelPipeline(OutputKind kind, TimeSpan windowSize, LatencyRecorder latency) : IProcessingPipeline
{
    public ResultDigest Summary { get; } = new();

    public EnrichedDigest Enriched { get; } = new();

    public string Mode => "painel";

    public long Messages => kind == OutputKind.Enriquecida ? Enriched.Messages : Summary.Windows;

    public string HashHex => (kind == OutputKind.Enriquecida ? Enriched.Hash : Summary.Hash).ToString("X16");

    public ValueTask SubmitAsync(int lane, ReadOnlySpan<byte> payload)
    {
        if (kind == OutputKind.Enriquecida)
        {
            var (evt, window) = OutputCodec.ReadEnriched(payload);
            Enriched.Add(evt, window);
            latency.Record(lane, evt.PublishedTicks);
        }
        else
        {
            var (stats, publishedTicks) = OutputCodec.ReadSummary(payload, windowSize);
            Summary.Add(stats);

            if (publishedTicks > 0)
            {
                latency.Record(lane, publishedTicks);
            }
        }

        return ValueTask.CompletedTask;
    }

    public Task CompleteAsync() => Task.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
