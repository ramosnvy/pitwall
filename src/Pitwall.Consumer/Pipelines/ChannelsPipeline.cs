using System.Threading.Channels;
using Pitwall.Contracts;
using Pitwall.Processing.Core;

namespace Pitwall.Consumer.Pipelines;

/// <summary>
/// Variante Channels: a thread do broker desserializa o evento e o entrega a
/// um <see cref="Channel{T}"/> limitado; um worker por faixa consome dali.
///
/// Fila produtor-consumidor de OBJETOS, dentro do processo. O limite de
/// capacidade e proposital: com <see cref="BoundedChannelFullMode.Wait"/>, um
/// worker lento aplica contrapressao sobre o consumo do broker em vez de a
/// fila crescer sem limite ate consumir a memoria. Descartar mensagem
/// invalidaria a rodada, entao esperar e a unica opcao correta.
/// </summary>
public sealed class ChannelsPipeline : IProcessingPipeline
{
    private readonly Channel<TelemetryEvent>[] _channels;
    private readonly Task[] _workers;
    private readonly LatencyRecorder _latency;

    public ChannelsPipeline(
        int lanes,
        int capacity,
        ProcessingOptions options,
        ResultDigest digest,
        LatencyRecorder latency)
    {
        _latency = latency;
        _channels = new Channel<TelemetryEvent>[lanes];
        _workers = new Task[lanes];

        for (var lane = 0; lane < lanes; lane++)
        {
            _channels[lane] = Channel.CreateBounded<TelemetryEvent>(
                new BoundedChannelOptions(capacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = true
                });

            var index = lane;
            var processor = new TelemetryProcessor(options, stats => digest.Add(stats));

            _workers[lane] = Task.Run(() => ConsumeAsync(index, processor));
        }
    }

    public string Mode => "channels";

    public void Submit(int lane, ReadOnlySpan<byte> payload)
    {
        var evt = TelemetryCodec.Read(payload);
        var writer = _channels[lane].Writer;

        // Caminho rapido sem alocacao; se a fila estiver cheia, bloqueia a
        // thread do broker, que e a contrapressao desejada.
        if (!writer.TryWrite(evt))
        {
            writer.WriteAsync(evt).AsTask().GetAwaiter().GetResult();
        }
    }

    private async Task ConsumeAsync(int lane, TelemetryProcessor processor)
    {
        var reader = _channels[lane].Reader;

        await foreach (var evt in reader.ReadAllAsync())
        {
            processor.Process(evt);
            _latency.Record(lane, evt.PublishedTicks);
        }

        processor.Complete();
    }

    public async Task CompleteAsync()
    {
        foreach (var channel in _channels)
        {
            channel.Writer.Complete();
        }

        await Task.WhenAll(_workers);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
