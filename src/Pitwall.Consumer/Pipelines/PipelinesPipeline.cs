using System.IO.Pipelines;
using Pitwall.Contracts;
using Pitwall.Processing.Core;

namespace Pitwall.Consumer.Pipelines;

/// <summary>
/// Variante Pipelines: a thread do broker copia os bytes crus do evento para
/// um <see cref="Pipe"/>; um worker por faixa le do <see cref="PipeReader"/>
/// e interpreta os bytes sem alocar objeto por mensagem.
///
/// Fluxo de BYTES, nao de objetos -- e a diferenca de natureza em relacao ao
/// Channels. Nenhum evento e desserializado na thread do broker: ela so
/// copia bytes, o que e o cenario em que o System.IO.Pipelines foi feito
/// para brilhar.
///
/// Como o registro tem tamanho fixo de 44 bytes, nao e preciso prefixo de
/// tamanho para saber onde um evento termina: o leitor consome de 44 em 44.
/// Isso evita dar a esta variante um cabecalho que a Channels nao pagaria.
/// </summary>
public sealed class PipelinesPipeline : IProcessingPipeline
{
    private readonly Pipe[] _pipes;
    private readonly Task[] _workers;
    private readonly LatencyRecorder _latency;

    public PipelinesPipeline(
        int lanes,
        int pauseBytes,
        ProcessingOptions options,
        ResultDigest digest,
        LatencyRecorder latency)
    {
        _latency = latency;
        _pipes = new Pipe[lanes];
        _workers = new Task[lanes];

        for (var lane = 0; lane < lanes; lane++)
        {
            // Contrapressao por bytes em transito, o analogo do limite de
            // capacidade do Channels: acima de pauseBytes o escritor espera.
            _pipes[lane] = new Pipe(new PipeOptions(
                pauseWriterThreshold: pauseBytes,
                resumeWriterThreshold: pauseBytes / 2,
                useSynchronizationContext: false));

            var index = lane;
            var processor = new TelemetryProcessor(options, stats => digest.Add(stats));

            _workers[lane] = Task.Run(() => ConsumeAsync(index, processor));
        }
    }

    public string Mode => "pipelines";

    public void Submit(int lane, ReadOnlySpan<byte> payload)
    {
        var writer = _pipes[lane].Writer;

        payload.CopyTo(writer.GetSpan(TelemetryCodec.Size));
        writer.Advance(TelemetryCodec.Size);

        // FlushAsync devolve uma tarefa ja completa enquanto ha espaco; se o
        // limite foi atingido, bloqueia a thread do broker -- a contrapressao.
        var flush = writer.FlushAsync();

        if (!flush.IsCompletedSuccessfully)
        {
            flush.AsTask().GetAwaiter().GetResult();
        }
    }

    private async Task ConsumeAsync(int lane, TelemetryProcessor processor)
    {
        var reader = _pipes[lane].Reader;

        while (true)
        {
            var result = await reader.ReadAsync();
            var buffer = result.Buffer;

            while (TelemetryCodec.TryRead(ref buffer, out var evt))
            {
                processor.Process(evt);
                _latency.Record(lane, evt.PublishedTicks);
            }

            // O que sobrou e um registro incompleto: fica para a proxima
            // leitura, que e exatamente o que o Pipe sabe fazer sem copiar.
            reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await reader.CompleteAsync();
        processor.Complete();
    }

    public async Task CompleteAsync()
    {
        foreach (var pipe in _pipes)
        {
            await pipe.Writer.CompleteAsync();
        }

        await Task.WhenAll(_workers);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
