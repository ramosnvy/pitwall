using System.Buffers;
using System.Diagnostics;
using Pitwall.Consumer;
using Pitwall.Consumer.Pipelines;
using Pitwall.Contracts;
using Pitwall.Processing.Core;

namespace Pitwall.Tests;

/// <summary>
/// Contrapressao dos mecanismos internos (REVISAO-TECNICA §2.3 e §2.4).
///
/// Com a fila interna cheia, SubmitAsync precisa devolver a espera em vez de
/// bloquear a thread chamadora: no RabbitMQ essa thread e o despachante
/// assincrono do cliente. E um registro incompleto ao encerrar o Pipe precisa
/// ser contado, nao descartado em silencio.
/// </summary>
public class PipelineBackpressureTests
{
    // 50 ms de custo sintetico por evento: o worker fica ocupado tempo
    // suficiente para a fila interna encher de forma deterministica.
    private static readonly ProcessingOptions Slow = new() { SyntheticCostMicros = 50_000 };

    private static byte[] Encoded(int driver, double seconds)
    {
        var evt = new TelemetryEvent
        {
            Sequence = 0,
            SessionKey = 9472,
            DriverNumber = driver,
            EventTime = new DateTimeOffset(2024, 3, 2, 15, 0, 0, TimeSpan.Zero).AddSeconds(seconds),
            PublishedTicks = Stopwatch.GetTimestamp(),
            Speed = 200,
            Rpm = 10_000,
            Gear = 6,
            Throttle = 80,
            Brake = 0,
            Drs = 0
        };

        var bytes = new byte[TelemetryCodec.Size];
        TelemetryCodec.Write(bytes, evt);
        return bytes;
    }

    private static LatencyRecorder Recorder() => new(1, TimeSpan.Zero);

    /// <summary>
    /// Entrega eventos ate a espera ficar pendente e devolve quanto tempo a
    /// chamada que a devolveu levou para retornar.
    /// </summary>
    private static async Task<TimeSpan> SubmitUntilPending(IProcessingPipeline pipeline)
    {
        for (var i = 0; i < 20; i++)
        {
            var watch = Stopwatch.StartNew();
            var submitted = pipeline.SubmitAsync(0, Encoded(1, i * 0.01));
            watch.Stop();

            if (!submitted.IsCompleted)
            {
                // A espera termina quando o worker libera espaco.
                await submitted;
                return watch.Elapsed;
            }
        }

        throw new Xunit.Sdk.XunitException("A fila interna nunca encheu: a contrapressao nao foi exercitada.");
    }

    [Fact]
    public async Task Channels_devolve_a_espera_em_vez_de_bloquear_quando_a_fila_enche()
    {
        await using var pipeline = new ChannelsPipeline(1, 1, Slow, _ => { }, Recorder());

        var returnedIn = await SubmitUntilPending(pipeline);

        // Bloqueando, a chamada so voltaria depois de um evento processado
        // (50 ms). Devolvendo a espera, volta na hora.
        Assert.True(returnedIn < TimeSpan.FromMilliseconds(20), $"SubmitAsync levou {returnedIn.TotalMilliseconds:0.0} ms");

        await pipeline.CompleteAsync();
    }

    [Fact]
    public async Task Pipelines_devolve_a_espera_em_vez_de_bloquear_quando_o_pipe_enche()
    {
        // Pausa acima de um registro: o segundo ja fica esperando o leitor.
        await using var pipeline = new PipelinesPipeline(1, TelemetryCodec.Size, Slow, _ => { }, Recorder());

        var returnedIn = await SubmitUntilPending(pipeline);

        Assert.True(returnedIn < TimeSpan.FromMilliseconds(20), $"SubmitAsync levou {returnedIn.TotalMilliseconds:0.0} ms");

        await pipeline.CompleteAsync();
    }

    [Fact]
    public async Task Direct_nunca_fica_pendente()
    {
        await using var pipeline = new DirectPipeline(1, new ProcessingOptions(), _ => { }, Recorder());

        var submitted = pipeline.SubmitAsync(0, Encoded(1, 0));

        Assert.True(submitted.IsCompletedSuccessfully);
        await pipeline.CompleteAsync();
    }

    [Fact]
    public async Task Pipelines_conta_registro_incompleto_ao_encerrar()
    {
        await using var pipeline = new PipelinesPipeline(1, 100 * TelemetryCodec.Size, new ProcessingOptions(), _ => { }, Recorder());

        await pipeline.SubmitAsync(0, Encoded(1, 0));

        // Meio registro escrito direto no Pipe, como faria um escritor com defeito.
        var writer = pipeline.WriterForTests(0);
        writer.Write(new byte[TelemetryCodec.Size / 2]);
        await writer.FlushAsync();

        await pipeline.CompleteAsync();

        Assert.Equal(1, pipeline.IncompleteRecords);
    }

    [Fact]
    public async Task Pipelines_sem_sobra_nao_conta_nada()
    {
        await using var pipeline = new PipelinesPipeline(1, 100 * TelemetryCodec.Size, new ProcessingOptions(), _ => { }, Recorder());

        for (var i = 0; i < 10; i++)
        {
            await pipeline.SubmitAsync(0, Encoded(1, i * 0.01));
        }

        await pipeline.CompleteAsync();

        Assert.Equal(0, pipeline.IncompleteRecords);
    }
}
