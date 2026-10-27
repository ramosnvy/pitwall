using System.Collections.Concurrent;
using Pitwall.Consumer;
using Pitwall.Consumer.Pipelines;
using Pitwall.Consumer.Republish;
using Pitwall.Contracts;
using Pitwall.Processing.Core;

namespace Pitwall.Tests;

/// <summary>
/// Segundo cenario, passo A (docs/DESENVOLVIMENTO.md, fase 9, etapas 9.2 a
/// 9.5): formatos de saida, rastreador de confirmacoes e os tres modos com
/// republicacao. O que mais importa: nenhuma entrada e liberada antes de a
/// saida que depende dela ser confirmada, e os tres modos produzem a mesma
/// saida.
/// </summary>
public class RepublishTests
{
    private static TelemetryEvent Event(int driver, double seconds, long sequence, short brake = 0, short gear = 3) => new()
    {
        Sequence = sequence,
        SessionKey = 9472,
        DriverNumber = driver,
        EventTime = new DateTimeOffset(2024, 3, 2, 15, 0, 0, TimeSpan.Zero).AddSeconds(seconds),
        PublishedTicks = System.Diagnostics.Stopwatch.GetTimestamp(),
        Speed = (short)(200 + sequence % 50),
        Rpm = 10_000,
        Gear = gear,
        Throttle = 80,
        Brake = brake,
        Drs = 0
    };

    private static byte[] Encode(in TelemetryEvent evt)
    {
        var bytes = new byte[TelemetryCodec.Size];
        TelemetryCodec.Write(bytes, evt);
        return bytes;
    }

    /// <summary>Tres carros na mesma faixa, 25 amostras cada a 10 Hz: janelas de 1 s viram duas vezes.</summary>
    private static List<TelemetryEvent> Workload()
    {
        var events = new List<TelemetryEvent>();
        long sequence = 0;

        for (var i = 0; i < 25; i++)
        {
            foreach (var driver in new[] { 1, 11, 44 })
            {
                events.Add(Event(driver, i * 0.1, sequence++, brake: (short)(i % 7 == 0 ? 100 : 0), gear: (short)(1 + i / 5)));
            }
        }

        return events;
    }

    /// <summary>Publicador de mentira: guarda as saidas e confirma quando o teste manda, ou sozinho.</summary>
    private sealed class FakeRepublisher(ReleaseTracker tracker, bool autoConfirm) : IRepublisher
    {
        public readonly ConcurrentQueue<(int Lane, byte[] Payload, long Id)> Sent = new();

        public long Published => Sent.Count;

        public long Failed => 0;

        public string EffectiveConfig => "fake";

        public void Publish(int lane, byte[] payload, long id)
        {
            Sent.Enqueue((lane, payload, id));

            if (autoConfirm)
            {
                // Confirma de outra thread, com atraso, como um broker.
                _ = Task.Run(async () =>
                {
                    await Task.Delay(1);
                    tracker.Confirm(lane, id);
                });
            }
        }

        public void Confirm(long id) => tracker.Confirm(0, id);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static IProcessingPipeline Create(string mode, RepublishOptions options, Action<DriverWindowStats> onWindow,
        ReleaseTracker tracker, IRepublisher republisher)
    {
        var latency = new LatencyRecorder(1, TimeSpan.Zero);
        var processing = new ProcessingOptions();

        return mode switch
        {
            "direct" => new RepublishDirectPipeline(1, options, processing, onWindow, latency, tracker, republisher),
            "channels" => new RepublishChannelsPipeline(1, options, processing, onWindow, latency, tracker, republisher),
            "pipelines" => new RepublishPipelinesPipeline(1, options, processing, onWindow, latency, tracker, republisher),
            _ => throw new ArgumentException(mode)
        };
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("Condicao nao atingida em 10 s.");
            }

            await Task.Delay(5);
        }
    }

    [Fact]
    public void EnrichedOutputRoundTrips()
    {
        var evt = Event(44, 1.25, 7, brake: 100);
        var window = new WindowSnapshot(0, 13, 251.5f, 302, 2, 4);
        var buffer = new byte[OutputCodec.EnrichedSize];

        OutputCodec.WriteEnriched(buffer, evt, window);
        var (readEvent, readWindow) = OutputCodec.ReadEnriched(buffer);

        Assert.Equal(evt, readEvent);
        Assert.Equal(window with { WindowStartTicks = 0 }, readWindow);
        Assert.Equal(64, OutputCodec.EnrichedSize);
    }

    [Fact]
    public void SummaryOutputRoundTrips()
    {
        var start = new DateTimeOffset(2024, 3, 2, 15, 0, 1, TimeSpan.Zero);
        var stats = new DriverWindowStats
        {
            DriverNumber = 1011,
            WindowStart = start,
            WindowEnd = start.AddSeconds(1),
            EventCount = 100,
            AvgSpeed = 233.25f,
            MaxSpeed = 312,
            HardBrakings = 3,
            GearChanges = 5
        };
        var buffer = new byte[OutputCodec.SummarySize];

        OutputCodec.WriteSummary(buffer, stats, triggerPublishedTicks: 987_654);
        var (read, ticks) = OutputCodec.ReadSummary(buffer, TimeSpan.FromSeconds(1));

        Assert.Equal(stats, read);
        Assert.Equal(987_654, ticks);
    }

    [Fact]
    public void TrackerReleasesOnlyTheContiguousConfirmedPrefix()
    {
        var tracker = new ReleaseTracker(1);
        var ids = new long[5];

        for (var position = 1; position <= 5; position++)
        {
            ids[position - 1] = tracker.Open(0, position);
            tracker.MarkPublished(0, ids[position - 1]);
            tracker.Processed(0, position);
        }

        Assert.Equal(0, tracker.Released(0));

        // Confirmacoes fora de ordem: a 3 sozinha nao libera nada.
        tracker.Confirm(0, ids[2]);
        Assert.Equal(0, tracker.Released(0));

        tracker.Confirm(0, ids[0]);
        Assert.Equal(1, tracker.Released(0));

        tracker.Confirm(0, ids[1]);
        Assert.Equal(3, tracker.Released(0));

        tracker.Confirm(0, ids[3]);
        tracker.Confirm(0, ids[4]);
        Assert.Equal(5, tracker.Released(0));
        Assert.Equal(0, tracker.InFlight(0));
    }

    [Fact]
    public void TrackerGrowsBeyondItsInitialCapacity()
    {
        var tracker = new ReleaseTracker(1);
        var ids = new List<long>();

        for (var position = 0; position < 5_000; position++)
        {
            var id = tracker.Open(0, position);
            tracker.MarkPublished(0, id);
            tracker.Processed(0, position);
            ids.Add(id);
        }

        foreach (var id in ids.AsEnumerable().Reverse())
        {
            tracker.Confirm(0, id);
        }

        Assert.Equal(4_999, tracker.Released(0));
    }

    [Theory]
    [InlineData("direct")]
    [InlineData("channels")]
    [InlineData("pipelines")]
    public async Task NoInputIsReleasedBeforeItsOutputIsConfirmed(string mode)
    {
        var tracker = new ReleaseTracker(1);
        var republisher = new FakeRepublisher(tracker, autoConfirm: false);
        var options = new RepublishOptions { Kind = OutputKind.Enriquecida, ConfirmBatch = 1_000, MaxInFlight = 1_000, Capacity = 100 };
        await using var pipeline = Create(mode, options, _ => { }, tracker, republisher);

        // Posicoes 1 a 30, como as etiquetas de entrega do RabbitMQ.
        var events = Workload().Take(30).ToList();
        for (var i = 0; i < events.Count; i++)
        {
            await pipeline.SubmitAsync(0, Encode(events[i]), i + 1);
        }

        await WaitUntil(() => republisher.Sent.Count == 30);
        Assert.Equal(0, pipeline.Released(0));

        var sent = republisher.Sent.ToArray();

        // A 20 confirmada antes das outras nao libera nada alem do trecho continuo.
        republisher.Confirm(sent[19].Id);
        for (var i = 0; i < 10; i++)
        {
            republisher.Confirm(sent[i].Id);
        }

        Assert.Equal(10, pipeline.Released(0));

        for (var i = 10; i < 30; i++)
        {
            if (i != 19)
            {
                republisher.Confirm(sent[i].Id);
            }
        }

        Assert.Equal(30, pipeline.Released(0));
        await pipeline.CompleteAsync();
    }

    [Fact]
    public async Task DirectStopsReceivingUntilTheBatchIsConfirmed()
    {
        var tracker = new ReleaseTracker(1);
        var republisher = new FakeRepublisher(tracker, autoConfirm: false);
        var options = new RepublishOptions { Kind = OutputKind.Enriquecida, ConfirmBatch = 5 };
        await using var pipeline = Create("direct", options, _ => { }, tracker, republisher);
        var events = Workload();

        for (var i = 0; i < 4; i++)
        {
            Assert.True(pipeline.SubmitAsync(0, Encode(events[i]), i + 1).IsCompletedSuccessfully);
        }

        // A quinta saida fecha o lote: o Direct para ate tudo ser confirmado.
        var fifth = pipeline.SubmitAsync(0, Encode(events[4]), 5).AsTask();
        await Task.Delay(50);
        Assert.False(fifth.IsCompleted);

        foreach (var (_, _, id) in republisher.Sent)
        {
            republisher.Confirm(id);
        }

        await fifth.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(5, pipeline.Released(0));
        Assert.True(tracker.Waits >= 1);
    }

    [Fact]
    public async Task SummaryReleasesAWindowOnlyAfterItsOutputIsConfirmed()
    {
        var tracker = new ReleaseTracker(1);
        var republisher = new FakeRepublisher(tracker, autoConfirm: false);
        var options = new RepublishOptions { Kind = OutputKind.Resumida, ConfirmBatch = 1_000 };
        await using var pipeline = Create("direct", options, _ => { }, tracker, republisher);

        // Um carro: posicoes 1 a 10 na janela do segundo 0, e a 11 abre a do segundo 1.
        for (var i = 0; i < 10; i++)
        {
            await pipeline.SubmitAsync(0, Encode(Event(1, i * 0.1, i)), i + 1);
        }

        Assert.Empty(republisher.Sent);
        Assert.Equal(0, pipeline.Released(0));

        await pipeline.SubmitAsync(0, Encode(Event(1, 1.0, 10)), 11);

        // A janela 0 saiu, mas sem confirmacao as entradas dela continuam presas.
        Assert.Single(republisher.Sent);
        Assert.Equal(0, pipeline.Released(0));

        republisher.Confirm(republisher.Sent.Single().Id);

        // Liberado ate antes da janela que continua aberta.
        Assert.Equal(10, pipeline.Released(0));
    }

    [Theory]
    [InlineData(OutputKind.Enriquecida)]
    [InlineData(OutputKind.Resumida)]
    public async Task TheThreeModesProduceTheSameOutput(OutputKind kind)
    {
        var digests = new List<string>();
        var processingDigests = new List<ulong>();

        foreach (var mode in new[] { "direct", "channels", "pipelines" })
        {
            var tracker = new ReleaseTracker(1);
            var republisher = new FakeRepublisher(tracker, autoConfirm: true);
            var options = new RepublishOptions { Kind = kind, ConfirmBatch = 7, MaxInFlight = 16, Capacity = 32 };
            var processed = new ResultDigest();
            var events = Workload();

            await using (var pipeline = Create(mode, options, s => processed.Add(s), tracker, republisher))
            {
                for (var i = 0; i < events.Count; i++)
                {
                    await pipeline.SubmitAsync(0, Encode(events[i]), i + 1);
                }

                await pipeline.CompleteAsync();
                Assert.Equal(events.Count, pipeline.Released(0));
            }

            // O painel le a saida e confere.
            var painel = new PainelPipeline(kind, TimeSpan.FromSeconds(1), new LatencyRecorder(1, TimeSpan.Zero));
            foreach (var (_, payload, _) in republisher.Sent)
            {
                await painel.SubmitAsync(0, payload);
            }

            if (kind == OutputKind.Enriquecida)
            {
                Assert.Equal(events.Count, painel.Messages);
            }
            else
            {
                // A saida resumida e exatamente o que o consumidor processou.
                Assert.Equal(processed.Hash, painel.Summary.Hash);
                Assert.Equal(processed.Windows, painel.Messages);
            }

            digests.Add(painel.HashHex);
            processingDigests.Add(processed.Hash);
        }

        Assert.Single(digests.Distinct());
        Assert.Single(processingDigests.Distinct());
    }
}
