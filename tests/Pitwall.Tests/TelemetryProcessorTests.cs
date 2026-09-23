using Pitwall.Contracts;
using Pitwall.Processing.Core;

namespace Pitwall.Tests;

/// <summary>
/// O processamento e a mesma logica nas seis variantes. Estes testes fixam o
/// que ele calcula e a propriedade que sustenta a verificacao cruzada: o
/// resultado nao depende de quantos workers existem nem da ordem em que as
/// janelas fecham.
/// </summary>
public class TelemetryProcessorTests
{
    private static readonly DateTimeOffset T0 = new(2024, 3, 2, 15, 0, 0, TimeSpan.Zero);

    private static TelemetryEvent Sample(
        int driver, double seconds, short speed = 200, short gear = 6, short brake = 0) => new()
    {
        Sequence = 0,
        SessionKey = 9472,
        DriverNumber = driver,
        EventTime = T0.AddSeconds(seconds),
        PublishedTicks = 0,
        Speed = speed,
        Rpm = 10_000,
        Gear = gear,
        Throttle = 80,
        Brake = brake,
        Drs = 0
    };

    private static List<DriverWindowStats> Run(IEnumerable<TelemetryEvent> events)
    {
        var windows = new List<DriverWindowStats>();
        var processor = new TelemetryProcessor(new ProcessingOptions(), windows.Add);

        foreach (var evt in events)
        {
            processor.Process(evt);
        }

        processor.Complete();
        return windows;
    }

    [Fact]
    public void Samples_in_the_same_second_form_one_window()
    {
        var windows = Run(
        [
            Sample(44, 0.1, speed: 100),
            Sample(44, 0.5, speed: 200),
            Sample(44, 0.9, speed: 300)
        ]);

        var w = Assert.Single(windows);
        Assert.Equal(3, w.EventCount);
        Assert.Equal(200f, w.AvgSpeed);
        Assert.Equal((short)300, w.MaxSpeed);
        Assert.Equal(T0, w.WindowStart);
        Assert.Equal(T0.AddSeconds(1), w.WindowEnd);
    }

    [Fact]
    public void Crossing_a_second_closes_the_window()
    {
        var windows = Run([Sample(44, 0.5), Sample(44, 1.5)]);

        Assert.Equal(2, windows.Count);
        Assert.Equal(T0, windows[0].WindowStart);
        Assert.Equal(T0.AddSeconds(1), windows[1].WindowStart);
    }

    /// <summary>
    /// Frenagem conta na TRANSICAO de freio solto para acionado. Tres amostras
    /// seguidas com freio acionado sao uma frenagem, nao tres.
    /// </summary>
    [Fact]
    public void Hard_braking_counts_transitions_not_samples()
    {
        var windows = Run(
        [
            Sample(44, 0.0, brake: 0),
            Sample(44, 0.1, brake: 100),
            Sample(44, 0.2, brake: 100),
            Sample(44, 0.3, brake: 100),
            Sample(44, 0.4, brake: 0),
            Sample(44, 0.5, brake: 100)
        ]);

        Assert.Equal(2, Assert.Single(windows).HardBrakings);
    }

    /// <summary>
    /// A deteccao de padrao depende da ordem por carro. Este teste e o motivo
    /// de a chave de particionamento e o arranjo de filas do RabbitMQ existirem:
    /// a mesma sequencia fora de ordem produz outra contagem.
    /// </summary>
    [Fact]
    public void Reordering_samples_changes_the_result()
    {
        TelemetryEvent[] ordered =
        [
            Sample(44, 0.0, gear: 5, brake: 0),
            Sample(44, 0.1, gear: 6, brake: 100),
            Sample(44, 0.2, gear: 7, brake: 0),
            Sample(44, 0.3, gear: 8, brake: 100)
        ];

        var inOrder = Assert.Single(Run(ordered));
        var shuffled = Assert.Single(Run([ordered[0], ordered[2], ordered[1], ordered[3]]));

        Assert.NotEqual(
            (inOrder.HardBrakings, inOrder.GearChanges),
            (shuffled.HardBrakings, shuffled.GearChanges));
    }

    [Fact]
    public void Neutral_is_not_a_gear_change()
    {
        var windows = Run(
        [
            Sample(44, 0.0, gear: 3),
            Sample(44, 0.1, gear: 0),
            Sample(44, 0.2, gear: 4)
        ]);

        Assert.Equal(1, Assert.Single(windows).GearChanges);
    }

    [Fact]
    public void Cars_do_not_share_state()
    {
        var windows = Run(
        [
            Sample(1, 0.1, brake: 0),
            Sample(2, 0.1, brake: 100),
            Sample(1, 0.2, brake: 100)
        ]);

        Assert.Equal(2, windows.Count);
        Assert.Equal(1, windows.Single(w => w.DriverNumber == 1).HardBrakings);
        Assert.Equal(0, windows.Single(w => w.DriverNumber == 2).HardBrakings);
    }

    /// <summary>
    /// A propriedade que sustenta a verificacao cruzada entre as seis
    /// variantes: dividir os carros entre 1, 2, 4 ou 8 workers produz o mesmo
    /// digest, desde que cada carro fique inteiro em um worker.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void Digest_does_not_depend_on_worker_count(int shards)
    {
        var events = SyntheticRace(cars: 40, seconds: 30);

        var reference = new ResultDigest();
        var single = new TelemetryProcessor(new ProcessingOptions(), s => reference.Add(s));
        foreach (var e in events) single.Process(e);
        single.Complete();

        var sharded = new ResultDigest();
        var processors = Enumerable.Range(0, shards)
            .Select(_ => new TelemetryProcessor(new ProcessingOptions(), s => sharded.Add(s)))
            .ToArray();

        foreach (var e in events) processors[e.DriverNumber % shards].Process(e);
        foreach (var p in processors) p.Complete();

        Assert.True(reference.Matches(sharded));
        Assert.Equal(reference.Hash, sharded.Hash);
    }

    [Fact]
    public void Digest_does_not_depend_on_window_order()
    {
        var windows = Run(SyntheticRace(cars: 10, seconds: 10));

        var forward = new ResultDigest();
        foreach (var w in windows) forward.Add(w);

        var backward = new ResultDigest();
        foreach (var w in Enumerable.Reverse(windows)) backward.Add(w);

        Assert.True(forward.Matches(backward));
    }

    [Fact]
    public void Digest_detects_a_lost_window()
    {
        var windows = Run(SyntheticRace(cars: 10, seconds: 10));

        var complete = new ResultDigest();
        foreach (var w in windows) complete.Add(w);

        var missingOne = new ResultDigest();
        foreach (var w in windows.Skip(1)) missingOne.Add(w);

        Assert.False(complete.Matches(missingOne));
    }

    /// <summary>
    /// Corrida sintetica deterministica: cada carro a 3,7 Hz, com marchas e
    /// freio variando de forma pseudoaleatoria mas reproduzivel.
    /// </summary>
    private static List<TelemetryEvent> SyntheticRace(int cars, int seconds)
    {
        var random = new Random(20240302);
        var events = new List<TelemetryEvent>();
        var samplesPerCar = (int)(seconds * 3.7);

        for (var i = 0; i < samplesPerCar; i++)
        {
            for (var car = 1; car <= cars; car++)
            {
                events.Add(Sample(
                    car,
                    i / 3.7,
                    speed: (short)random.Next(80, 330),
                    gear: (short)random.Next(0, 9),
                    brake: (short)(random.Next(0, 4) == 0 ? 100 : 0)));
            }
        }

        return events;
    }
}
