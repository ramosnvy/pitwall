using Pitwall.Contracts;
using Pitwall.Processing.Core;
using Pitwall.Workload;

namespace Pitwall.Tests;

/// <summary>
/// A reamostragem para 100 Hz so e defensavel se nao distorcer o que o
/// processamento mede: frenagens e trocas de marcha continuam sendo as
/// mesmas, so ha mais eventos entre elas.
/// </summary>
public class TelemetryInterpolatorTests
{
    private static readonly DateTimeOffset T0 = new(2024, 3, 2, 15, 0, 0, TimeSpan.Zero);

    private static TelemetryEvent Sample(int driver, double ms, short speed, short gear, short brake, short throttle = 50, int rpm = 10_000) => new()
    {
        Sequence = 0,
        SessionKey = 9472,
        DriverNumber = driver,
        EventTime = T0.AddMilliseconds(ms),
        PublishedTicks = 0,
        Speed = speed,
        Rpm = rpm,
        Gear = gear,
        Throttle = throttle,
        Brake = brake,
        Drs = 0
    };

    [Fact]
    public void Fills_the_gap_between_two_samples_at_the_target_frequency()
    {
        // Duas amostras a 270 ms: a 100 Hz cabem 27 intervalos, 26 pontos novos.
        var events = new[] { Sample(1, 0, 100, 5, 0), Sample(1, 270, 127, 5, 0) };

        var result = TelemetryInterpolator.Upsample(events, 100);

        Assert.Equal(28, result.Length);
        Assert.Equal(events[0].EventTime, result[0].EventTime);
        Assert.Equal(events[1].EventTime, result[^1].EventTime);
    }

    [Fact]
    public void Continuous_channels_are_linear_and_discrete_channels_hold()
    {
        var events = new[]
        {
            Sample(1, 0, speed: 100, gear: 5, brake: 0, throttle: 100, rpm: 10_000),
            Sample(1, 200, speed: 200, gear: 6, brake: 100, throttle: 0, rpm: 12_000)
        };

        var result = TelemetryInterpolator.Upsample(events, 10);   // 1 ponto novo, no meio

        Assert.Equal(3, result.Length);
        var mid = result[1];
        Assert.Equal(150, mid.Speed);
        Assert.Equal(11_000, mid.Rpm);
        Assert.Equal(50, mid.Throttle);
        Assert.Equal(5, mid.Gear);      // degrau: valor da amostra anterior
        Assert.Equal(0, mid.Brake);
    }

    [Fact]
    public void Does_not_invent_data_across_large_gaps()
    {
        // Dois segundos sem amostra (box, perda de sinal): nao preenche.
        var events = new[] { Sample(1, 0, 80, 3, 0), Sample(1, 2000, 90, 3, 0) };

        var result = TelemetryInterpolator.Upsample(events, 100);

        Assert.Equal(2, result.Length);
    }

    [Fact]
    public void Each_car_is_interpolated_only_with_its_own_samples()
    {
        var events = new[]
        {
            Sample(1, 0, 100, 5, 0), Sample(2, 50, 300, 8, 0),
            Sample(1, 100, 200, 5, 0), Sample(2, 150, 300, 8, 0)
        };

        var result = TelemetryInterpolator.Upsample(events, 100);

        Assert.All(result.Where(e => e.DriverNumber == 2), e => Assert.Equal(300, e.Speed));
        Assert.All(result.Where(e => e.DriverNumber == 1), e => Assert.InRange(e.Speed, (short)100, (short)200));

        // Ordem global por tempo e sequencia renumerada.
        Assert.True(result.Zip(result.Skip(1)).All(p => p.First.EventTime <= p.Second.EventTime));
        Assert.Equal(Enumerable.Range(0, result.Length).Select(i => (long)i), result.Select(e => e.Sequence));
    }

    [Fact]
    public void Upsampling_does_not_change_the_detected_brakings_or_gear_changes()
    {
        // Um carro freando e trocando de marcha varias vezes, a 3,7 Hz.
        var native = new List<TelemetryEvent>();
        short[] brake = [0, 0, 100, 100, 0, 0, 100, 0, 0, 0, 100, 100, 100, 0];
        short[] gear = [7, 7, 6, 5, 5, 6, 6, 4, 4, 5, 3, 3, 4, 5];
        for (var i = 0; i < brake.Length; i++)
        {
            native.Add(Sample(44, i * 270.0, (short)(250 - i * 5), gear[i], brake[i]));
        }

        var upsampled = TelemetryInterpolator.Upsample(native.ToArray(), 100);

        Assert.True(upsampled.Length > native.Count * 20);
        Assert.Equal(Count(native), Count(upsampled));
    }

    private static (int Brakings, int GearChanges) Count(IEnumerable<TelemetryEvent> events)
    {
        var windows = new List<DriverWindowStats>();
        var processor = new TelemetryProcessor(new ProcessingOptions(), windows.Add);
        foreach (var e in events)
        {
            processor.Process(e);
        }
        processor.Complete();
        return (windows.Sum(w => w.HardBrakings), windows.Sum(w => w.GearChanges));
    }
}
