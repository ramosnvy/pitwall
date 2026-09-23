using Pitwall.Contracts;
using Pitwall.Workload;

namespace Pitwall.Tests;

/// <summary>
/// A multiplicacao de frota e o que transforma 75 ev/s reais em centenas de
/// milhares. Se ela gerar carros repetidos, o processamento mistura o estado
/// de dois carros; se gerar numero negativo, o calculo da faixa quebra.
/// </summary>
public class FleetAmplifierTests
{
    private static TelemetryEvent Car(int driverNumber) => new()
    {
        Sequence = 0,
        SessionKey = 9472,
        DriverNumber = driverNumber,
        EventTime = new DateTimeOffset(2024, 3, 2, 15, 0, 0, TimeSpan.Zero),
        PublishedTicks = 0,
        Speed = 200,
        Rpm = 10_000,
        Gear = 6,
        Throttle = 80,
        Brake = 0,
        Drs = 0
    };

    [Fact]
    public void Replica_zero_is_the_original_car()
    {
        var amplifier = new FleetAmplifier(10);
        var original = Car(44);

        var replica = amplifier.Replicate(original, replica: 0, sequence: 7);

        Assert.Equal(44, replica.DriverNumber);
        Assert.Equal(original.EventTime, replica.EventTime);
        Assert.Equal(7, replica.Sequence);
    }

    /// <summary>
    /// Os numeros de carro da F1 vao de 1 a 99. Com fator 5.000 -- acima dos
    /// 5.364 necessarios para 400 mil ev/s com 20 carros -- nenhum numero pode
    /// se repetir nem ficar negativo.
    /// </summary>
    [Fact]
    public void Every_replica_of_every_car_gets_a_unique_positive_number()
    {
        const int factor = 5_000;
        var amplifier = new FleetAmplifier(factor);
        int[] grid = [1, 4, 10, 11, 14, 16, 18, 20, 22, 23, 24, 27, 31, 44, 55, 63, 77, 81, 2, 3];

        var seen = new HashSet<int>();

        foreach (var driver in grid)
        {
            for (var replica = 0; replica < factor; replica++)
            {
                var number = amplifier.Replicate(Car(driver), replica, 0).DriverNumber;

                Assert.True(number > 0, $"carro {driver} replica {replica} gerou {number}");
                Assert.True(seen.Add(number), $"numero {number} repetido");
            }
        }

        Assert.Equal(grid.Length * factor, seen.Count);
    }

    /// <summary>
    /// As replicas sao espalhadas dentro do intervalo de amostragem da OpenF1
    /// (3,7 Hz, cerca de 270 ms). Sem isso a frota inteira publicaria em
    /// rajadas sincronizadas.
    /// </summary>
    [Fact]
    public void Phase_offsets_stay_inside_one_sampling_interval()
    {
        const int factor = 100;
        var amplifier = new FleetAmplifier(factor);
        var original = Car(44);

        var offsets = Enumerable.Range(0, factor)
            .Select(r => amplifier.Replicate(original, r, 0).EventTime - original.EventTime)
            .ToArray();

        Assert.All(offsets, o => Assert.InRange(o.TotalMilliseconds, 0, 270));
        Assert.Equal(factor, offsets.Distinct().Count());
    }

    [Fact]
    public void Factor_must_be_positive()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new FleetAmplifier(0));
    }
}
