using Pitwall.Contracts;

namespace Pitwall.Workload;

/// <summary>
/// Reamostra a telemetria de cada carro para uma frequencia maior que a da
/// OpenF1 (3,7 Hz), inserindo pontos entre duas amostras reais consecutivas.
///
/// **Os pontos inseridos nao sao medidos.** A OpenF1 so tem 3,7 Hz; um carro
/// de F1 real amostra ate 100 Hz. A reamostragem serve para estudar a
/// frequencia por carro como fator do experimento -- muitos carros lentos
/// contra poucos carros rapidos na mesma vazao -- e nao acrescenta informacao
/// ao dado. Isso precisa constar no texto.
///
/// Canais continuos (velocidade, rotacao, acelerador) sao interpolados em
/// linha reta. Canais discretos (marcha, freio, DRS) mantem o valor da amostra
/// anterior ate a seguinte, em degrau. Com isso, cada transicao do freio
/// continua acontecendo uma unica vez, e o numero de frenagens detectadas pelo
/// Processing.Core nao muda com a frequencia -- so o numero de eventos.
///
/// Lacunas maiores que <c>maxGap</c> (carro no box, perda de sinal) nao sao
/// preenchidas: inventar um segundo inteiro de telemetria seria fabricar dado.
/// </summary>
public static class TelemetryInterpolator
{
    public static readonly TimeSpan DefaultMaxGap = TimeSpan.FromSeconds(1);

    /// <param name="ordered">Eventos em ordem de tempo, de todos os carros.</param>
    /// <param name="targetHz">Frequencia alvo por carro.</param>
    /// <param name="maxGap">Maior intervalo entre duas amostras que ainda e preenchido.</param>
    public static TelemetryEvent[] Upsample(TelemetryEvent[] ordered, double targetHz, TimeSpan? maxGap = null)
    {
        if (targetHz <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(targetHz), "A frequencia precisa ser maior que zero.");
        }

        var gapLimit = maxGap ?? DefaultMaxGap;
        var result = new List<TelemetryEvent>(capacity: EstimateCapacity(ordered, targetHz));

        foreach (var car in ordered.GroupBy(e => e.DriverNumber))
        {
            TelemetryEvent? previous = null;

            foreach (var current in car)
            {
                if (previous is { } a)
                {
                    AddBetween(result, a, current, targetHz, gapLimit);
                }

                result.Add(current);
                previous = current;
            }
        }

        // Reconstroi a ordem da corrida, como o TelemetryDataset faz ao carregar.
        var merged = result
            .OrderBy(e => e.EventTime)
            .ThenBy(e => e.DriverNumber)
            .ToArray();

        for (var i = 0; i < merged.Length; i++)
        {
            merged[i] = merged[i] with { Sequence = i };
        }

        return merged;
    }

    private static void AddBetween(List<TelemetryEvent> sink, in TelemetryEvent a, in TelemetryEvent b, double hz, TimeSpan maxGap)
    {
        var gap = b.EventTime - a.EventTime;
        if (gap <= TimeSpan.Zero || gap > maxGap)
        {
            return;
        }

        // Quantos pontos cabem entre as duas amostras reais nessa frequencia.
        var slots = (int)Math.Round(gap.TotalSeconds * hz);
        var inserted = slots - 1;

        for (var i = 1; i <= inserted; i++)
        {
            var f = i / (double)slots;

            sink.Add(a with
            {
                EventTime = a.EventTime + gap * f,
                Speed = (short)Math.Round(a.Speed + (b.Speed - a.Speed) * f),
                Rpm = (int)Math.Round(a.Rpm + (b.Rpm - a.Rpm) * f),
                Throttle = (short)Math.Round(a.Throttle + (b.Throttle - a.Throttle) * f)
                // Gear, Brake e Drs: mantem os de 'a', em degrau.
            });
        }
    }

    private static int EstimateCapacity(TelemetryEvent[] ordered, double hz)
    {
        if (ordered.Length < 2)
        {
            return ordered.Length;
        }

        var span = (ordered[^1].EventTime - ordered[0].EventTime).TotalSeconds;
        var cars = ordered.Select(e => e.DriverNumber).Distinct().Count();
        return (int)Math.Min(int.MaxValue / 2, Math.Max(ordered.Length, span * hz * cars * 1.05));
    }
}
