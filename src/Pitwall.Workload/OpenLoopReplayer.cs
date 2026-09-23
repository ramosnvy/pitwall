using System.Diagnostics;
using Pitwall.Contracts;

namespace Pitwall.Workload;

/// <summary>
/// Gerador de carga em malha aberta.
///
/// O evento e emitido no instante previsto pela taxa alvo, independentemente
/// de o destino ter acompanhado o anterior. E o oposto do gerador em malha
/// fechada, que so envia o proximo depois da resposta do anterior e que, sob
/// saturacao, simplesmente desacelera e deixa de registrar as latencias altas
/// (o problema conhecido como coordinated omission). Como os percentis P95 e
/// P99 sao justamente o que este trabalho compara, o gerador precisa ser de
/// malha aberta.
///
/// Quando o destino nao acompanha, o atraso acumulado e medido e reportado em
/// <see cref="ReplayResult.MaxLatenessMs"/>: uma rodada em que o gerador nao
/// sustentou a taxa alvo nao e comparavel e deve ser descartada.
/// </summary>
public sealed class OpenLoopReplayer(TelemetryDataset dataset, IEventSink sink)
{
    public async Task<ReplayResult> RunAsync(ReplayOptions options, CancellationToken ct)
    {
        var events = dataset.Events;
        var amplifier = new FleetAmplifier(options.FleetFactor);
        if (events.Length == 0)
        {
            throw new InvalidOperationException("O dataset esta vazio.");
        }

        var ticksPerEvent = (double)Stopwatch.Frequency / options.TargetRate;
        var warmupTicks = (long)(options.Warmup.TotalSeconds * Stopwatch.Frequency);
        var totalTicks = (long)((options.Warmup + options.Duration).TotalSeconds * Stopwatch.Frequency);

        long emitted = 0, emittedAfterWarmup = 0, maxLatenessTicks = 0;
        var index = 0;
        var replica = 0;
        var lap = 0;

        var startTimestamp = Stopwatch.GetTimestamp();
        var measurementStart = 0L;

        while (!ct.IsCancellationRequested)
        {
            var deadline = startTimestamp + (long)(emitted * ticksPerEvent);
            var now = WaitUntil(deadline, ct);

            var elapsed = now - startTimestamp;
            if (elapsed >= totalTicks)
            {
                break;
            }

            var lateness = now - deadline;
            if (elapsed >= warmupTicks && lateness > maxLatenessTicks)
            {
                maxLatenessTicks = lateness;
            }

            // O dataset tem duracao finita. Para sustentar a carga por mais
            // tempo, ele e reproduzido em ciclo; o numero da volta entra no
            // sequencial para que cada evento publicado seja unico.
            var sequence = ((lap * (long)events.Length) + index) * options.FleetFactor + replica;
            var evt = amplifier.Replicate(events[index], replica, sequence) with
            {
                PublishedTicks = Stopwatch.GetTimestamp()
            };

            await sink.PublishAsync(evt, ct);
            emitted++;

            if (elapsed >= warmupTicks)
            {
                if (measurementStart == 0)
                {
                    measurementStart = now;
                }

                emittedAfterWarmup++;
            }

            // Percorre as replicas de uma amostra antes de passar para a
            // proxima: e assim que uma frota se comporta, com todos os
            // carros reportando dentro do mesmo intervalo de amostragem.
            if (++replica == options.FleetFactor)
            {
                replica = 0;

                if (++index == events.Length)
                {
                    index = 0;
                    lap++;
                }
            }
        }

        await sink.FlushAsync(ct);

        var measuredTicks = Stopwatch.GetTimestamp() - measurementStart;
        var measuredSeconds = measuredTicks / (double)Stopwatch.Frequency;

        return new ReplayResult
        {
            TargetRate = options.TargetRate,
            EventsEmitted = emittedAfterWarmup,
            MeasuredSeconds = measuredSeconds,
            AchievedRate = measuredSeconds > 0 ? emittedAfterWarmup / measuredSeconds : 0,
            MaxLatenessMs = maxLatenessTicks * 1000.0 / Stopwatch.Frequency,
            DatasetLaps = lap
        };
    }

    /// <summary>
    /// Espera ate o instante alvo. Para esperas longas usa o relogio do
    /// sistema, que libera o processador; para as curtas usa espera ativa,
    /// porque a granularidade do agendador do Windows (cerca de 15 ms) e
    /// grosseira demais para taxas de dezenas de milhares de eventos por
    /// segundo.
    /// </summary>
    private static long WaitUntil(long deadline, CancellationToken ct)
    {
        var now = Stopwatch.GetTimestamp();
        if (now >= deadline)
        {
            return now;   // ja passou do horario: emite imediatamente
        }

        var remainingMs = (deadline - now) * 1000.0 / Stopwatch.Frequency;

        if (remainingMs > 2)
        {
            Thread.Sleep((int)(remainingMs - 1));
        }

        var spinner = new SpinWait();
        while ((now = Stopwatch.GetTimestamp()) < deadline)
        {
            if (ct.IsCancellationRequested)
            {
                break;
            }

            spinner.SpinOnce(sleep1Threshold: -1);   // nunca cede para Sleep(1)
        }

        return now;
    }
}

public sealed record ReplayOptions
{
    public required int TargetRate { get; init; }
    public required TimeSpan Duration { get; init; }
    public TimeSpan Warmup { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Quantas vezes a frota do dataset e replicada. 1 mantem os 20 carros
    /// originais; 50 simula mil carros com a mesma cadencia de sensor.
    /// </summary>
    public int FleetFactor { get; init; } = 1;
}

public sealed record ReplayResult
{
    public required int TargetRate { get; init; }
    public required long EventsEmitted { get; init; }
    public required double MeasuredSeconds { get; init; }
    public required double AchievedRate { get; init; }

    /// <summary>
    /// Maior atraso entre o instante previsto e o instante real de emissao.
    /// Acima de poucos milissegundos, o gerador nao sustentou a taxa e a
    /// rodada deve ser descartada.
    /// </summary>
    public required double MaxLatenessMs { get; init; }

    public required int DatasetLaps { get; init; }

    public double RateErrorPercent => TargetRate > 0
        ? (AchievedRate - TargetRate) / TargetRate * 100.0
        : 0;
}
