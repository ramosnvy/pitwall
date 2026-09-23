using System.Diagnostics;
using HdrHistogram;

namespace Pitwall.Consumer;

/// <summary>
/// Latencia de ponta a ponta: da publicacao no broker ate o fim do
/// processamento do evento.
///
/// Um histograma por faixa, sem sincronizacao entre elas: registrar latencia
/// num histograma compartilhado criaria contencao no caminho quente e a
/// propria medicao alteraria o resultado. Os histogramas sao somados no fim.
///
/// HdrHistogram guarda a distribuicao inteira em memoria constante, o que
/// permite extrair P50, P95 e P99 exatos -- media e desvio nao bastam para
/// caudas, que e onde as arquiteturas costumam se diferenciar.
/// </summary>
public sealed class LatencyRecorder(int lanes)
{
    private static readonly double TicksPerMicrosecond = Stopwatch.Frequency / 1_000_000.0;

    private readonly LongHistogram[] _histograms = CreateHistograms(lanes);
    private readonly long[] _counts = new long[lanes];

    public void Record(int lane, long publishedTicks)
    {
        var micros = (long)((Stopwatch.GetTimestamp() - publishedTicks) / TicksPerMicrosecond);

        // Relogio nao-monotonico entre processos nao deveria ocorrer na mesma
        // maquina, mas um valor negativo corromperia o histograma inteiro.
        if (micros < 0)
        {
            micros = 0;
        }

        _histograms[lane].RecordValue(micros);
        _counts[lane]++;
    }

    public long TotalCount => _counts.Sum();

    public LongHistogram Merged()
    {
        var merged = NewHistogram();

        foreach (var histogram in _histograms)
        {
            merged.Add(histogram);
        }

        return merged;
    }

    public string Summary()
    {
        var h = Merged();

        return $"n={h.TotalCount:N0} | media={h.GetMean() / 1000:0.00}ms | " +
               $"P50={h.GetValueAtPercentile(50) / 1000.0:0.00}ms | " +
               $"P95={h.GetValueAtPercentile(95) / 1000.0:0.00}ms | " +
               $"P99={h.GetValueAtPercentile(99) / 1000.0:0.00}ms | " +
               $"max={h.GetMaxValue() / 1000.0:0.00}ms";
    }

    private static LongHistogram[] CreateHistograms(int lanes) =>
        Enumerable.Range(0, lanes).Select(_ => NewHistogram()).ToArray();

    // De 1 microssegundo a 5 minutos, 3 digitos significativos.
    private static LongHistogram NewHistogram() =>
        new(1, TimeSpan.TicksPerMinute * 5, 3);
}
