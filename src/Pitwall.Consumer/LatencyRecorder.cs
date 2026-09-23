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
///
/// **Aquecimento descartado.** Os primeiros segundos de uma rodada misturam
/// compilacao JIT, abertura de conexao, busca de metadados do broker e
/// eventos que chegaram antes de o consumidor assinar. A 30 mil ev/s, um
/// unico segundo de aquecimento ja e 1,1% dos eventos de uma janela de 90 s
/// -- mais do que o 1% que define o P99. Sem descarte, o numero mais
/// importante do trabalho seria medido justamente sobre o aquecimento.
/// </summary>
public sealed class LatencyRecorder(int lanes, TimeSpan warmup)
{
    private static readonly double TicksPerMicrosecond = Stopwatch.Frequency / 1_000_000.0;

    private readonly long _warmupTicks = (long)(warmup.TotalSeconds * Stopwatch.Frequency);
    private readonly LongHistogram[] _histograms = CreateHistograms(lanes);
    private readonly long[] _counts = new long[lanes];
    private readonly long[] _warmupCounts = new long[lanes];

    // Primeiro e ultimo evento MEDIDO de cada faixa, para calcular a vazao
    // sobre a janela de medicao -- sem o aquecimento e sem a espera ociosa
    // que encerra a rodada.
    private readonly long[] _firstTicks = new long[lanes];
    private readonly long[] _lastTicks = new long[lanes];

    // Instante do primeiro evento da rodada, em qualquer faixa. O aquecimento
    // conta a partir dele, igual para todas as faixas.
    private long _roundStart;

    public TimeSpan Warmup => warmup;

    public void Record(int lane, long publishedTicks)
    {
        var now = Stopwatch.GetTimestamp();

        if (Interlocked.Read(ref _roundStart) == 0)
        {
            Interlocked.CompareExchange(ref _roundStart, now, 0);
        }

        if (now - Interlocked.Read(ref _roundStart) < _warmupTicks)
        {
            _warmupCounts[lane]++;
            return;
        }

        var micros = (long)((now - publishedTicks) / TicksPerMicrosecond);

        // Relogio nao-monotonico entre processos nao deveria ocorrer na mesma
        // maquina, mas um valor negativo corromperia o histograma inteiro.
        if (micros < 0)
        {
            micros = 0;
        }

        _histograms[lane].RecordValue(micros);

        if (_counts[lane] == 0)
        {
            _firstTicks[lane] = now;
        }

        _lastTicks[lane] = now;
        _counts[lane]++;
    }

    /// <summary>Eventos na janela de medicao, depois do aquecimento.</summary>
    public long TotalCount => _counts.Sum();

    /// <summary>Eventos descartados como aquecimento.</summary>
    public long WarmupCount => _warmupCounts.Sum();

    /// <summary>
    /// Duracao da janela de medicao: do primeiro ao ultimo evento medido, sem
    /// o aquecimento e sem a espera ociosa do encerramento.
    /// </summary>
    public double ActiveSeconds
    {
        get
        {
            var active = Enumerable.Range(0, _counts.Length).Where(i => _counts[i] > 0).ToArray();

            if (active.Length == 0)
            {
                return 0;
            }

            var first = active.Min(i => _firstTicks[i]);
            var last = active.Max(i => _lastTicks[i]);

            return (last - first) / (double)Stopwatch.Frequency;
        }
    }

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

        if (h.TotalCount == 0)
        {
            return "sem eventos na janela de medicao";
        }

        return $"n={h.TotalCount:N0} | media={h.GetMean() / 1000:0.00}ms | " +
               $"P50={h.GetValueAtPercentile(50) / 1000.0:0.00}ms | " +
               $"P95={h.GetValueAtPercentile(95) / 1000.0:0.00}ms | " +
               $"P99={h.GetValueAtPercentile(99) / 1000.0:0.00}ms | " +
               $"max={h.GetMaxValue() / 1000.0:0.00}ms";
    }

    private static LongHistogram[] CreateHistograms(int lanes) =>
        Enumerable.Range(0, lanes).Select(_ => NewHistogram()).ToArray();

    // Valores em microssegundos: de 1 us a 5 minutos, 3 digitos significativos.
    private const long MaxTrackableMicros = 5L * 60 * 1_000_000;

    private static LongHistogram NewHistogram() =>
        new(1, MaxTrackableMicros, 3);
}
