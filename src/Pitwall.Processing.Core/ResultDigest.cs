namespace Pitwall.Processing.Core;

/// <summary>
/// Resumo verificavel do resultado do processamento.
///
/// Serve para a verificacao cruzada entre as seis variantes da matriz: com a
/// mesma entrada, todas precisam produzir o mesmo digest. Divergencia
/// significa perda de evento, quebra de ordem ou bug -- e a rodada nao pode
/// entrar na analise. O DEBS Grand Challenge avalia submissoes por vazao,
/// latencia e correcao do resultado; este e o mecanismo de correcao aqui.
///
/// A combinacao e por soma e XOR, ambas comutativas: o digest independe da
/// ordem em que as janelas foram fechadas e de quantos workers existiam.
/// </summary>
public sealed class ResultDigest
{
    private long _windows;
    private long _events;
    private long _hardBrakings;
    private long _gearChanges;
    private long _maxSpeedSum;
    private ulong _hash;

    private readonly Lock _gate = new();

    public long Windows => _windows;
    public long Events => _events;
    public long HardBrakings => _hardBrakings;
    public long GearChanges => _gearChanges;
    public ulong Hash => _hash;

    public void Add(in DriverWindowStats stats)
    {
        // Hash por janela, combinado por XOR. A velocidade media fica de fora
        // de proposito: e ponto flutuante, e a ordem das somas poderia mudar
        // o ultimo bit entre variantes sem que houvesse erro nenhum.
        var h = Fnv1a(
            (ulong)stats.DriverNumber,
            (ulong)stats.WindowStart.UtcTicks,
            (ulong)stats.EventCount,
            (ulong)stats.MaxSpeed,
            (ulong)stats.HardBrakings,
            (ulong)stats.GearChanges);

        lock (_gate)
        {
            _windows++;
            _events += stats.EventCount;
            _hardBrakings += stats.HardBrakings;
            _gearChanges += stats.GearChanges;
            _maxSpeedSum += stats.MaxSpeed;
            _hash ^= h;
        }
    }

    public override string ToString() =>
        $"janelas={_windows:N0} eventos={_events:N0} frenagens={_hardBrakings:N0} " +
        $"trocas={_gearChanges:N0} somaVmax={_maxSpeedSum:N0} hash={_hash:X16}";

    /// <summary>Compara dois digests campo a campo, para relatorio de divergencia.</summary>
    public bool Matches(ResultDigest other) =>
        _windows == other._windows
        && _events == other._events
        && _hardBrakings == other._hardBrakings
        && _gearChanges == other._gearChanges
        && _maxSpeedSum == other._maxSpeedSum
        && _hash == other._hash;

    private static ulong Fnv1a(params ulong[] values)
    {
        const ulong offset = 14695981039346656037;
        const ulong prime = 1099511628211;

        var hash = offset;

        foreach (var value in values)
        {
            for (var shift = 0; shift < 64; shift += 8)
            {
                hash ^= (byte)(value >> shift);
                hash *= prime;
            }
        }

        return hash;
    }
}
