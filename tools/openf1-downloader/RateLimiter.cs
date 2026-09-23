namespace Pitwall.Tools.OpenF1Downloader;

/// <summary>
/// Respeita os dois limites publicados pela OpenF1 para o plano gratuito:
/// 3 requisicoes por segundo e 30 por minuto. O segundo e o mais restritivo
/// em regime sustentado (uma requisicao a cada 2 segundos), entao e ele que
/// determina o tempo total da coleta.
/// </summary>
internal sealed class RateLimiter(int perSecond, int perMinute)
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Queue<DateTimeOffset> _recent = new();

    public async Task WaitTurnAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            while (true)
            {
                var now = DateTimeOffset.UtcNow;

                // Descarta o que ja saiu da janela de um minuto.
                while (_recent.Count > 0 && now - _recent.Peek() > TimeSpan.FromMinutes(1))
                {
                    _recent.Dequeue();
                }

                var inLastSecond = _recent.Count(t => now - t <= TimeSpan.FromSeconds(1));

                if (inLastSecond < perSecond && _recent.Count < perMinute)
                {
                    _recent.Enqueue(now);
                    return;
                }

                // Espera ate a requisicao mais antiga relevante sair da janela.
                var waitUntil = _recent.Count >= perMinute
                    ? _recent.Peek().AddMinutes(1)
                    : _recent.Where(t => now - t <= TimeSpan.FromSeconds(1)).Min().AddSeconds(1);

                var delay = waitUntil - now;
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, ct);
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }
}
