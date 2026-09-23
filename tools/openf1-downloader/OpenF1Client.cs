using System.Net;
using System.Text.Json;

namespace Pitwall.Tools.OpenF1Downloader;

/// <summary>
/// Cliente da API publica da OpenF1 (apenas dados historicos, que sao
/// gratuitos e nao exigem autenticacao).
/// </summary>
internal sealed class OpenF1Client(HttpClient http, RateLimiter limiter)
{
    private const string BaseUrl = "https://api.openf1.org/v1";
    private const int MaxAttempts = 5;

    /// <summary>Resposta para consultas sem resultado (a API devolve 404 nesse caso).</summary>
    private const string EmptyArray = "[]";

    /// <summary>
    /// Executa um GET e devolve o corpo bruto. Repete em 429 e em erros 5xx,
    /// com espera exponencial; honra o cabecalho Retry-After quando presente.
    /// </summary>
    public async Task<string> GetRawAsync(string endpoint, string query, CancellationToken ct)
    {
        var url = $"{BaseUrl}/{endpoint}?{query}";

        for (var attempt = 1; ; attempt++)
        {
            await limiter.WaitTurnAsync(ct);

            HttpResponseMessage response;
            try
            {
                response = await http.GetAsync(url, ct);
            }
            catch (HttpRequestException) when (attempt < MaxAttempts)
            {
                await BackoffAsync(attempt, ct);
                continue;
            }

            if (response.IsSuccessStatusCode)
            {
                return await response.Content.ReadAsStringAsync(ct);
            }

            // A OpenF1 responde 404 quando a consulta nao casa com nenhum
            // registro, o que acontece o tempo todo: um carro que abandonou
            // nao tem telemetria nas janelas seguintes. Nao e erro, e conjunto
            // vazio.
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                return EmptyArray;
            }

            var retryable = response.StatusCode == HttpStatusCode.TooManyRequests
                            || (int)response.StatusCode >= 500;

            if (!retryable || attempt >= MaxAttempts)
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                throw new InvalidOperationException(
                    $"OpenF1 respondeu {(int)response.StatusCode} para {url}: {Truncate(body, 300)}");
            }

            var retryAfter = response.Headers.RetryAfter?.Delta;
            if (retryAfter is { } wait)
            {
                Console.WriteLine($"  429 recebido, aguardando {wait.TotalSeconds:0}s (Retry-After)");
                await Task.Delay(wait, ct);
            }
            else
            {
                await BackoffAsync(attempt, ct);
            }
        }
    }

    public async Task<JsonElement[]> GetJsonAsync(string endpoint, string query, CancellationToken ct)
    {
        var raw = await GetRawAsync(endpoint, query, ct);
        using var doc = JsonDocument.Parse(raw);

        return doc.RootElement.ValueKind == JsonValueKind.Array
            ? doc.RootElement.EnumerateArray().Select(e => e.Clone()).ToArray()
            : [];
    }

    private static Task BackoffAsync(int attempt, CancellationToken ct)
    {
        var seconds = Math.Pow(2, attempt);
        Console.WriteLine($"  tentativa {attempt} falhou, aguardando {seconds:0}s");
        return Task.Delay(TimeSpan.FromSeconds(seconds), ct);
    }

    private static string Truncate(string s, int max) =>
        s.Length <= max ? s : s[..max] + "...";
}
