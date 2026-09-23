using System.Diagnostics;
using System.Text.Json;

namespace Pitwall.Tools.OpenF1Downloader;

/// <summary>
/// Baixa a telemetria de uma sessao e grava em JSONL (um evento por linha).
///
/// A coleta e fatiada por piloto e por janela de tempo por dois motivos: a
/// resposta de uma corrida inteira para um piloto passa de 25 mil registros,
/// e janelas menores permitem retomar a coleta de onde parou sem refazer tudo.
/// </summary>
internal sealed class SessionDownloader(OpenF1Client client, string outputRoot)
{
    public async Task<int> DownloadAsync(
        int sessionKey,
        string[] endpoints,
        int chunkMinutes,
        CancellationToken ct)
    {
        var sessionDir = Path.Combine(outputRoot, sessionKey.ToString());
        Directory.CreateDirectory(sessionDir);

        var session = await GetSessionAsync(sessionKey, ct);
        if (session is null)
        {
            Console.Error.WriteLine($"Sessao {sessionKey} nao encontrada.");
            return 1;
        }

        var start = session.Value.GetProperty("date_start").GetDateTimeOffset();
        var end = session.Value.GetProperty("date_end").GetDateTimeOffset();

        Console.WriteLine($"Sessao {sessionKey}: {Describe(session.Value)}");
        Console.WriteLine($"Periodo: {start:u} ate {end:u} ({(end - start).TotalMinutes:0} min)");

        var drivers = await client.GetJsonAsync("drivers", $"session_key={sessionKey}", ct);
        var driverNumbers = drivers
            .Select(d => d.GetProperty("driver_number").GetInt32())
            .Distinct()
            .OrderBy(n => n)
            .ToArray();

        Console.WriteLine($"Pilotos: {driverNumbers.Length}");

        var windows = BuildWindows(start, end, TimeSpan.FromMinutes(chunkMinutes)).ToArray();
        var totalRequests = windows.Length * driverNumbers.Length * endpoints.Length;

        Console.WriteLine(
            $"Janelas de {chunkMinutes} min: {windows.Length} | " +
            $"requisicoes previstas: {totalRequests} | " +
            $"tempo estimado: {EstimateMinutes(totalRequests):0} min (limite de 30 req/min)");
        Console.WriteLine();

        var stopwatch = Stopwatch.StartNew();
        var counts = new Dictionary<string, long>();

        foreach (var endpoint in endpoints)
        {
            var target = Path.Combine(sessionDir, $"{endpoint}.jsonl");

            // A coleta e retomavel: se o arquivo ja existe, refazer do zero
            // gastaria dezenas de minutos de limite de requisicoes a toa.
            if (File.Exists(target))
            {
                Console.WriteLine($"[{endpoint}] {target} ja existe, pulando. Apague o arquivo para recoletar.");
                counts[endpoint] = File.ReadLines(target).LongCount();
                continue;
            }

            var written = await DownloadEndpointAsync(
                endpoint, sessionKey, driverNumbers, windows, target, ct);

            counts[endpoint] = written;
            Console.WriteLine($"[{endpoint}] {written:N0} eventos gravados em {target}");
        }

        await WriteManifestAsync(sessionDir, sessionKey, session.Value, endpoints, counts, chunkMinutes, ct);

        Console.WriteLine();
        Console.WriteLine($"Concluido em {stopwatch.Elapsed.TotalMinutes:0.0} min.");
        return 0;
    }

    private async Task<long> DownloadEndpointAsync(
        string endpoint,
        int sessionKey,
        int[] driverNumbers,
        (DateTimeOffset From, DateTimeOffset To)[] windows,
        string target,
        CancellationToken ct)
    {
        // Grava num arquivo temporario e so renomeia no fim: uma coleta
        // interrompida nao deixa um dataset parcial parecendo completo.
        var temp = target + ".partial";
        await using var writer = new StreamWriter(temp, append: false);

        long written = 0;
        var requestNumber = 0;
        var totalRequests = windows.Length * driverNumbers.Length;

        foreach (var (from, to) in windows)
        {
            foreach (var driver in driverNumbers)
            {
                requestNumber++;

                var query =
                    $"session_key={sessionKey}" +
                    $"&driver_number={driver}" +
                    $"&date>={Iso(from)}" +
                    $"&date<{Iso(to)}";

                var events = await client.GetJsonAsync(endpoint, query, ct);

                foreach (var evt in events)
                {
                    await writer.WriteLineAsync(evt.GetRawText().AsMemory(), ct);
                    written++;
                }

                if (requestNumber % 10 == 0 || requestNumber == totalRequests)
                {
                    Console.WriteLine(
                        $"[{endpoint}] {requestNumber}/{totalRequests} requisicoes | " +
                        $"{written:N0} eventos");
                }
            }
        }

        await writer.FlushAsync(ct);
        writer.Close();
        File.Move(temp, target, overwrite: true);

        return written;
    }

    private async Task<JsonElement?> GetSessionAsync(int sessionKey, CancellationToken ct)
    {
        var sessions = await client.GetJsonAsync("sessions", $"session_key={sessionKey}", ct);
        return sessions.Length > 0 ? sessions[0] : null;
    }

    private static IEnumerable<(DateTimeOffset From, DateTimeOffset To)> BuildWindows(
        DateTimeOffset start, DateTimeOffset end, TimeSpan size)
    {
        for (var cursor = start; cursor < end; cursor += size)
        {
            var next = cursor + size;
            yield return (cursor, next > end ? end : next);
        }
    }

    private async Task WriteManifestAsync(
        string sessionDir,
        int sessionKey,
        JsonElement session,
        string[] endpoints,
        Dictionary<string, long> counts,
        int chunkMinutes,
        CancellationToken ct)
    {
        // O manifesto e o que torna o dataset citavel no artigo: registra de
        // qual sessao os dados vieram, quando foram coletados e quantos
        // eventos cada endpoint rendeu.
        var manifest = new
        {
            session_key = sessionKey,
            session = new
            {
                name = Text(session, "session_name"),
                type = Text(session, "session_type"),
                country = Text(session, "country_name"),
                circuit = Text(session, "circuit_short_name"),
                year = session.TryGetProperty("year", out var y) ? y.GetInt32() : (int?)null,
                date_start = Text(session, "date_start"),
                date_end = Text(session, "date_end")
            },
            endpoints,
            event_counts = counts,
            chunk_minutes = chunkMinutes,
            source = "https://openf1.org",
            downloaded_at = DateTimeOffset.UtcNow
        };

        var json = JsonSerializer.Serialize(manifest, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(Path.Combine(sessionDir, "manifest.json"), json, ct);
    }

    private static double EstimateMinutes(int requests) => requests / 30.0;

    private static string Iso(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss");

    private static string Describe(JsonElement session) =>
        $"{Text(session, "country_name")} / {Text(session, "session_name")} ({Text(session, "year")})";

    private static string Text(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) ? value.ToString() : "";
}
