using Pitwall.Tools.OpenF1Downloader;

// Coleta unica do dataset de telemetria usado nos experimentos.
//
// Os experimentos NUNCA chamam a OpenF1: os dados sao baixados uma vez para
// disco e reproduzidos localmente pelo replayer. Isso mantem o workload
// reproduzivel e respeita os limites da API (3 req/s, 30 req/min no plano
// gratuito).

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\nInterrompido. Nenhum arquivo parcial foi promovido.");
};

var options = CommandLineOptions.Parse(args);
if (options is null)
{
    CommandLineOptions.PrintUsage();
    return 1;
}

using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(120) };
http.DefaultRequestHeaders.UserAgent.ParseAdd("pitwall-openf1-downloader/1.0");

var limiter = new RateLimiter(perSecond: 3, perMinute: 30);
var client = new OpenF1Client(http, limiter);

try
{
    if (options.ListSessions is { } year)
    {
        return await ListSessionsAsync(client, year, cts.Token);
    }

    var downloader = new SessionDownloader(client, options.OutputRoot);
    return await downloader.DownloadAsync(
        options.SessionKey!.Value,
        options.Endpoints,
        options.ChunkMinutes,
        cts.Token);
}
catch (OperationCanceledException)
{
    return 130;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Erro: {ex.Message}");
    return 1;
}

static async Task<int> ListSessionsAsync(OpenF1Client client, int year, CancellationToken ct)
{
    var sessions = await client.GetJsonAsync("sessions", $"year={year}", ct);

    Console.WriteLine($"{"key",-8} {"pais",-22} {"sessao",-18} inicio");
    Console.WriteLine(new string('-', 70));

    foreach (var s in sessions)
    {
        var key = s.GetProperty("session_key").GetInt32();
        var country = s.TryGetProperty("country_name", out var c) ? c.ToString() : "";
        var name = s.TryGetProperty("session_name", out var n) ? n.ToString() : "";
        var start = s.TryGetProperty("date_start", out var d) ? d.ToString() : "";

        Console.WriteLine($"{key,-8} {country,-22} {name,-18} {start}");
    }

    Console.WriteLine();
    Console.WriteLine($"{sessions.Length} sessoes em {year}.");
    return 0;
}

internal sealed record CommandLineOptions
{
    public int? SessionKey { get; init; }
    public int? ListSessions { get; init; }
    public string[] Endpoints { get; init; } = ["car_data"];
    public string OutputRoot { get; init; } = "data/raw";
    public int ChunkMinutes { get; init; } = 10;

    public static CommandLineOptions? Parse(string[] args)
    {
        int? sessionKey = null;
        int? listSessions = null;
        string[] endpoints = ["car_data"];
        var output = "data/raw";
        var chunk = 10;

        for (var i = 0; i < args.Length; i++)
        {
            var next = i + 1 < args.Length ? args[i + 1] : null;

            switch (args[i])
            {
                case "--session-key" when int.TryParse(next, out var sk):
                    sessionKey = sk;
                    i++;
                    break;

                case "--list-sessions" when int.TryParse(next, out var year):
                    listSessions = year;
                    i++;
                    break;

                case "--endpoints" when next is not null:
                    endpoints = next.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                    i++;
                    break;

                case "--out" when next is not null:
                    output = next;
                    i++;
                    break;

                case "--chunk-minutes" when int.TryParse(next, out var minutes) && minutes > 0:
                    chunk = minutes;
                    i++;
                    break;

                case "--help" or "-h":
                    return null;

                default:
                    Console.Error.WriteLine($"Argumento desconhecido: {args[i]}");
                    return null;
            }
        }

        if (sessionKey is null && listSessions is null)
        {
            return null;
        }

        return new CommandLineOptions
        {
            SessionKey = sessionKey,
            ListSessions = listSessions,
            Endpoints = endpoints,
            OutputRoot = output,
            ChunkMinutes = chunk
        };
    }

    public static void PrintUsage()
    {
        Console.WriteLine("""
            Coleta de telemetria historica da OpenF1 para os experimentos.

            Uso:
              dotnet run -- --list-sessions <ano>
              dotnet run -- --session-key <key> [opcoes]

            Opcoes:
              --session-key <n>      Sessao a baixar (use --list-sessions para descobrir)
              --list-sessions <ano>  Lista as sessoes do ano e sai
              --endpoints <a,b>      Endpoints a coletar (padrao: car_data)
                                     Ex.: car_data,location
              --out <dir>            Diretorio de saida (padrao: data/raw)
              --chunk-minutes <n>    Tamanho da janela de tempo por requisicao (padrao: 10)

            Saida:
              <out>/<session_key>/<endpoint>.jsonl  um evento por linha
              <out>/<session_key>/manifest.json     origem, contagens e data da coleta

            A coleta respeita 3 req/s e 30 req/min. Uma corrida completa com
            car_data leva cerca de 8 minutos. Arquivos ja existentes sao
            preservados: apague o .jsonl para recoletar.
            """);
    }
}
