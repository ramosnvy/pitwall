using System.Text.Json;
using Pitwall.Contracts;

namespace Pitwall.Workload;

/// <summary>
/// Carrega para a memoria o dataset coletado pelo openf1-downloader.
///
/// O arquivo inteiro e lido antes de a medicao comecar: ler do disco durante
/// o experimento adicionaria I/O ao caminho quente e a variacao do disco
/// entraria nas medias de latencia.
/// </summary>
public sealed class TelemetryDataset
{
    private TelemetryDataset(TelemetryEvent[] events, int sessionKey)
    {
        Events = events;
        SessionKey = sessionKey;
    }

    public TelemetryEvent[] Events { get; }
    public int SessionKey { get; }

    public static TelemetryDataset Load(string jsonlPath, int? maxEvents = null)
    {
        if (!File.Exists(jsonlPath))
        {
            throw new FileNotFoundException(
                $"Dataset nao encontrado em {jsonlPath}. Rode o openf1-downloader primeiro.",
                jsonlPath);
        }

        var events = new List<TelemetryEvent>(capacity: 600_000);
        var sessionKey = 0;
        long sequence = 0;

        foreach (var line in File.ReadLines(jsonlPath))
        {
            if (line.Length == 0)
            {
                continue;
            }

            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;

            sessionKey = GetInt(root, "session_key");

            events.Add(new TelemetryEvent
            {
                Sequence = sequence++,
                SessionKey = sessionKey,
                DriverNumber = (short)GetInt(root, "driver_number"),
                EventTime = GetDate(root, "date"),
                PublishedTicks = 0,   // preenchido no momento da publicacao
                Speed = (short)GetInt(root, "speed"),
                Rpm = GetInt(root, "rpm"),
                Gear = (short)GetInt(root, "n_gear"),
                Throttle = (short)GetInt(root, "throttle"),
                Brake = (short)GetInt(root, "brake"),
                Drs = (short)GetInt(root, "drs")
            });

            if (maxEvents is { } limit && events.Count >= limit)
            {
                break;
            }
        }

        // A OpenF1 e consultada por piloto, entao o arquivo vem agrupado por
        // piloto. Ordenar por tempo reconstroi a corrida como ela aconteceu:
        // sem isso o replayer emitiria um piloto inteiro de cada vez.
        var ordered = events.OrderBy(e => e.EventTime).ToArray();

        for (var i = 0; i < ordered.Length; i++)
        {
            ordered[i] = ordered[i] with { Sequence = i };
        }

        return new TelemetryDataset(ordered, sessionKey);
    }

    private static int GetInt(JsonElement element, string property) =>
        element.TryGetProperty(property, out var v) && v.ValueKind == JsonValueKind.Number
            ? v.GetInt32()
            : 0;

    private static DateTimeOffset GetDate(JsonElement element, string property) =>
        element.TryGetProperty(property, out var v) && v.TryGetDateTimeOffset(out var d)
            ? d
            : DateTimeOffset.MinValue;
}
