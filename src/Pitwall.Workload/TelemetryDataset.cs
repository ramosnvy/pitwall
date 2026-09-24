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

    /// <summary>
    /// Trecho da corrida de <paramref name="length"/> a partir de
    /// <paramref name="start"/>, contados do primeiro evento.
    ///
    /// Com a frota compensando a taxa alvo, uma rodada consome da corrida so o
    /// tempo que ela dura. Recortar a janela permite reamostrar a 100 Hz sem
    /// materializar a corrida inteira (~12 milhoes de eventos), e deixa as
    /// duas frequencias comparaveis sobre o mesmo trecho.
    /// </summary>
    public TelemetryDataset Window(TimeSpan start, TimeSpan length)
    {
        var from = Events[0].EventTime + start;
        var to = from + length;

        var slice = Events
            .Where(e => e.EventTime >= from && e.EventTime < to)
            .Select((e, i) => e with { Sequence = i })
            .ToArray();

        if (slice.Length == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(start), "A janela nao contem nenhum evento.");
        }

        return new TelemetryDataset(slice, SessionKey);
    }

    /// <summary>Reamostra cada carro para <paramref name="hz"/>; ver <see cref="TelemetryInterpolator"/>.</summary>
    public TelemetryDataset Upsample(double hz, TimeSpan? maxGap = null) =>
        new(TelemetryInterpolator.Upsample(Events, hz, maxGap), SessionKey);

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
                DriverNumber = GetInt(root, "driver_number"),
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
