using System.Globalization;
using System.Text.Json;
using Pitwall.Processing.Core;
using Pitwall.Workload;

// Prepara os dados do replay visual (tools/race-replay/web) a partir do que o
// openf1-downloader gravou em data/raw. Uma saida por corrida, mais um indice.
//
// Nada daqui participa do experimento: e uma ferramenta de visualizacao, para
// explicar o workload e conferir a olho que o Processing.Core detecta
// frenagens onde elas fisicamente acontecem, na entrada das curvas.
//
//   dotnet run -c Release -- --raw ../../data/raw --out web/data

var raw = "data/raw";
var output = "tools/race-replay/web/data";

for (var i = 0; i < args.Length - 1; i++)
{
    if (args[i] == "--raw") raw = args[++i];
    else if (args[i] == "--out") output = args[++i];
}

Directory.CreateDirectory(output);
var index = new List<SessionInfo>();

foreach (var dir in Directory.GetDirectories(raw).OrderBy(d => d))
{
    if (!File.Exists(Path.Combine(dir, "car_data.jsonl")) || !File.Exists(Path.Combine(dir, "location.jsonl")))
    {
        continue;
    }

    var info = ReplayBuilder.Build(dir, output);
    if (info is not null)
    {
        index.Add(info);
    }
}

await File.WriteAllTextAsync(
    Path.Combine(output, "index.json"),
    JsonSerializer.Serialize(index.OrderBy(s => s.Date), ReplayBuilder.Json));

Console.WriteLine($"{index.Count} corridas em {Path.GetFullPath(output)}");

internal sealed record SessionInfo(int Key, string Name, string Circuit, string Country, int Year, string Date, string File);

internal static class ReplayBuilder
{
    public static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public static SessionInfo? Build(string dir, string output)
    {
        using var manifest = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "manifest.json")));
        var session = manifest.RootElement.GetProperty("session");
        var key = manifest.RootElement.GetProperty("session_key").GetInt32();

        var telemetry = TelemetryDataset.Load(Path.Combine(dir, "car_data.jsonl")).Events;
        var locations = LoadLocations(Path.Combine(dir, "location.jsonl"));

        // Frenagens detectadas pela MESMA classe que roda nas seis variantes.
        // Com janela de 1 ms cada amostra fica na propria janela, e a janela
        // que contar uma frenagem aponta o instante exato em que ela ocorreu.
        // A deteccao nao depende do tamanho da janela (o estado do carro
        // atravessa as janelas); o total e conferido contra a janela de 1 s,
        // a mesma da matriz.
        var brakings = new Dictionary<int, List<long>>();
        var fine = new TelemetryProcessor(new ProcessingOptions { WindowSize = TimeSpan.FromMilliseconds(1) }, w =>
        {
            for (var k = 0; k < w.HardBrakings; k++)
            {
                GetOrAdd(brakings, w.DriverNumber).Add(w.WindowStart.ToUnixTimeMilliseconds());
            }
        });

        long totalCoarse = 0;
        var coarse = new TelemetryProcessor(new ProcessingOptions(), w => totalCoarse += w.HardBrakings);

        foreach (var evt in telemetry)
        {
            fine.Process(evt);
            coarse.Process(evt);
        }

        fine.Complete();
        coarse.Complete();

        var totalFine = brakings.Values.Sum(l => l.Count);
        if (totalFine != totalCoarse)
        {
            throw new InvalidOperationException(
                $"Sessao {key}: {totalFine} frenagens com janela de 1 ms e {totalCoarse} com 1 s. A deteccao deveria ser a mesma.");
        }

        var drivers = LoadDrivers(Path.Combine(dir, "drivers.json"));
        var laps = LoadLaps(Path.Combine(dir, "laps.json"));
        var positions = LoadPositions(Path.Combine(dir, "position.json"));

        var t0 = Math.Min(telemetry[0].EventTime.ToUnixTimeMilliseconds(), locations.Min(l => l.T));
        var tEnd = Math.Max(telemetry[^1].EventTime.ToUnixTimeMilliseconds(), locations.Max(l => l.T));

        // Largada: inicio da volta 1. A OpenF1 costuma deixar a volta 1 sem
        // data; nesse caso, inicio da volta 2 menos a duracao da volta 1.
        var raceStart = laps.Where(l => l.Number == 1 && l.Start is not null).Select(l => l.Start!.Value).DefaultIfEmpty(0).Min();
        if (raceStart == 0)
        {
            raceStart = laps.Where(l => l.Number == 2 && l.Start is not null)
                .Select(l => l.Start!.Value - (long)((laps.FirstOrDefault(x => x.Driver == l.Driver && x.Number == 1)?.Duration ?? 100) * 1000))
                .DefaultIfEmpty(t0).Min();
        }

        var byDriverLoc = locations.GroupBy(l => l.Driver).ToDictionary(g => g.Key, g => g.OrderBy(l => l.T).ToArray());
        var track = BuildTrack(laps, byDriverLoc);

        var file = $"{key}.json";
        using (var stream = File.Create(Path.Combine(output, file)))
        using (var w = new Utf8JsonWriter(stream))
        {
            w.WriteStartObject();

            w.WriteStartObject("session");
            w.WriteNumber("key", key);
            w.WriteString("name", Text(session, "name"));
            w.WriteString("circuit", Text(session, "circuit"));
            w.WriteString("country", Text(session, "country"));
            w.WriteNumber("year", session.TryGetProperty("year", out var y) && y.ValueKind == JsonValueKind.Number ? y.GetInt32() : 0);
            w.WriteEndObject();

            w.WriteNumber("t0", t0);
            w.WriteNumber("start", Math.Max(0, raceStart - t0 - 60_000));
            w.WriteNumber("end", tEnd - t0);
            w.WriteNumber("totalLaps", laps.Count == 0 ? 0 : laps.Max(l => l.Number));
            w.WriteNumber("brakeThreshold", new ProcessingOptions().BrakeThreshold);

            w.WriteStartArray("drivers");
            foreach (var n in byDriverLoc.Keys.Union(telemetry.Select(e => e.DriverNumber)).Distinct().OrderBy(n => n))
            {
                drivers.TryGetValue(n, out var d);
                w.WriteStartObject();
                w.WriteNumber("n", n);
                w.WriteString("code", d?.Code ?? n.ToString(CultureInfo.InvariantCulture));
                w.WriteString("name", d?.Name ?? $"Carro {n}");
                w.WriteString("team", d?.Team ?? "");
                w.WriteString("color", "#" + (d?.Color ?? "8a8f98"));
                w.WriteEndObject();
            }
            w.WriteEndArray();

            // Pontos da pista, em coordenadas da OpenF1 (decimetros).
            w.WriteStartArray("track");
            foreach (var p in track)
            {
                w.WriteNumberValue(p.X);
                w.WriteNumberValue(p.Y);
            }
            w.WriteEndArray();

            // Series por carro com codificacao delta: tempo em ms desde t0,
            // posicao em decimetros. Reduz o arquivo a cerca de um terco.
            w.WriteStartObject("loc");
            foreach (var (driver, points) in byDriverLoc)
            {
                w.WriteStartObject(driver.ToString(CultureInfo.InvariantCulture));
                WriteDeltas(w, "t", points.Select(p => p.T - t0));
                WriteDeltas(w, "x", points.Select(p => (long)p.X));
                WriteDeltas(w, "y", points.Select(p => (long)p.Y));
                w.WriteEndObject();
            }
            w.WriteEndObject();

            w.WriteStartObject("tel");
            foreach (var group in telemetry.GroupBy(e => e.DriverNumber))
            {
                var samples = group.ToArray();
                w.WriteStartObject(group.Key.ToString(CultureInfo.InvariantCulture));
                WriteDeltas(w, "t", samples.Select(e => e.EventTime.ToUnixTimeMilliseconds() - t0));
                WriteValues(w, "speed", samples.Select(e => (long)e.Speed));
                WriteValues(w, "rpm", samples.Select(e => (long)e.Rpm));
                WriteValues(w, "gear", samples.Select(e => (long)e.Gear));
                WriteValues(w, "throttle", samples.Select(e => (long)e.Throttle));
                WriteValues(w, "brake", samples.Select(e => (long)e.Brake));
                WriteValues(w, "drs", samples.Select(e => (long)e.Drs));
                w.WriteEndObject();
            }
            w.WriteEndObject();

            w.WriteStartObject("brakings");
            foreach (var (driver, times) in brakings)
            {
                WriteDeltas(w, driver.ToString(CultureInfo.InvariantCulture), times.Order().Select(t => t - t0));
            }
            w.WriteEndObject();

            // [tempo, carro, posicao], em ordem de tempo.
            w.WriteStartArray("positions");
            foreach (var p in positions.OrderBy(p => p.T))
            {
                w.WriteNumberValue(p.T - t0);
                w.WriteNumberValue(p.Driver);
                w.WriteNumberValue(p.Place);
            }
            w.WriteEndArray();

            // [tempo, carro, volta], so voltas com inicio conhecido.
            w.WriteStartArray("laps");
            foreach (var l in laps.Where(l => l.Start is not null).OrderBy(l => l.Start))
            {
                w.WriteNumberValue(l.Start!.Value - t0);
                w.WriteNumberValue(l.Driver);
                w.WriteNumberValue(l.Number);
            }
            w.WriteEndArray();

            w.WriteEndObject();
        }

        var size = new FileInfo(Path.Combine(output, file)).Length / 1024.0 / 1024.0;
        Console.WriteLine(
            $"{key} {Text(session, "country")}: {telemetry.Length:N0} amostras de telemetria, {locations.Count:N0} de posicao, " +
            $"{totalFine:N0} frenagens, {track.Count} pontos de pista, {size:0.0} MB");

        return new SessionInfo(key, Text(session, "name"), Text(session, "circuit"), Text(session, "country"),
            session.TryGetProperty("year", out var yy) && yy.ValueKind == JsonValueKind.Number ? yy.GetInt32() : 0,
            Text(session, "date_start"), file);
    }

    /// <summary>
    /// Contorno da pista: a trajetoria da volta mais rapida da corrida, que
    /// e uma volta limpa, sem entrada nem saida de box. Sem dados de volta,
    /// cai para dois minutos do carro com mais amostras.
    /// </summary>
    private static List<(int X, int Y)> BuildTrack(List<LapRecord> laps, Dictionary<int, Location[]> byDriver)
    {
        // A posicao tem lacunas em algumas corridas: tenta as voltas da mais
        // rapida para a mais lenta ate achar uma coberta por amostras
        // (~3,7 Hz, entao uma volta de 80 s tem perto de 300 pontos).
        var candidates = laps
            .Where(l => l.Start is not null && l.Duration is > 60 && !l.PitOut && byDriver.ContainsKey(l.Driver))
            .OrderBy(l => l.Duration)
            .Take(200);

        foreach (var lap in candidates)
        {
            var from = lap.Start!.Value;
            var to = from + (long)(lap.Duration!.Value * 1000) + 300;
            var points = byDriver[lap.Driver].Where(p => p.T >= from && p.T <= to).ToList();

            if (points.Count >= lap.Duration!.Value * 3)
            {
                return points.Select(p => (p.X, p.Y)).ToList();
            }
        }

        var densest = byDriver.MaxBy(kv => kv.Value.Length).Value;
        var mid = densest[densest.Length / 2].T;
        return densest.Where(p => p.T >= mid && p.T <= mid + 120_000).Select(p => (p.X, p.Y)).ToList();
    }

    private static List<Location> LoadLocations(string path)
    {
        var list = new List<Location>(capacity: 500_000);
        foreach (var line in File.ReadLines(path))
        {
            if (line.Length == 0) continue;
            using var doc = JsonDocument.Parse(line);
            var r = doc.RootElement;
            var x = r.GetProperty("x").GetInt32();
            var y = r.GetProperty("y").GetInt32();

            // (0, 0) e o valor que a OpenF1 usa quando o carro nao tem
            // posicao (antes de sair do box ou depois de abandonar).
            if (x == 0 && y == 0) continue;

            list.Add(new Location(
                r.GetProperty("driver_number").GetInt32(),
                r.GetProperty("date").GetDateTimeOffset().ToUnixTimeMilliseconds(),
                x, y));
        }
        return list;
    }

    private static Dictionary<int, Driver> LoadDrivers(string path)
    {
        var result = new Dictionary<int, Driver>();
        if (!File.Exists(path)) return result;

        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        foreach (var d in doc.RootElement.EnumerateArray())
        {
            var n = d.GetProperty("driver_number").GetInt32();
            result[n] = new Driver(Text(d, "name_acronym"), Text(d, "full_name"), Text(d, "team_name"),
                string.IsNullOrEmpty(Text(d, "team_colour")) ? null : Text(d, "team_colour"));
        }
        return result;
    }

    private static List<LapRecord> LoadLaps(string path)
    {
        var result = new List<LapRecord>();
        if (!File.Exists(path)) return result;

        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        foreach (var l in doc.RootElement.EnumerateArray())
        {
            result.Add(new LapRecord(
                l.GetProperty("driver_number").GetInt32(),
                l.GetProperty("lap_number").GetInt32(),
                l.TryGetProperty("date_start", out var s) && s.ValueKind == JsonValueKind.String ? s.GetDateTimeOffset().ToUnixTimeMilliseconds() : null,
                l.TryGetProperty("lap_duration", out var dur) && dur.ValueKind == JsonValueKind.Number ? dur.GetDouble() : null,
                l.TryGetProperty("is_pit_out_lap", out var pit) && pit.ValueKind == JsonValueKind.True));
        }
        return result;
    }

    private static List<PositionRecord> LoadPositions(string path)
    {
        var result = new List<PositionRecord>();
        if (!File.Exists(path)) return result;

        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        foreach (var p in doc.RootElement.EnumerateArray())
        {
            result.Add(new PositionRecord(
                p.GetProperty("driver_number").GetInt32(),
                p.GetProperty("date").GetDateTimeOffset().ToUnixTimeMilliseconds(),
                p.GetProperty("position").GetInt32()));
        }
        return result;
    }

    private static void WriteDeltas(Utf8JsonWriter w, string name, IEnumerable<long> values)
    {
        w.WriteStartArray(name);
        long previous = 0;
        foreach (var v in values)
        {
            w.WriteNumberValue(v - previous);
            previous = v;
        }
        w.WriteEndArray();
    }

    private static void WriteValues(Utf8JsonWriter w, string name, IEnumerable<long> values)
    {
        w.WriteStartArray(name);
        foreach (var v in values) w.WriteNumberValue(v);
        w.WriteEndArray();
    }

    private static List<long> GetOrAdd(Dictionary<int, List<long>> map, int key)
    {
        if (!map.TryGetValue(key, out var list))
        {
            list = [];
            map[key] = list;
        }
        return list;
    }

    private static string Text(JsonElement e, string property) =>
        e.TryGetProperty(property, out var v) && v.ValueKind != JsonValueKind.Null ? v.ToString() : "";

    private sealed record Location(int Driver, long T, int X, int Y);
    private sealed record Driver(string Code, string Name, string Team, string? Color);
    private sealed record LapRecord(int Driver, int Number, long? Start, double? Duration, bool PitOut);
    private sealed record PositionRecord(int Driver, long T, int Place);
}
