using System.Diagnostics;
using Pitwall.Replayer;

// Gerador de carga dos experimentos.
//
// Reproduz o dataset coletado da OpenF1 na taxa alvo, em malha aberta. Por
// enquanto so com o destino nulo (--sink null), que serve para verificar ate
// que taxa a propria maquina sustenta a emissao. Os destinos Kafka e RabbitMQ
// entram quando o Docker estiver no ar.

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var dataset = GetArg("--dataset") ?? "data/raw/9472/car_data.jsonl";
var rate = int.Parse(GetArg("--rate") ?? "10000");
var duration = TimeSpan.FromSeconds(int.Parse(GetArg("--duration") ?? "30"));
var warmup = TimeSpan.FromSeconds(int.Parse(GetArg("--warmup") ?? "5"));
var sinkName = GetArg("--sink") ?? "null";
var maxEvents = GetArg("--max-events") is { } m ? int.Parse(m) : (int?)null;

if (args.Contains("--help") || args.Contains("-h"))
{
    PrintUsage();
    return 0;
}

Console.WriteLine($"Carregando {dataset}...");
var loadWatch = Stopwatch.StartNew();
var data = TelemetryDataset.Load(dataset, maxEvents);
loadWatch.Stop();

Console.WriteLine(
    $"{data.Events.Length:N0} eventos da sessao {data.SessionKey} " +
    $"carregados em {loadWatch.Elapsed.TotalSeconds:0.0}s");

if (data.Events.Length > 0)
{
    var span = data.Events[^1].EventTime - data.Events[0].EventTime;
    var naturalRate = data.Events.Length / span.TotalSeconds;
    Console.WriteLine(
        $"Duracao original: {span.TotalMinutes:0} min | " +
        $"taxa natural: {naturalRate:0} ev/s | " +
        $"fator de aceleracao para {rate:N0} ev/s: {rate / naturalRate:0.0}x");
}

IEventSink sink = sinkName switch
{
    "null" => new NullSink(),
    _ => throw new ArgumentException(
        $"Destino '{sinkName}' ainda nao implementado. Disponivel: null.")
};

Console.WriteLine();
Console.WriteLine($"Destino: {sink.Name} | alvo: {rate:N0} ev/s | " +
                  $"warmup: {warmup.TotalSeconds:0}s | medicao: {duration.TotalSeconds:0}s");

var replayer = new OpenLoopReplayer(data, sink);
var result = await replayer.RunAsync(
    new ReplayOptions { TargetRate = rate, Duration = duration, Warmup = warmup },
    cts.Token);

await sink.DisposeAsync();

Console.WriteLine();
Console.WriteLine($"Eventos emitidos : {result.EventsEmitted:N0}");
Console.WriteLine($"Taxa obtida      : {result.AchievedRate:N0} ev/s " +
                  $"({result.RateErrorPercent:+0.00;-0.00;0.00}% do alvo)");
Console.WriteLine($"Atraso maximo    : {result.MaxLatenessMs:0.00} ms");
Console.WriteLine($"Ciclos do dataset: {result.DatasetLaps}");

// Um atraso alto significa que o gerador nao conseguiu manter o ritmo: a
// rodada nao e comparavel com as outras e nao deve entrar na analise.
if (result.MaxLatenessMs > 50)
{
    Console.WriteLine();
    Console.WriteLine("AVISO: o gerador nao sustentou a taxa alvo. Reduza a taxa ou descarte esta rodada.");
    return 2;
}

return 0;

string? GetArg(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

static void PrintUsage() => Console.WriteLine("""
    Gerador de carga em malha aberta a partir do dataset da OpenF1.

    Uso:
      dotnet run -- [opcoes]

    Opcoes:
      --dataset <arquivo>  JSONL gerado pelo openf1-downloader
                           (padrao: data/raw/9472/car_data.jsonl)
      --rate <n>           Taxa alvo em eventos/s (padrao: 10000)
      --duration <s>       Duracao da janela de medicao (padrao: 30)
      --warmup <s>         Aquecimento descartado antes de medir (padrao: 5)
      --sink <nome>        Destino dos eventos (por enquanto: null)
      --max-events <n>     Limita quantos eventos carregar (testes rapidos)

    Codigo de saida 2 indica que o gerador nao sustentou a taxa alvo.
    """);
