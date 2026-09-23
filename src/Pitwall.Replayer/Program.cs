using System.Globalization;
using System.Diagnostics;
using Pitwall.Workload;
using Pitwall.Replayer.Sinks;

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
var fleetArg = GetArg("--fleet") ?? "auto";
var exactEvents = GetArg("--events") is { } e ? long.Parse(e) : (long?)null;
var producerReport = GetArg("--report");

// Resolucao do timer do Windows (ver TimerResolution no Contracts).
var timerResolutionMs = uint.Parse(GetArg("--timer-resolution-ms") ?? "1");
using var timerResolution = Pitwall.Contracts.TimerResolution.Request(timerResolutionMs);

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

var span = data.Events[^1].EventTime - data.Events[0].EventTime;
var naturalRate = data.Events.Length / span.TotalSeconds;
var distinctDrivers = data.Events.Select(e => e.DriverNumber).Distinct().Count();

// O fator de frota que entrega a taxa alvo mantendo a cadencia real de cada
// sensor. Em "auto" o replayer escolhe esse valor, de modo que a carga venha
// de haver mais carros e nao de acelerar o tempo.
var recommendedFleet = Math.Max(1, (int)Math.Round(rate / naturalRate));
var fleet = fleetArg == "auto" ? recommendedFleet : int.Parse(fleetArg);

// O que a taxa alvo ainda exige alem da frota. Em 1,0 o tempo corre na
// velocidade real; acima disso a corrida esta sendo acelerada, o que
// distorce o intervalo entre amostras de um mesmo carro.
var residualCompression = rate / (naturalRate * fleet);

Console.WriteLine(
    $"Duracao original: {span.TotalMinutes:0} min | " +
    $"{distinctDrivers} carros | " +
    $"taxa natural: {naturalRate:0} ev/s");
Console.WriteLine(
    $"Frota: {fleet}x = {distinctDrivers * fleet:N0} carros | " +
    $"compressao temporal residual: {residualCompression:0.00}x" +
    (fleetArg == "auto" ? " (fator escolhido automaticamente)" : $" (recomendado: {recommendedFleet}x)"));

IEventSink sink = sinkName switch
{
    "null" => new NullSink(),
    "kafka" => new KafkaSink(new KafkaSinkOptions
    {
        BootstrapServers = GetArg("--bootstrap") ?? "localhost:9092",
        Topic = GetArg("--topic") ?? "telemetry",
        LingerMs = double.Parse(GetArg("--linger-ms") ?? "5"),
        BatchSize = int.Parse(GetArg("--batch-size") ?? "65536")
    }),
    "rabbit" or "rabbitmq" => await RabbitMqSink.ConnectAsync(new RabbitMqSinkOptions
    {
        Host = GetArg("--rabbit-host") ?? "localhost",
        Queue = GetArg("--queue") ?? "telemetry",
        Partitions = int.Parse(GetArg("--partitions") ?? "4"),
        Persistent = !args.Contains("--transient"),
        ConfirmBatchSize = int.Parse(GetArg("--confirm-batch") ?? "1000"),
        ConnectionPerLane = !args.Contains("--single-connection")
    }, cts.Token),
    _ => throw new ArgumentException(
        $"Destino '{sinkName}' desconhecido. Disponiveis: null, kafka, rabbit.")
};

Console.WriteLine();
Console.WriteLine($"Destino: {sink.Name} | alvo: {rate:N0} ev/s | " +
                  $"warmup: {warmup.TotalSeconds:0}s | medicao: {duration.TotalSeconds:0}s");

var replayer = new OpenLoopReplayer(data, sink);
var result = await replayer.RunAsync(
    new ReplayOptions { TargetRate = rate, Duration = duration, Warmup = warmup, FleetFactor = fleet, MaxEvents = exactEvents },
    cts.Token);

await sink.DisposeAsync();


Console.WriteLine();
Console.WriteLine($"Eventos emitidos : {result.EventsEmitted:N0}");
Console.WriteLine($"Taxa obtida      : {result.AchievedRate:N0} ev/s " +
                  $"({result.RateErrorPercent:+0.00;-0.00;0.00}% do alvo)");
Console.WriteLine($"Atraso maximo    : {result.MaxLatenessMs:0.00} ms");
Console.WriteLine($"Ciclos do dataset: {result.DatasetLaps}");

// A contagem confirmada pelo broker precisa bater com a emitida. Diferenca
// significa evento perdido, e uma rodada com perda nao entra na analise.
switch (sink)
{
    case KafkaSink kafka:
        Console.WriteLine($"Confirmados pelo broker: {kafka.Delivered:N0} | falhas: {kafka.Failed:N0}");
        break;
    case RabbitMqSink rabbit:
        Console.WriteLine($"Confirmados pelo broker: {rabbit.Published:N0}");
        break;
}

if (producerReport is not null)
{
    // Lado do produtor: taxa obtida e jitter de emissao. O RIoTBench mede a
    // diferenca entre a taxa esperada e a real como metrica propria.
    var exists = File.Exists(producerReport);
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(producerReport))!);

    using var w = new StreamWriter(producerReport, append: true);

    if (!exists)
    {
        w.WriteLine("timestamp,sink,target_rate,achieved_rate,rate_error_pct,max_lateness_ms,events,fleet,dataset_laps");
    }

    w.WriteLine(string.Join(',',
        DateTimeOffset.UtcNow.ToString("o"),
        sink.Name,
        result.TargetRate,
        result.AchievedRate.ToString("0.0", CultureInfo.InvariantCulture),
        result.RateErrorPercent.ToString("0.000", CultureInfo.InvariantCulture),
        result.MaxLatenessMs.ToString("0.00", CultureInfo.InvariantCulture),
        result.EventsEmitted,
        fleet,
        result.DatasetLaps));
}

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
      --sink <nome>        Destino dos eventos: null, kafka ou rabbit
      --bootstrap <host>   Kafka: servidor (padrao: localhost:9092)
      --topic <nome>       Kafka: topico (padrao: telemetry)
      --linger-ms <n>      Kafka: espera de agrupamento (padrao: 5)
      --batch-size <n>     Kafka: tamanho do lote em bytes (padrao: 65536)
      --rabbit-host <host> RabbitMQ: servidor (padrao: localhost)
      --queue <nome>       RabbitMQ: fila (padrao: telemetry)
      --confirm-batch <n>  RabbitMQ: publicacoes em voo por faixa (padrao: 1000)
      --single-connection  RabbitMQ: uma conexao para todos os canais
      --events <n>         Publica exatamente n eventos e encerra (verificacao)
      --transient          RabbitMQ: mensagens nao persistentes (ver docs)
      --fleet <n|auto>     Replicacao da frota: cada carro vira n carros com a
                           mesma cadencia de sensor (padrao: auto, que escolhe
                           o fator que entrega a taxa alvo sem acelerar o tempo)
      --max-events <n>     Limita quantos eventos carregar (testes rapidos)

    Codigo de saida 2 indica que o gerador nao sustentou a taxa alvo.
    """);
