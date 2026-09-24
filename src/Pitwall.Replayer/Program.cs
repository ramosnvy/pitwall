using System.Globalization;
using System.Diagnostics;
using Pitwall.Contracts;
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
var producerRunId = GetArg("--run-id") ?? "";

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

// Frequencia por carro. Sem --hz, a cadencia nativa da OpenF1 (3,7 Hz). Com
// --hz, cada carro e reamostrado por interpolacao (TelemetryInterpolator): os
// pontos inseridos nao sao medidos. Nos dois casos, --window-start recorta o
// trecho da corrida usado, para que as frequencias se comparem sobre o mesmo
// trecho; com --hz o recorte e obrigatorio, porque a corrida inteira a 100 Hz
// nao cabe na memoria do produtor.
var targetHz = GetArg("--hz") is { } hzArg ? double.Parse(hzArg, CultureInfo.InvariantCulture) : (double?)null;
var windowStart = GetArg("--window-start") is { } wsArg
    ? TimeSpan.FromSeconds(double.Parse(wsArg, CultureInfo.InvariantCulture))
    : (TimeSpan?)null;

if (windowStart is not null || targetHz is not null)
{
    var start = windowStart ?? TimeSpan.Zero;
    var length = warmup + duration + TimeSpan.FromSeconds(30);
    data = data.Window(start, length);
    Console.WriteLine($"Janela da corrida: {start.TotalSeconds:0} s a {(start + length).TotalSeconds:0} s | {data.Events.Length:N0} eventos");
}

if (targetHz is { } hz)
{
    var before = data.Events.Length;
    data = data.Upsample(hz);
    Console.WriteLine($"Reamostrado para {hz:0.#} Hz por carro: {before:N0} -> {data.Events.Length:N0} eventos (pontos interpolados, nao medidos)");
}

var samplePeriodMs = targetHz is { } h ? 1000.0 / h : FleetAmplifier.NativeSamplePeriodMs;

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
        // Sem a opcao, vale o padrao da librdkafka (docs/AUDITORIA-CONFIG.md).
        LingerMs = GetArg("--linger-ms") is { } linger ? double.Parse(linger, CultureInfo.InvariantCulture) : null,
        BatchSize = GetArg("--batch-size") is { } batch ? int.Parse(batch) : null,
        QueueBufferingMaxMessages = GetArg("--queue-max-messages") is { } queueMax ? int.Parse(queueMax) : null,
        SocketNagleDisable = ParseNagle()
    }),
    "rabbit" or "rabbitmq" => await RabbitMqSink.ConnectAsync(new RabbitMqSinkOptions
    {
        Host = GetArg("--rabbit-host") ?? "localhost",
        Queue = GetArg("--queue") ?? "telemetry",
        Partitions = int.Parse(GetArg("--partitions") ?? "4"),
        Persistent = !args.Contains("--transient"),
        ConfirmBatchSize = int.Parse(GetArg("--confirm-batch") ?? "1000"),
        ConnectionPerLane = !args.Contains("--single-connection"),
        LaneHash = LanePartitioner.Parse(GetArg("--lane-hash") ?? "crc32")
    }, cts.Token),
    _ => throw new ArgumentException(
        $"Destino '{sinkName}' desconhecido. Disponiveis: null, kafka, rabbit.")
};

Console.WriteLine();
Console.WriteLine($"Destino: {sink.Name} | alvo: {rate:N0} ev/s | " +
                  $"warmup: {warmup.TotalSeconds:0}s | medicao: {duration.TotalSeconds:0}s");
Console.WriteLine($"Cliente: {sink.EffectiveConfig}");

var replayer = new OpenLoopReplayer(data, sink);
// O laco de ritmo gira em espera ativa por design (malha aberta), entao ocupa
// uma thread o tempo todo. Numa thread do pool, num container com 2 CPUs, ele
// tomaria metade do minimo do pool e atrasaria as continuacoes de E/S do
// cliente do broker -- o mesmo esgotamento corrigido no consumidor. Por isso
// roda numa thread dedicada.
var replayOptions = new ReplayOptions
{
    TargetRate = rate, Duration = duration, Warmup = warmup, FleetFactor = fleet, MaxEvents = exactEvents,
    SamplePeriodMs = samplePeriodMs
};
var result = await Task.Factory.StartNew(
        () => replayer.RunAsync(replayOptions, cts.Token),
        CancellationToken.None,
        TaskCreationOptions.LongRunning,
        TaskScheduler.Default)
    .Unwrap();

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
        // run_id na primeira coluna: o script de experimento casa esta linha com
        // a do consumidor pela chave, e nao pela posicao no arquivo. Casar pela
        // ultima linha atribuiu a uma rodada, cujo produtor caiu sem gravar
        // relatorio, o jitter de outra rodada -- e ela pareceu valida.
        w.WriteLine("run_id,timestamp,sink,target_rate,achieved_rate,rate_error_pct,max_lateness_ms,events,fleet,dataset_laps," +
                    "client_config,dotnet_env,queue_full_waits");
    }

    w.WriteLine(string.Join(',',
        producerRunId,
        DateTimeOffset.UtcNow.ToString("o"),
        sink.Name,
        result.TargetRate,
        result.AchievedRate.ToString("0.0", CultureInfo.InvariantCulture),
        result.RateErrorPercent.ToString("0.000", CultureInfo.InvariantCulture),
        result.MaxLatenessMs.ToString("0.00", CultureInfo.InvariantCulture),
        result.EventsEmitted,
        fleet,
        result.DatasetLaps,
        sink.EffectiveConfig,
        RunSettings.DotnetEnvironment(),
        sink is KafkaSink k ? k.QueueFullWaits.ToString() : ""));
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

// --nagle on liga o Nagle (socket.nagle.disable=false), como nas rodadas
// anteriores a auditoria; --nagle off o desliga explicitamente. Sem a opcao,
// vale o padrao da librdkafka, que ja o desliga. --nagle-disable e o nome
// antigo de "off".
bool? ParseNagle() => GetArg("--nagle") switch
{
    "on" => false,
    "off" => true,
    null => args.Contains("--nagle-disable") ? true : null,
    var other => throw new ArgumentException($"--nagle aceita on ou off, nao '{other}'.")
};

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
      --linger-ms <n>      Kafka: espera de agrupamento (padrao da librdkafka: 5)
      --batch-size <n>     Kafka: lote em bytes (padrao da librdkafka: 1000000)
      --queue-max-messages <n>
                           Kafka: fila local do cliente (padrao: 100000)
      --nagle <on|off>    Kafka: liga ou desliga o Nagle (padrao da
                           librdkafka: desligado)
      --rabbit-host <host> RabbitMQ: servidor (padrao: localhost)
      --queue <nome>       RabbitMQ: fila (padrao: telemetry)
      --confirm-batch <n>  RabbitMQ: publicacoes em voo por faixa (padrao: 1000)
      --single-connection  RabbitMQ: uma conexao para todos os canais
      --lane-hash <f>      RabbitMQ: crc32 (padrao, igual ao Kafka) ou modulo
                           (carro % filas, como na matriz 7e283c2)
      --events <n>         Publica exatamente n eventos e encerra (verificacao)
      --transient          RabbitMQ: mensagens nao persistentes (ver docs)
      --fleet <n|auto>     Replicacao da frota: cada carro vira n carros com a
                           mesma cadencia de sensor (padrao: auto, que escolhe
                           o fator que entrega a taxa alvo sem acelerar o tempo)
      --max-events <n>     Limita quantos eventos carregar (testes rapidos)
      --hz <n>             Reamostra cada carro para n Hz por interpolacao
                           (padrao: cadencia nativa da OpenF1, 3,7 Hz). Os
                           pontos inseridos nao sao medidos; ver docs
      --window-start <s>   Usa o trecho da corrida a partir de s segundos, com
                           a duracao da rodada (obrigatorio o recorte com --hz)

    Codigo de saida 2 indica que o gerador nao sustentou a taxa alvo.
    """);
