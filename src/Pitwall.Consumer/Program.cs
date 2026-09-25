using System.Diagnostics;
using System.Globalization;
using HdrHistogram;
using Pitwall.Consumer;
using Pitwall.Consumer.Pipelines;
using Pitwall.Consumer.Sources;
using Pitwall.Contracts;
using Pitwall.Persistence;
using Pitwall.Processing.Core;

// Consumidor das seis variantes da matriz.
//
// A combinacao de --broker (kafka | rabbit) com --mode (direct | channels |
// pipelines) define a celula avaliada. Um unico executavel para as seis, e nao
// seis programas: qualquer diferenca acidental entre eles apareceria no
// resultado como se fosse diferenca de arquitetura.

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

if (args.Contains("--help") || args.Contains("-h"))
{
    PrintUsage();
    return 0;
}

var broker = GetArg("--broker") ?? "kafka";
var mode = GetArg("--mode") ?? "direct";
var partitions = int.Parse(GetArg("--partitions") ?? "4");
var capacity = int.Parse(GetArg("--capacity") ?? "10000");
var syntheticCost = double.Parse(GetArg("--synthetic-cost-us") ?? "0");
var idleSeconds = int.Parse(GetArg("--idle-timeout") ?? "10");
var reportPath = GetArg("--report");
var targetRate = int.Parse(GetArg("--target-rate") ?? "0");
var replication = int.Parse(GetArg("--replication") ?? "1");
var persist = args.Contains("--persist");
var truncate = args.Contains("--truncate");
var persistBatch = int.Parse(GetArg("--persist-batch") ?? "10000");
var warmupSeconds = int.Parse(GetArg("--warmup-seconds") ?? "10");
var expectedEvents = long.Parse(GetArg("--expected-events") ?? "0");

// Resolucao do timer do Windows (ver TimerResolution): o padrao de 15,6 ms
// penaliza a librdkafka, que agenda envios e buscas com esperas temporizadas.
var timerResolutionMs = uint.Parse(GetArg("--timer-resolution-ms") ?? "1");
using var timerResolution = TimerResolution.Request(timerResolutionMs);

var connectionString = GetArg("--conn")
    ?? "Host=localhost;Port=5432;Username=pitwall;Password=pitwall;Database=pitwall";

// O identificador da rodada pode vir do script de experimento, para que as
// linhas do produtor, do consumidor e das metricas dos containers possam ser
// unidas pela mesma chave na analise.
var runId = GetArg("--run-id") is { } rid ? Guid.Parse(rid) : Guid.NewGuid();
var processingOptions = new ProcessingOptions { SyntheticCostMicros = syntheticCost };
var digest = new ResultDigest();
var latency = new LatencyRecorder(partitions, TimeSpan.FromSeconds(warmupSeconds));

IEventSource source = broker switch
{
    "kafka" => new KafkaEventSource(new KafkaSourceOptions
    {
        BootstrapServers = GetArg("--bootstrap") ?? "localhost:9092",
        Topic = GetArg("--topic") ?? "telemetry",
        GroupId = GetArg("--group") ?? "pitwall",
        // Sem a opcao, vale o padrao da librdkafka (docs/AUDITORIA-CONFIG.md).
        FetchWaitMaxMs = GetArg("--fetch-wait-max-ms") is { } fetchWait ? int.Parse(fetchWait) : null,
        SocketNagleDisable = ParseNagle(),
        ExpectedEvents = expectedEvents,
        Partitions = partitions,
        IdleTimeout = TimeSpan.FromSeconds(idleSeconds)
    }),
    "rabbit" or "rabbitmq" => new RabbitEventSource(new RabbitSourceOptions
    {
        Host = GetArg("--rabbit-host") ?? "localhost",
        Queue = GetArg("--queue") ?? "telemetry",
        Partitions = partitions,
        // 0 = sem limite, o padrao do RabbitMQ.
        Prefetch = ushort.Parse(GetArg("--prefetch") ?? "0"),
        AckBatch = int.Parse(GetArg("--ack-batch") ?? "100"),
        ExpectedEvents = expectedEvents,
        IdleTimeout = TimeSpan.FromSeconds(idleSeconds)
    }),
    _ => throw new ArgumentException($"Broker desconhecido: {broker}. Use kafka ou rabbit.")
};

var architecture = source.Broker + "-" + mode;
var recorder = new ExperimentRunRecorder(connectionString);

if (truncate)
{
    // Rodada comeca com as tabelas de saida vazias: o acumulo entre rodadas
    // faria cada uma encontrar um banco diferente da anterior.
    await recorder.TruncateOutputsAsync(cts.Token);
}

WindowStatsWriter? writer = persist
    ? new WindowStatsWriter(connectionString, runId, architecture, persistBatch)
    : null;

// O destino das janelas fechadas. O digest e sempre alimentado, porque e o
// criterio de validade da rodada; a persistencia e opcional para permitir a
// bateria sem banco, que isola o custo do PostgreSQL.
Action<DriverWindowStats> onWindow = writer is null
    ? stats => digest.Add(stats)
    : stats =>
    {
        digest.Add(stats);
        writer.Enqueue(stats);
    };

// A capacidade e declarada em eventos e convertida para bytes na variante
// Pipelines, para que as duas tenham a mesma contrapressao em numero de
// eventos e nao em unidades diferentes.
await using IProcessingPipeline pipeline = mode switch
{
    "direct" => new DirectPipeline(partitions, processingOptions, onWindow, latency),
    "channels" => new ChannelsPipeline(partitions, capacity, processingOptions, onWindow, latency),
    "pipelines" => new PipelinesPipeline(
        partitions, capacity * TelemetryCodec.Size, processingOptions, onWindow, latency),
    _ => throw new ArgumentException($"Modo desconhecido: {mode}. Use direct, channels ou pipelines.")
};

if (persist)
{
    await recorder.StartAsync(
        runId, architecture, source.Broker, mode, targetRate, replication, persist,
        new Dictionary<string, object?>
        {
            ["partitions"] = partitions,
            ["capacity"] = capacity,
            ["synthetic_cost_us"] = syntheticCost,
            ["client_config"] = source.EffectiveConfig,
            ["dotnet_env"] = RunSettings.DotnetEnvironment(),
            ["warmup_seconds"] = warmupSeconds,
            ["machine"] = Environment.MachineName,
            ["cpu_count"] = Environment.ProcessorCount,
            ["dotnet"] = Environment.Version.ToString()
        },
        cts.Token);
}

Console.WriteLine($"Arquitetura: {architecture} | faixas: {partitions} | " +
                  $"capacidade: {capacity:N0} eventos | custo sintetico: {syntheticCost} us | " +
                  $"persistencia: {(persist ? "ligada" : "desligada")} | aquecimento: {warmupSeconds}s");
Console.WriteLine($"Rodada: {runId}");
Console.WriteLine($"Cliente: {source.EffectiveConfig} | runtime: {RunSettings.DotnetEnvironment()}");
Console.WriteLine($"Aguardando eventos (encerra apos {idleSeconds}s sem mensagem)...");
Console.WriteLine();

await using var resources = new ResourceSampler();

// Linha do tempo da rodada (fase 4): a cada 100 ms, eventos processados e a
// maior latencia do intervalo, com as pausas de coleta de lixo e o pool de
// threads que o RunTracer registra por conta propria.
RunTracer? tracer = null;
if (GetArg("--trace") is { } tracePath)
{
    tracer = new RunTracer(tracePath, TimeSpan.FromMilliseconds(100))
        .Counter("processed", () => latency.RecordedSoFar)
        .Gauge("latency_max_us", latency.TakeIntervalMaxMicros);
    tracer.Start();
}

var watch = Stopwatch.StartNew();
var received = await source.ConsumeAsync(pipeline, cts.Token);
await pipeline.CompleteAsync();
watch.Stop();
tracer?.Dispose();

if (writer is not null)
{
    await writer.DisposeAsync();
}

// A vazao e medida sobre a janela de medicao: numerador e denominador
// precisam cobrir o MESMO intervalo. Eventos medidos (sem aquecimento) sobre
// o tempo do primeiro ao ultimo evento medido (sem aquecimento e sem a espera
// ociosa que encerra a rodada).
var seconds = latency.ActiveSeconds;
var wallSeconds = watch.Elapsed.TotalSeconds;
var throughput = seconds > 0 ? latency.TotalCount / seconds : 0;

Console.WriteLine($"Eventos recebidos : {received:N0} ({latency.WarmupCount:N0} no aquecimento, {latency.TotalCount:N0} medidos)");
Console.WriteLine($"Tempo ativo       : {seconds:0.00}s (total {wallSeconds:0.00}s com a espera ociosa)");
Console.WriteLine($"Throughput        : {throughput:N0} ev/s");
Console.WriteLine($"Latencia          : {latency.Summary()}");
Console.WriteLine($"Recursos          : {resources.Summary()}");
Console.WriteLine($"Digest            : {digest}");

if (pipeline.IncompleteRecords > 0)
{
    Console.WriteLine($"AVISO: {pipeline.IncompleteRecords} registro(s) incompleto(s) ao encerrar. A rodada e invalida.");
}

if (writer is not null)
{
    Console.WriteLine($"Persistencia      : {writer.Written:N0} janelas gravadas, {writer.Dropped:N0} descartadas");

    if (writer.Dropped > 0)
    {
        Console.WriteLine("AVISO: houve descarte na fila de persistencia. A rodada serve para latencia e vazao, nao para completude.");
    }
}

if (persist)
{
    await recorder.FinishAsync(runId, notes: null, cts.Token);
}

if (reportPath is not null)
{
    WriteReport(reportPath, runId, architecture, partitions, targetRate, replication,
        received, seconds, throughput, latency, resources, digest, syntheticCost, persist,
        writer?.Written ?? 0, writer?.Dropped ?? 0,
        source.EffectiveConfig, RunSettings.DotnetEnvironment(), pipeline.IncompleteRecords);

    Console.WriteLine($"Relatorio         : {reportPath}");
}

return 0;

string? GetArg(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

// Mesma convencao do produtor: --nagle on liga o Nagle, off o desliga, e sem
// a opcao vale o padrao da librdkafka (desligado).
bool? ParseNagle() => GetArg("--nagle") switch
{
    "on" => false,
    "off" => true,
    null => args.Contains("--nagle-disable") ? true : null,
    var other => throw new ArgumentException($"--nagle aceita on ou off, nao '{other}'.")
};

static void WriteReport(
    string path,
    Guid runId,
    string architecture,
    int partitions,
    int targetRate,
    int replication,
    long received,
    double seconds,
    double throughput,
    LatencyRecorder latency,
    ResourceSampler resources,
    ResultDigest digest,
    double syntheticCost,
    bool persist,
    long windowsWritten,
    long windowsDropped,
    string clientConfig,
    string dotnetEnv,
    long incompleteRecords)
{
    var histogram = latency.Merged();
    var exists = File.Exists(path);

    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);

    using var writer = new StreamWriter(path, append: true);

    if (!exists)
    {
        writer.WriteLine(
            "timestamp,run_id,architecture,partitions,target_rate,replication,synthetic_cost_us," +
            "persistence,events,seconds,throughput,mean_us,p50_us,p95_us,p99_us,max_us," +
            "cpu_avg,cpu_peak,mem_avg_mb,mem_peak_mb,gc_gen0,gc_gen1,gc_gen2,allocated_mb," +
            "windows,digest_hash,warmup_events,measured_events,windows_written,windows_dropped," +
            "client_config,dotnet_env,incomplete_records");
    }

    var gc = resources.Collections;

    writer.WriteLine(string.Join(',',
        DateTimeOffset.UtcNow.ToString("o"),
        runId,
        architecture,
        partitions,
        targetRate,
        replication,
        syntheticCost.ToString(CultureInfo.InvariantCulture),
        persist ? 1 : 0,
        received,
        seconds.ToString("0.000", CultureInfo.InvariantCulture),
        throughput.ToString("0.0", CultureInfo.InvariantCulture),
        histogram.GetMean().ToString("0.0", CultureInfo.InvariantCulture),
        histogram.GetValueAtPercentile(50),
        histogram.GetValueAtPercentile(95),
        histogram.GetValueAtPercentile(99),
        histogram.GetMaxValue(),
        resources.AverageCpuPercent.ToString("0.0", CultureInfo.InvariantCulture),
        resources.PeakCpuPercent.ToString("0.0", CultureInfo.InvariantCulture),
        resources.AverageMemoryMb.ToString("0.0", CultureInfo.InvariantCulture),
        resources.PeakMemoryMb.ToString("0.0", CultureInfo.InvariantCulture),
        gc.Gen0,
        gc.Gen1,
        gc.Gen2,
        resources.AllocatedMb,
        digest.Windows,
        digest.Hash.ToString("X16"),
        latency.WarmupCount,
        latency.TotalCount,
        windowsWritten,
        windowsDropped,
        clientConfig,
        dotnetEnv,
        incompleteRecords));
}

static void PrintUsage() => Console.WriteLine("""
    Consumidor das seis variantes da matriz.

    Uso:
      dotnet run -- --broker kafka --mode channels [opcoes]

    Opcoes:
      --broker <nome>          kafka | rabbit
      --mode <nome>            direct | channels | pipelines
      --partitions <n>         Grau de paralelismo: particoes do Kafka ou
                               filas do RabbitMQ (padrao: 4)
      --capacity <n>           Eventos em transito antes da contrapressao
                               (padrao: 10000)
      --synthetic-cost-us <n>  Custo sintetico por evento em microssegundos
                               (padrao: 0, desligado)
      --idle-timeout <s>       Silencio que encerra a rodada (padrao: 10)
      --report <arquivo>       Acrescenta uma linha CSV com o resultado
      --persist                Grava as janelas no PostgreSQL
      --truncate               Esvazia as tabelas de saida antes de comecar
      --persist-batch <n>      Linhas por COPY (padrao: 10000)
      --warmup-seconds <n>     Aquecimento descartado da medicao (padrao: 10)
      --expected-events <n>    Total que o produtor publicara; a rodada termina
                               ao receber todos (o tempo ocioso vira rede de
                               seguranca global, nao por particao)
      --run-id <guid>          Identificador da rodada, vindo do script
      --ack-batch <n>          RabbitMQ: entregas por confirmacao (padrao: 100)
      --conn <string>          Conexao do PostgreSQL
      --target-rate <n>        Taxa alvo da rodada, para o registro
      --replication <n>        Numero da repeticao, para o registro
      --bootstrap <host>       Kafka (padrao: localhost:9092)
      --topic <nome>           Kafka (padrao: telemetry)
      --group <nome>           Kafka (padrao: pitwall)
      --rabbit-host <host>     RabbitMQ (padrao: localhost)
      --queue <prefixo>        RabbitMQ (padrao: telemetry)
      --prefetch <n>           RabbitMQ; 0 = sem limite (padrao do RabbitMQ)
      --fetch-wait-max-ms <n>  Kafka (padrao da librdkafka: 500)
      --nagle <on|off>         Kafka: liga ou desliga o Nagle (padrao da
                               librdkafka: desligado)
    """);
