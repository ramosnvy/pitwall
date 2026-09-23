using System.Diagnostics;
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

var connectionString = GetArg("--conn")
    ?? "Host=localhost;Port=5432;Username=pitwall;Password=pitwall;Database=pitwall";

var runId = Guid.NewGuid();
var processingOptions = new ProcessingOptions { SyntheticCostMicros = syntheticCost };
var digest = new ResultDigest();
var latency = new LatencyRecorder(partitions);

IEventSource source = broker switch
{
    "kafka" => new KafkaEventSource(new KafkaSourceOptions
    {
        BootstrapServers = GetArg("--bootstrap") ?? "localhost:9092",
        Topic = GetArg("--topic") ?? "telemetry",
        GroupId = GetArg("--group") ?? "pitwall",
        Partitions = partitions,
        IdleTimeout = TimeSpan.FromSeconds(idleSeconds)
    }),
    "rabbit" or "rabbitmq" => new RabbitEventSource(new RabbitSourceOptions
    {
        Host = GetArg("--rabbit-host") ?? "localhost",
        Queue = GetArg("--queue") ?? "telemetry",
        Partitions = partitions,
        Prefetch = ushort.Parse(GetArg("--prefetch") ?? "1000"),
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
            ["prefetch"] = GetArg("--prefetch") ?? "1000",
            ["machine"] = Environment.MachineName,
            ["cpu_count"] = Environment.ProcessorCount,
            ["dotnet"] = Environment.Version.ToString()
        },
        cts.Token);
}

Console.WriteLine($"Arquitetura: {architecture} | faixas: {partitions} | " +
                  $"capacidade: {capacity:N0} eventos | custo sintetico: {syntheticCost} us | " +
                  $"persistencia: {(persist ? "ligada" : "desligada")}");
Console.WriteLine($"Rodada: {runId}");
Console.WriteLine($"Aguardando eventos (encerra apos {idleSeconds}s sem mensagem)...");
Console.WriteLine();

await using var resources = new ResourceSampler();

var watch = Stopwatch.StartNew();
var received = await source.ConsumeAsync(pipeline, cts.Token);
await pipeline.CompleteAsync();
watch.Stop();

if (writer is not null)
{
    await writer.DisposeAsync();
}

// A vazao e medida sobre a janela ativa (do primeiro ao ultimo evento), nao
// sobre o tempo total, que inclui a espera ociosa que encerra a rodada.
var seconds = latency.ActiveSeconds;
var wallSeconds = watch.Elapsed.TotalSeconds;
var throughput = seconds > 0 ? received / seconds : 0;

Console.WriteLine($"Eventos recebidos : {received:N0}");
Console.WriteLine($"Tempo ativo       : {seconds:0.00}s (total {wallSeconds:0.00}s com a espera ociosa)");
Console.WriteLine($"Throughput        : {throughput:N0} ev/s");
Console.WriteLine($"Latencia          : {latency.Summary()}");
Console.WriteLine($"Recursos          : {resources.Summary()}");
Console.WriteLine($"Digest            : {digest}");

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
        received, seconds, throughput, latency, resources, digest, syntheticCost, persist);

    Console.WriteLine($"Relatorio         : {reportPath}");
}

return 0;

string? GetArg(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

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
    bool persist)
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
            "windows,digest_hash");
    }

    var gc = resources.Collections;

    writer.WriteLine(string.Join(',',
        DateTimeOffset.UtcNow.ToString("o"),
        runId,
        architecture,
        partitions,
        targetRate,
        replication,
        syntheticCost,
        persist ? 1 : 0,
        received,
        seconds.ToString("0.000"),
        throughput.ToString("0.0"),
        histogram.GetMean().ToString("0.0"),
        histogram.GetValueAtPercentile(50),
        histogram.GetValueAtPercentile(95),
        histogram.GetValueAtPercentile(99),
        histogram.GetMaxValue(),
        resources.AverageCpuPercent.ToString("0.0"),
        resources.PeakCpuPercent.ToString("0.0"),
        resources.AverageMemoryMb.ToString("0.0"),
        resources.PeakMemoryMb.ToString("0.0"),
        gc.Gen0,
        gc.Gen1,
        gc.Gen2,
        resources.AllocatedMb,
        digest.Windows,
        digest.Hash.ToString("X16")));
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
      --conn <string>          Conexao do PostgreSQL
      --target-rate <n>        Taxa alvo da rodada, para o registro
      --replication <n>        Numero da repeticao, para o registro
      --bootstrap <host>       Kafka (padrao: localhost:9092)
      --topic <nome>           Kafka (padrao: telemetry)
      --group <nome>           Kafka (padrao: pitwall)
      --rabbit-host <host>     RabbitMQ (padrao: localhost)
      --queue <prefixo>        RabbitMQ (padrao: telemetry)
      --prefetch <n>           RabbitMQ (padrao: 1000)
    """);
