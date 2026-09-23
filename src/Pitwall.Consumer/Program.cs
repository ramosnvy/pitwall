using System.Diagnostics;
using HdrHistogram;
using Pitwall.Consumer;
using Pitwall.Consumer.Pipelines;
using Pitwall.Consumer.Sources;
using Pitwall.Contracts;
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

// A capacidade e declarada em eventos e convertida para bytes na variante
// Pipelines, para que as duas tenham a mesma contrapressao em numero de
// eventos e nao em unidades diferentes.
await using IProcessingPipeline pipeline = mode switch
{
    "direct" => new DirectPipeline(partitions, processingOptions, digest, latency),
    "channels" => new ChannelsPipeline(partitions, capacity, processingOptions, digest, latency),
    "pipelines" => new PipelinesPipeline(
        partitions, capacity * TelemetryCodec.Size, processingOptions, digest, latency),
    _ => throw new ArgumentException($"Modo desconhecido: {mode}. Use direct, channels ou pipelines.")
};

var architecture = source.Broker + "-" + pipeline.Mode;

Console.WriteLine($"Arquitetura: {architecture} | faixas: {partitions} | " +
                  $"capacidade: {capacity:N0} eventos | custo sintetico: {syntheticCost} us");
Console.WriteLine($"Aguardando eventos (encerra apos {idleSeconds}s sem mensagem)...");
Console.WriteLine();

var watch = Stopwatch.StartNew();
var received = await source.ConsumeAsync(pipeline, cts.Token);
await pipeline.CompleteAsync();
watch.Stop();

var seconds = watch.Elapsed.TotalSeconds;

Console.WriteLine($"Eventos recebidos : {received:N0}");
Console.WriteLine($"Tempo             : {seconds:0.00}s");
Console.WriteLine($"Throughput        : {(seconds > 0 ? received / seconds : 0):N0} ev/s");
Console.WriteLine($"Latencia          : {latency.Summary()}");
Console.WriteLine($"Digest            : {digest}");

if (reportPath is not null)
{
    WriteReport(reportPath, architecture, partitions, received, seconds, latency, digest, syntheticCost);
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
    string architecture,
    int partitions,
    long received,
    double seconds,
    LatencyRecorder latency,
    ResultDigest digest,
    double syntheticCost)
{
    var histogram = latency.Merged();
    var exists = File.Exists(path);

    using var writer = new StreamWriter(path, append: true);

    if (!exists)
    {
        writer.WriteLine(
            "timestamp,architecture,partitions,synthetic_cost_us,events,seconds,throughput," +
            "mean_us,p50_us,p95_us,p99_us,max_us,windows,digest_hash");
    }

    writer.WriteLine(string.Join(',',
        DateTimeOffset.UtcNow.ToString("o"),
        architecture,
        partitions,
        syntheticCost,
        received,
        seconds.ToString("0.000"),
        (seconds > 0 ? received / seconds : 0).ToString("0.0"),
        histogram.GetMean().ToString("0.0"),
        histogram.GetValueAtPercentile(50),
        histogram.GetValueAtPercentile(95),
        histogram.GetValueAtPercentile(99),
        histogram.GetMaxValue(),
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
      --bootstrap <host>       Kafka (padrao: localhost:9092)
      --topic <nome>           Kafka (padrao: telemetry)
      --group <nome>           Kafka (padrao: pitwall)
      --rabbit-host <host>     RabbitMQ (padrao: localhost)
      --queue <prefixo>        RabbitMQ (padrao: telemetry)
      --prefetch <n>           RabbitMQ (padrao: 1000)
    """);
