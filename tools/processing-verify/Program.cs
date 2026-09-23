using System.Diagnostics;
using Pitwall.Contracts;
using Pitwall.Processing.Core;
using Pitwall.Workload;

// Verificador de corretude do processamento.
//
// Roda a mesma entrada com numeros diferentes de workers e compara os
// digests. Se o particionamento por carro estiver correto, o resultado e
// identico em todos os casos -- e e essa propriedade que permite usar o
// digest para validar as seis variantes da matriz mais adiante.
//
// Tambem mede o custo do processamento isolado, sem broker: e o piso contra
// o qual comparar os resultados com transporte.

var datasetPath = GetArg("--dataset") ?? "data/raw/9472/car_data.jsonl";
var maxEvents = GetArg("--max-events") is { } m ? int.Parse(m) : (int?)null;
var shardCounts = (GetArg("--shards") ?? "1,2,4,8")
    .Split(',')
    .Select(int.Parse)
    .ToArray();

Console.WriteLine($"Carregando {datasetPath}...");
var dataset = TelemetryDataset.Load(datasetPath, maxEvents);
Console.WriteLine($"{dataset.Events.Length:N0} eventos da sessao {dataset.SessionKey}");
Console.WriteLine();

var options = new ProcessingOptions();
ResultDigest? reference = null;
var allMatch = true;

Console.WriteLine($"{"workers",-9} {"tempo",-10} {"ev/s",-14} resultado");
Console.WriteLine(new string('-', 78));

foreach (var shards in shardCounts)
{
    var digest = new ResultDigest();
    var elapsed = RunSharded(dataset.Events, shards, options, digest);
    var rate = dataset.Events.Length / elapsed.TotalSeconds;

    string verdict;

    if (reference is null)
    {
        reference = digest;
        verdict = "referencia";
    }
    else if (reference.Matches(digest))
    {
        verdict = "identico a referencia";
    }
    else
    {
        verdict = "DIVERGENTE";
        allMatch = false;
    }

    Console.WriteLine(
        $"{shards,-9} {elapsed.TotalSeconds,-10:0.00}s {rate,-14:N0} {verdict}");
}

Console.WriteLine();
Console.WriteLine($"Digest: {reference}");
Console.WriteLine();

if (!allMatch)
{
    Console.WriteLine("FALHA: o resultado mudou com o numero de workers.");
    Console.WriteLine("O particionamento por carro nao esta preservando ordem.");
    return 1;
}

Console.WriteLine("OK: o resultado independe do numero de workers.");
Console.WriteLine("O digest pode ser usado para verificar as seis variantes da matriz.");
return 0;

/// <summary>
/// Distribui os carros entre workers e processa em paralelo. Cada carro cai
/// sempre no mesmo worker, o que preserva a ordem por carro.
/// </summary>
static TimeSpan RunSharded(
    TelemetryEvent[] events,
    int shards,
    ProcessingOptions options,
    ResultDigest digest)
{
    var processors = new TelemetryProcessor[shards];
    var queues = new List<TelemetryEvent>[shards];

    for (var i = 0; i < shards; i++)
    {
        processors[i] = new TelemetryProcessor(options, stats => digest.Add(stats));
        queues[i] = new List<TelemetryEvent>(events.Length / shards + 16);
    }

    // O particionamento em si fica fora da medicao: no sistema real quem
    // distribui e o broker (particao do Kafka) ou o consumer.
    foreach (var evt in events)
    {
        queues[evt.DriverNumber % shards].Add(evt);
    }

    var watch = Stopwatch.StartNew();

    Parallel.For(0, shards, i =>
    {
        var processor = processors[i];
        var queue = queues[i];

        for (var j = 0; j < queue.Count; j++)
        {
            processor.Process(queue[j]);
        }

        processor.Complete();
    });

    watch.Stop();
    return watch.Elapsed;
}

string? GetArg(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}
