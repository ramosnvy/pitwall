using Confluent.Kafka;
using Pitwall.Consumer.Pipelines;

namespace Pitwall.Consumer.Sources;

/// <summary>
/// Consome de um topico do Kafka com uma thread por particao.
///
/// Cada thread assina uma particao especifica em vez de entrar num grupo de
/// consumidores: o rebalanceamento automatico moveria particoes entre threads
/// no meio da rodada, o que quebraria a afinidade carro-faixa de que a
/// deteccao de padrao depende. Atribuicao manual e o que torna a rodada
/// deterministica e comparavel com o RabbitMQ.
/// </summary>
public sealed class KafkaEventSource(KafkaSourceOptions options) : IEventSource
{
    public string Broker => "kafka";

    public int Lanes => options.Partitions;

    public async Task<long> ConsumeAsync(IProcessingPipeline pipeline, CancellationToken ct)
    {
        var tasks = Enumerable.Range(0, options.Partitions)
            .Select(partition => Task.Run(() => ConsumePartition(partition, pipeline, ct), ct))
            .ToArray();

        var counts = await Task.WhenAll(tasks);
        return counts.Sum();
    }

    private long ConsumePartition(int partition, IProcessingPipeline pipeline, CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId + "-" + partition,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            FetchMinBytes = options.FetchMinBytes,
            FetchWaitMaxMs = options.FetchWaitMaxMs
        };

        using var consumer = new ConsumerBuilder<int, byte[]>(config).Build();
        consumer.Assign(new TopicPartitionOffset(options.Topic, partition, Offset.Beginning));

        long consumed = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = consumer.Consume(options.IdleTimeout);

                if (result?.Message is null)
                {
                    // Nenhuma mensagem dentro do tempo limite: o produtor
                    // terminou e a particao esta drenada.
                    break;
                }

                pipeline.Submit(partition, result.Message.Value);
                consumed++;
            }
        }
        catch (OperationCanceledException)
        {
            // Encerramento normal.
        }
        finally
        {
            consumer.Close();
        }

        return consumed;
    }
}

public sealed record KafkaSourceOptions
{
    public string BootstrapServers { get; init; } = "localhost:9092";
    public string Topic { get; init; } = "telemetry";
    public string GroupId { get; init; } = "pitwall";
    public int Partitions { get; init; } = 4;
    public int FetchMinBytes { get; init; } = 1;
    public int FetchWaitMaxMs { get; init; } = 10;

    /// <summary>Tempo sem mensagem que encerra a rodada.</summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(10);
}
