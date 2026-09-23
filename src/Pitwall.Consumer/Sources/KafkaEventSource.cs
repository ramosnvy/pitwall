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

            // At-least-once no consumidor, no padrao documentado da
            // librdkafka: o offset so e armazenado explicitamente depois que o
            // evento foi entregue ao pipeline (StoreOffset abaixo), e o commit
            // dos offsets armazenados e periodico. Antes, o consumidor nunca
            // registrava progresso -- enquanto o RabbitMQ confirmava cada
            // mensagem, o Kafka nao pagava custo nenhum de confirmacao, e a
            // comparacao favorecia o Kafka por omissao.
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            AutoCommitIntervalMs = options.AutoCommitIntervalMs,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            FetchMinBytes = options.FetchMinBytes,
            FetchWaitMaxMs = options.FetchWaitMaxMs
        };

        using var consumer = new ConsumerBuilder<int, byte[]>(config).Build();
        consumer.Assign(new TopicPartitionOffset(options.Topic, partition, Offset.Beginning));

        long consumed = 0;
        var startupDeadline = DateTime.UtcNow + options.StartupTimeout;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = consumer.Consume(options.IdleTimeout);

                if (result?.Message is null)
                {
                    // Antes da primeira mensagem, o silencio significa que o
                    // produtor ainda nao comecou -- encerrar aqui faria a
                    // rodada terminar com zero evento. Depois da primeira,
                    // significa que a particao drenou.
                    if (consumed == 0 && DateTime.UtcNow < startupDeadline)
                    {
                        continue;
                    }

                    break;
                }

                pipeline.Submit(partition, result.Message.Value);
                consumer.StoreOffset(result);
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

    /// <summary>
    /// Intervalo do commit periodico dos offsets armazenados. O padrao da
    /// librdkafka e 5.000 ms; mantido, pela regra de usar a pratica
    /// documentada de cada fornecedor.
    /// </summary>
    public int AutoCommitIntervalMs { get; init; } = 5000;

    /// <summary>Tempo sem mensagem que encerra a rodada.</summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Espera pela primeira mensagem, antes de o tempo ocioso valer.</summary>
    public TimeSpan StartupTimeout { get; init; } = TimeSpan.FromMinutes(5);
}
