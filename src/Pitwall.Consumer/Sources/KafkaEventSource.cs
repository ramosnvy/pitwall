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

    // Progresso global, compartilhado pelas threads de particao. O
    // encerramento e decidido por ele, nao pelo silencio de uma particao
    // isolada: na matriz oficial, um travamento de 5,6 s levou uma particao a
    // encerrar sozinha pelo tempo ocioso de 6 s, e 2,1 milhoes de eventos
    // daquela particao nunca foram consumidos.
    private long _received;
    private long _lastProgressTicks;

    public async Task<long> ConsumeAsync(IProcessingPipeline pipeline, CancellationToken ct)
    {
        _received = 0;
        _lastProgressTicks = DateTime.UtcNow.Ticks;

        // Uma thread DEDICADA por particao, nao uma do pool. O Consume() do
        // Confluent.Kafka e sincrono e bloqueia a thread enquanto espera. Com
        // Task.Run, as quatro threads presas saiam do pool de threads do .NET,
        // que num container limitado a 3 CPUs tem minimo de 3 threads: o pool
        // esgotava, e as continuacoes dos workers de Channels e Pipelines
        // esperavam a injecao de threads novas -- que ocorre a 1 ou 2 por
        // segundo (docs da Microsoft sobre ThreadPool starvation). O sintoma
        // foi latencia maxima de ~1 s em todas as rodadas dessas variantes. No
        // host Windows, com 12 nucleos, o pool tinha folga e o defeito ficava
        // escondido.
        var tasks = Enumerable.Range(0, options.Partitions)
            .Select(partition => Task.Factory.StartNew(
                () => ConsumePartition(partition, pipeline, ct),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default))
            .ToArray();

        var counts = await Task.WhenAll(tasks);
        return counts.Sum();
    }

    /// <summary>
    /// Decide se a rodada acabou. Com o total esperado conhecido, acaba quando
    /// todos chegaram -- ou, como rede de seguranca, quando o sistema INTEIRO
    /// fica sem progresso pelo tempo ocioso. Sem o total esperado, mantem o
    /// comportamento antigo de silencio por particao.
    /// </summary>
    private bool ShouldStop(long consumedHere, DateTime startupDeadline)
    {
        if (options.ExpectedEvents > 0)
        {
            if (Interlocked.Read(ref _received) >= options.ExpectedEvents)
            {
                return true;
            }

            var idleFor = DateTime.UtcNow - new DateTime(Interlocked.Read(ref _lastProgressTicks), DateTimeKind.Utc);
            var limit = Interlocked.Read(ref _received) == 0 ? options.StartupTimeout : options.IdleTimeout;
            return idleFor > limit;
        }

        return !(consumedHere == 0 && DateTime.UtcNow < startupDeadline);
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
            FetchWaitMaxMs = options.FetchWaitMaxMs,

            // Nagle desligado (TCP_NODELAY). Ver KafkaSourceOptions.SocketNagleDisable.
            SocketNagleDisable = options.SocketNagleDisable
        };

        using var consumer = new ConsumerBuilder<int, byte[]>(config).Build();
        consumer.Assign(new TopicPartitionOffset(options.Topic, partition, Offset.Beginning));

        long consumed = 0;
        var startupDeadline = DateTime.UtcNow + options.StartupTimeout;

        try
        {
            // Com total esperado, a espera por mensagem e curta para reavaliar o
            // criterio global com frequencia; sem ele, e o proprio tempo ocioso.
            var pollTimeout = options.ExpectedEvents > 0 ? TimeSpan.FromMilliseconds(200) : options.IdleTimeout;

            while (!ct.IsCancellationRequested)
            {
                var result = consumer.Consume(pollTimeout);

                if (result?.Message is null)
                {
                    if (ShouldStop(consumed, startupDeadline))
                    {
                        break;
                    }

                    continue;
                }

                pipeline.Submit(partition, result.Message.Value);
                consumer.StoreOffset(result);
                consumed++;

                Interlocked.Increment(ref _received);
                Interlocked.Exchange(ref _lastProgressTicks, DateTime.UtcNow.Ticks);

                if (options.ExpectedEvents > 0 && Interlocked.Read(ref _received) >= options.ExpectedEvents)
                {
                    break;
                }
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

    /// <summary>Desliga o algoritmo de Nagle no socket (ver KafkaSinkOptions).</summary>
    public bool SocketNagleDisable { get; init; }

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

    /// <summary>
    /// Total de eventos que o produtor vai publicar. Quando informado, a rodada
    /// termina ao recebe-los todos, e o tempo ocioso passa a valer para o
    /// sistema inteiro, nao para cada particao.
    /// </summary>
    public long ExpectedEvents { get; init; }
}
