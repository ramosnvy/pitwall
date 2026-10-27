using System.Threading.Channels;
using Confluent.Kafka;
using Pitwall.Contracts;
using RabbitMQ.Client;

namespace Pitwall.Consumer.Republish;

/// <summary>
/// Publica a saida do segundo cenario no mesmo tipo de broker da entrada
/// (docs/APROFUNDAMENTO.md secao 8.6). Nao espera a confirmacao: quando ela
/// chega, avisa o <see cref="ReleaseTracker"/>. Quem decide quando esperar e
/// o modo de processamento.
///
/// A saida vai para a faixa de mesmo numero da entrada: os carros de uma faixa
/// de entrada continuam juntos na saida, como o co-particionamento pede.
/// </summary>
public interface IRepublisher : IAsyncDisposable
{
    /// <summary>
    /// Publica na faixa. Chamado sempre pela mesma etapa por faixa, nunca por
    /// duas threads ao mesmo tempo na mesma faixa.
    /// </summary>
    void Publish(int lane, byte[] payload, long id);

    long Published { get; }

    /// <summary>Saidas que o broker recusou. Qualquer valor acima de zero invalida a rodada.</summary>
    long Failed { get; }

    string EffectiveConfig { get; }
}

/// <summary>
/// Saida no Kafka: mesmo cliente e mesmos padroes do produtor de entrada
/// (acks=all, sem idempotencia, sem compressao, o resto de fabrica).
/// </summary>
public sealed class KafkaRepublisher : IRepublisher
{
    private readonly IProducer<int, byte[]> _producer;
    private readonly TopicPartition[] _partitions;
    private readonly ReleaseTracker _tracker;
    private readonly Action<DeliveryReport<int, byte[]>> _onDelivery;
    private long _published;
    private long _failed;

    public KafkaRepublisher(string bootstrapServers, string topic, int lanes, ReleaseTracker tracker)
    {
        _tracker = tracker;

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            CompressionType = CompressionType.None,
            EnableIdempotence = false
        };

        EffectiveConfig = RunSettings.Format(config
            .Where(kv => kv.Key != "bootstrap.servers")
            .Select(kv => new KeyValuePair<string, string?>("out." + kv.Key, kv.Value)));

        _producer = new ProducerBuilder<int, byte[]>(config)
            .SetErrorHandler((_, e) => Console.Error.WriteLine($"[kafka-saida] {e.Reason}"))
            .Build();

        _partitions = Enumerable.Range(0, lanes)
            .Select(lane => new TopicPartition(topic, new Partition(lane)))
            .ToArray();

        // A chave leva o identificador da pendencia: a particao e explicita, entao a
        // chave nao decide nada e evita uma closure por mensagem.
        _onDelivery = report =>
        {
            if (report.Error.IsError)
            {
                Interlocked.Increment(ref _failed);
            }

            _tracker.Confirm(report.Partition.Value, report.Message.Key);
        };
    }

    public string EffectiveConfig { get; }

    public long Published => Interlocked.Read(ref _published);

    public long Failed => Interlocked.Read(ref _failed);

    public void Publish(int lane, byte[] payload, long id)
    {
        var message = new Message<int, byte[]> { Key = (int)id, Value = payload };

        // Fila local cheia: espera servindo os relatorios de entrega, como o
        // produtor de entrada (KafkaSink).
        while (true)
        {
            try
            {
                _producer.Produce(_partitions[lane], message, _onDelivery);
                break;
            }
            catch (ProduceException<int, byte[]> e) when (e.Error.Code == ErrorCode.Local_QueueFull)
            {
                _producer.Poll(TimeSpan.FromMilliseconds(10));
            }
        }

        Interlocked.Increment(ref _published);
    }

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(30));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Saida no RabbitMQ: exchange direct e uma fila duravel por faixa, publisher
/// confirms com rastreamento e mensagem persistente, como a entrada no perfil
/// padrao. Uma conexao por faixa, como o produtor de entrada.
///
/// A ValueTask de cada publicacao so completa quando o broker confirma. Uma
/// tarefa por faixa as aguarda em ordem e avisa o rastreador; a etapa que
/// publica nunca espera aqui.
/// </summary>
public sealed class RabbitRepublisher : IRepublisher
{
    private readonly ReleaseTracker _tracker;
    private readonly string _exchange;
    private readonly BasicProperties _properties;
    private IConnection[] _connections = [];
    private IChannel[] _channels = [];
    private Channel<(ValueTask Confirm, long Id)>[] _confirms = [];
    private Task[] _drains = [];
    private long _published;
    private long _failed;

    private RabbitRepublisher(string exchange, bool persistent, ReleaseTracker tracker)
    {
        _exchange = exchange;
        _tracker = tracker;
        _properties = new BasicProperties { Persistent = persistent };
        EffectiveConfig = RunSettings.Format(new Dictionary<string, string?>
        {
            ["out.persistent"] = persistent ? "true" : "false",
            ["out.connection_per_lane"] = "true"
        });
    }

    public string EffectiveConfig { get; }

    public long Published => Interlocked.Read(ref _published);

    public long Failed => Interlocked.Read(ref _failed);

    public static async Task<RabbitRepublisher> ConnectAsync(
        string host, string exchange, int lanes, bool persistent, ReleaseTracker tracker, CancellationToken ct)
    {
        var republisher = new RabbitRepublisher(exchange, persistent, tracker);
        await republisher.InitializeAsync(host, lanes, ct);
        return republisher;
    }

    private async Task InitializeAsync(string host, int lanes, CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            HostName = host,
            UserName = "pitwall",
            Password = "pitwall"
        };

        _connections = new IConnection[lanes];
        _channels = new IChannel[lanes];
        _confirms = new Channel<(ValueTask, long)>[lanes];
        _drains = new Task[lanes];

        for (var lane = 0; lane < lanes; lane++)
        {
            _connections[lane] = await factory.CreateConnectionAsync(ct);
            _channels[lane] = await _connections[lane].CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true),
                ct);
        }

        await DeclareAsync(_channels[0], _exchange, lanes, ct);

        for (var lane = 0; lane < lanes; lane++)
        {
            _confirms[lane] = Channel.CreateUnbounded<(ValueTask, long)>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

            var index = lane;
            _drains[lane] = Task.Run(() => DrainAsync(index));
        }
    }

    /// <summary>Exchange e filas de saida. O painel declara do mesmo jeito, para poder subir antes.</summary>
    public static async Task DeclareAsync(IChannel channel, string exchange, int lanes, CancellationToken ct)
    {
        await channel.ExchangeDeclareAsync(exchange, "direct", durable: true, autoDelete: false, cancellationToken: ct);

        for (var lane = 0; lane < lanes; lane++)
        {
            var queue = exchange + "." + lane;
            await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false,
                arguments: null, cancellationToken: ct);
            await channel.QueueBindAsync(queue, exchange, lane.ToString(), cancellationToken: ct);
        }
    }

    public void Publish(int lane, byte[] payload, long id)
    {
        var confirm = _channels[lane].BasicPublishAsync(
            exchange: _exchange,
            routingKey: lane.ToString(),
            mandatory: false,
            basicProperties: _properties,
            body: payload);

        _confirms[lane].Writer.TryWrite((confirm, id));
        Interlocked.Increment(ref _published);
    }

    private async Task DrainAsync(int lane)
    {
        await foreach (var (confirm, id) in _confirms[lane].Reader.ReadAllAsync())
        {
            try
            {
                await confirm;
            }
            catch (Exception e)
            {
                Interlocked.Increment(ref _failed);
                Console.Error.WriteLine($"[rabbit-saida] faixa {lane}: {e.Message}");
            }

            _tracker.Confirm(lane, id);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var confirms in _confirms)
        {
            confirms.Writer.TryComplete();
        }

        await Task.WhenAll(_drains);

        foreach (var channel in _channels)
        {
            await channel.DisposeAsync();
        }

        foreach (var connection in _connections)
        {
            await connection.DisposeAsync();
        }
    }
}
