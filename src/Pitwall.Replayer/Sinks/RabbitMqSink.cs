using Pitwall.Contracts;
using RabbitMQ.Client;

namespace Pitwall.Replayer.Sinks;

using Pitwall.Workload;

/// <summary>
/// Publica os eventos numa fila do RabbitMQ.
///
/// O par do <see cref="KafkaSink"/>: mesma garantia de entrega
/// (at-least-once, via publisher confirms) para que a comparacao entre os
/// brokers meça arquitetura e nao diferenca de configuracao.
/// </summary>
public sealed class RabbitMqSink : IAsyncDisposable, IEventSink
{
    private readonly RabbitMqSinkOptions _options;
    private IConnection? _connection;
    private IChannel? _channel;
    private ValueTask[] _pending = [];
    private int _pendingCount;
    private long _published;

    private RabbitMqSink(RabbitMqSinkOptions options) => _options = options;

    public string Name => "rabbitmq";

    /// <summary>Nome da fila de uma faixa. Compartilhado com o consumidor.</summary>
    public static string QueueName(string prefix, int lane) => $"{prefix}.{lane}";

    /// <summary>Faixa de um carro. Mesma funcao no produtor e no consumidor.</summary>
    public static string Lane(int driverNumber, int partitions) =>
        (driverNumber % partitions).ToString();

    public long Published => Interlocked.Read(ref _published);

    public static async Task<RabbitMqSink> ConnectAsync(RabbitMqSinkOptions options, CancellationToken ct)
    {
        var sink = new RabbitMqSink(options);
        await sink.InitializeAsync(ct);
        return sink;
    }

    private async Task InitializeAsync(CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            HostName = _options.Host,
            Port = _options.Port,
            UserName = _options.User,
            Password = _options.Password
        };

        _connection = await factory.CreateConnectionAsync(ct);

        // Publisher confirms com rastreamento: e o equivalente do acks=all do
        // Kafka. As confirmacoes nao sao aguardadas uma a uma -- isso poria o
        // gerador em malha fechada, limitando a taxa ao tempo de ida e volta
        // do broker -- e sim em barreira a cada lote (ver PublishAsync).
        _channel = await _connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true),
            ct);

        // P filas ligadas a um exchange direct, uma por faixa de carros.
        //
        // O RabbitMQ nao tem particionamento por chave como o Kafka: numa
        // fila unica com varios consumidores, as amostras de um mesmo carro
        // caem em consumidores diferentes e a ordem por carro se perde --
        // o que quebraria a deteccao de frenagem e a verificacao cruzada.
        // Com uma fila por faixa e routing key derivada do numero do carro,
        // cada carro tem sempre o mesmo consumidor, que e a garantia que o
        // Kafka da por particao. E o que torna a comparacao justa.
        await _channel.ExchangeDeclareAsync(
            exchange: _options.Exchange,
            type: "direct",
            durable: true,
            autoDelete: false,
            cancellationToken: ct);

        for (var lane = 0; lane < _options.Partitions; lane++)
        {
            var queue = QueueName(_options.Queue, lane);

            await _channel.QueueDeclareAsync(
                queue: queue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                cancellationToken: ct);

            await _channel.QueueBindAsync(
                queue: queue,
                exchange: _options.Exchange,
                routingKey: lane.ToString(),
                cancellationToken: ct);
        }

        _pending = new ValueTask[_options.ConfirmBatchSize];
        _pendingCount = 0;
        _published = 0;
    }

    public async ValueTask PublishAsync(TelemetryEvent evt, CancellationToken ct)
    {
        var payload = new byte[TelemetryCodec.Size];
        TelemetryCodec.Write(payload, evt);

        // A ValueTask so completa quando o broker confirma. Ela e guardada
        // num array de structs (sem alocar Task por mensagem) e aguardada na
        // barreira, mantendo varias publicacoes em voo ao mesmo tempo.
        _pending[_pendingCount++] = _channel!.BasicPublishAsync(
            exchange: _options.Exchange,
            routingKey: Lane(evt.DriverNumber, _options.Partitions),
            mandatory: false,
            basicProperties: new BasicProperties { Persistent = _options.Persistent },
            body: payload,
            cancellationToken: ct);

        if (_pendingCount == _pending.Length)
        {
            await DrainAsync();
        }
    }

    ValueTask IEventSink.PublishAsync(in TelemetryEvent evt, CancellationToken ct) =>
        PublishAsync(evt, ct);

    /// <summary>
    /// Aguarda as confirmacoes do lote corrente. Uma publicacao nao
    /// confirmada e falha de rodada, nao evento perdido em silencio.
    /// </summary>
    private async ValueTask DrainAsync()
    {
        for (var i = 0; i < _pendingCount; i++)
        {
            await _pending[i];
            Interlocked.Increment(ref _published);
        }

        _pendingCount = 0;
    }

    public async ValueTask FlushAsync(CancellationToken ct)
    {
        // Sem isso, eventos ainda em voo ficariam de fora da contagem.
        await DrainAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
    }
}

public sealed record RabbitMqSinkOptions
{
    public string Host { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string User { get; init; } = "pitwall";
    public string Password { get; init; } = "pitwall";
    public string Queue { get; init; } = "telemetry";

    public string Exchange { get; init; } = "telemetry";

    /// <summary>
    /// Numero de filas, uma por faixa de carros. Equivale ao numero de
    /// particoes do Kafka e precisa ser igual a ele para que os dois brokers
    /// tenham o mesmo grau de paralelismo.
    /// </summary>
    public int Partitions { get; init; } = 4;

    /// <summary>
    /// Mensagem persistida em disco. Ligado por padrao para equivaler ao
    /// log do Kafka, que sempre grava.
    /// </summary>
    public bool Persistent { get; init; } = true;

    /// <summary>
    /// Quantas publicacoes ficam em voo antes de a barreira esperar as
    /// confirmacoes. E o analogo do batch.size do Kafka e precisa constar na
    /// tabela de configuracao do artigo.
    /// </summary>
    public int ConfirmBatchSize { get; init; } = 1000;
}
