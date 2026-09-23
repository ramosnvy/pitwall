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

        // Fila duravel e classica. Quorum seria mais resiliente, mas exige
        // replicacao e mudaria a garantia em relacao ao Kafka de no unico.
        await _channel.QueueDeclareAsync(
            queue: _options.Queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: ct);

        _pending = new ValueTask[_options.ConfirmBatchSize];
        _pendingCount = 0;
        _published = 0;
    }

    public async ValueTask PublishAsync(TelemetryEvent evt, CancellationToken ct)
    {
        var payload = new byte[TelemetryCodec.Size];
        TelemetryCodec.Write(payload, evt);

        // Exchange padrao com routing key igual ao nome da fila: o caminho
        // mais curto do RabbitMQ, sem custo de roteamento por topico. E o
        // equivalente mais justo a publicar direto num topico do Kafka.
        // A ValueTask so completa quando o broker confirma. Ela e guardada
        // num array de structs (sem alocar Task por mensagem) e aguardada na
        // barreira, mantendo varias publicacoes em voo ao mesmo tempo.
        _pending[_pendingCount++] = _channel!.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: _options.Queue,
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
