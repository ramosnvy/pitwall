using Pitwall.Contracts;
using Pitwall.Workload;
using RabbitMQ.Client;

namespace Pitwall.Replayer.Sinks;

/// <summary>
/// Publica os eventos em P filas do RabbitMQ, uma por faixa de carros.
///
/// **Um canal por faixa.** Canais AMQP serializam as operacoes: publicar nas P
/// filas por um unico canal daria ao produtor um P-esimo do paralelismo do
/// consumidor, e o teto medido seria do instrumento, nao do broker. O piloto
/// mostrou exatamente isso -- a 50 mil ev/s o produtor de canal unico entregou
/// 27,7 mil com 8 s de atraso acumulado (ver docs/PILOTO.md).
///
/// O RabbitMQ nao tem particionamento por chave como o Kafka. As P filas com
/// routing key derivada do numero do carro reproduzem essa garantia: um carro
/// cai sempre na mesma fila, e portanto sempre no mesmo consumidor.
/// </summary>
public sealed class RabbitMqSink : IEventSink
{
    private readonly RabbitMqSinkOptions _options;
    private IConnection? _connection;
    private IChannel[] _channels = [];
    private ValueTask[][] _pending = [];
    private int[] _pendingCount = [];
    private long _published;

    private RabbitMqSink(RabbitMqSinkOptions options) => _options = options;

    public string Name => "rabbitmq";

    public long Published => Interlocked.Read(ref _published);

    /// <summary>Nome da fila de uma faixa. Compartilhado com o consumidor.</summary>
    public static string QueueName(string prefix, int lane) => $"{prefix}.{lane}";

    /// <summary>Faixa de um carro. Mesma funcao no produtor e no consumidor.</summary>
    public static int Lane(int driverNumber, int partitions) =>
        (driverNumber & int.MaxValue) % partitions;

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

        _channels = new IChannel[_options.Partitions];
        _pending = new ValueTask[_options.Partitions][];
        _pendingCount = new int[_options.Partitions];

        for (var lane = 0; lane < _options.Partitions; lane++)
        {
            // Publisher confirms com rastreamento: o equivalente do acks=all do
            // Kafka. As confirmacoes nao sao aguardadas uma a uma -- isso poria
            // o gerador em malha fechada, limitando a taxa ao tempo de ida e
            // volta do broker -- e sim em barreira a cada lote por faixa.
            _channels[lane] = await _connection.CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true),
                ct);

            _pending[lane] = new ValueTask[_options.ConfirmBatchSize];
            _pendingCount[lane] = 0;
        }

        // Exchange e filas declarados uma vez, pelo primeiro canal. O consumidor
        // tambem declara, de forma idempotente, para poder subir antes.
        var setup = _channels[0];

        await setup.ExchangeDeclareAsync(
            exchange: _options.Exchange,
            type: "direct",
            durable: true,
            autoDelete: false,
            cancellationToken: ct);

        for (var lane = 0; lane < _options.Partitions; lane++)
        {
            var queue = QueueName(_options.Queue, lane);

            await setup.QueueDeclareAsync(
                queue: queue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                cancellationToken: ct);

            await setup.QueueBindAsync(
                queue: queue,
                exchange: _options.Exchange,
                routingKey: lane.ToString(),
                cancellationToken: ct);
        }

        _published = 0;
    }

    public async ValueTask PublishAsync(TelemetryEvent evt, CancellationToken ct)
    {
        var payload = new byte[TelemetryCodec.Size];
        TelemetryCodec.Write(payload, evt);

        var lane = Lane(evt.DriverNumber, _options.Partitions);

        // A ValueTask so completa quando o broker confirma. Fica guardada num
        // array de structs (sem alocar Task por mensagem) e e aguardada na
        // barreira, mantendo varias publicacoes em voo por faixa.
        _pending[lane][_pendingCount[lane]++] = _channels[lane].BasicPublishAsync(
            exchange: _options.Exchange,
            routingKey: lane.ToString(),
            mandatory: false,
            basicProperties: new BasicProperties { Persistent = _options.Persistent },
            body: payload,
            cancellationToken: ct);

        if (_pendingCount[lane] == _pending[lane].Length)
        {
            await DrainAsync(lane);
        }
    }

    ValueTask IEventSink.PublishAsync(in TelemetryEvent evt, CancellationToken ct) =>
        PublishAsync(evt, ct);

    /// <summary>
    /// Aguarda as confirmacoes pendentes de uma faixa. Publicacao nao
    /// confirmada e falha de rodada, nao evento perdido em silencio.
    /// </summary>
    private async ValueTask DrainAsync(int lane)
    {
        for (var i = 0; i < _pendingCount[lane]; i++)
        {
            await _pending[lane][i];
            Interlocked.Increment(ref _published);
        }

        _pendingCount[lane] = 0;
    }

    public async ValueTask FlushAsync(CancellationToken ct)
    {
        // Sem isso, eventos ainda em voo ficariam de fora da contagem.
        for (var lane = 0; lane < _channels.Length; lane++)
        {
            await DrainAsync(lane);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var channel in _channels)
        {
            await channel.DisposeAsync();
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
    /// Numero de filas e de canais, uma por faixa de carros. Equivale ao numero
    /// de particoes do Kafka e precisa ser igual a ele para que os dois brokers
    /// tenham o mesmo grau de paralelismo.
    /// </summary>
    public int Partitions { get; init; } = 4;

    /// <summary>
    /// Mensagem persistida em disco. Ligado por padrao para equivaler ao log do
    /// Kafka, que sempre grava. O piloto mediu a diferenca entre persistente e
    /// transiente em 2,8%, dentro do ruido.
    /// </summary>
    public bool Persistent { get; init; } = true;

    /// <summary>
    /// Publicacoes em voo POR FAIXA antes da barreira de confirmacao. Com P
    /// canais o total em voo e P vezes este valor, por isso o padrao caiu de
    /// 1.000 para 250 quando o produtor passou a usar um canal por faixa.
    /// </summary>
    public int ConfirmBatchSize { get; init; } = 250;
}
