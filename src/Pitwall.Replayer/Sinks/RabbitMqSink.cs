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
    private IConnection[] _connections = [];
    private IChannel[] _channels = [];

    // Buffer duplo por faixa. Um lote enche enquanto o anterior aguarda
    // confirmacao em segundo plano. A versao anterior esperava as confirmacoes
    // DENTRO do laco de ritmo a cada lote cheio: como o RabbitMQ so confirma
    // mensagem persistente depois do fsync, o gerador parava ali dezenas de
    // milissegundos -- um trecho de malha fechada num gerador que precisa ser
    // de malha aberta, e que penalizava so o RabbitMQ (o produtor Kafka
    // confirma por callback e nunca bloqueia). Resultado: jitter de 30 a 40 ms
    // ja a 10 mil ev/s.
    private ValueTask[][] _pending = [];
    private ValueTask[][] _spare = [];
    private Task?[] _draining = [];
    private int[] _pendingCount = [];
    private long _published;

    private RabbitMqSink(RabbitMqSinkOptions options) => _options = options;

    public string Name => "rabbitmq";

    public long Published => Interlocked.Read(ref _published);

    /// <summary>Nome da fila de uma faixa. Compartilhado com o consumidor.</summary>
    public static string QueueName(string prefix, int lane) => $"{prefix}.{lane}";

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

        var connectionCount = _options.ConnectionPerLane ? _options.Partitions : 1;

        _connections = new IConnection[connectionCount];

        for (var i = 0; i < connectionCount; i++)
        {
            _connections[i] = await factory.CreateConnectionAsync(ct);
        }

        _channels = new IChannel[_options.Partitions];
        _pending = new ValueTask[_options.Partitions][];
        _spare = new ValueTask[_options.Partitions][];
        _draining = new Task?[_options.Partitions];
        _pendingCount = new int[_options.Partitions];

        for (var lane = 0; lane < _options.Partitions; lane++)
        {
            // Uma conexao TCP por faixa, conforme a recomendacao do proprio
            // RabbitMQ para publicacao em alta vazao: canais compartilham o
            // socket da conexao, entao varios canais numa conexao unica ainda
            // disputam a mesma saida de rede.
            var connection = _connections[_options.ConnectionPerLane ? lane : 0];

            // Publisher confirms com rastreamento: o equivalente do acks=all do
            // Kafka. As confirmacoes nao sao aguardadas uma a uma -- isso poria
            // o gerador em malha fechada, limitando a taxa ao tempo de ida e
            // volta do broker -- e sim em barreira a cada lote por faixa.
            _channels[lane] = await connection.CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true),
                ct);

            _pending[lane] = new ValueTask[_options.ConfirmBatchSize];
            _spare[lane] = new ValueTask[_options.ConfirmBatchSize];
            _draining[lane] = null;
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

        var lane = LanePartitioner.Lane(evt.DriverNumber, _options.Partitions, _options.LaneHash);

        // A ValueTask so completa quando o broker confirma. Fica guardada num
        // array de structs (sem alocar Task por mensagem), e o lote e aguardado
        // em segundo plano quando enche.
        _pending[lane][_pendingCount[lane]++] = _channels[lane].BasicPublishAsync(
            exchange: _options.Exchange,
            routingKey: lane.ToString(),
            mandatory: false,
            basicProperties: new BasicProperties { Persistent = _options.Persistent },
            body: payload,
            cancellationToken: ct);

        if (_pendingCount[lane] == _pending[lane].Length)
        {
            // So bloqueia se o lote ANTERIOR ainda nao foi confirmado -- ou
            // seja, com dois lotes inteiros em voo. Isso e contrapressao real
            // do broker, nao uma barreira fixa a cada lote.
            if (_draining[lane] is { } previous)
            {
                await previous;
            }

            var full = _pending[lane];
            var count = _pendingCount[lane];

            // O buffer reserva ja teve sua drenagem concluida (aguardada
            // acima), entao pode ser reaproveitado.
            _pending[lane] = _spare[lane];
            _spare[lane] = full;
            _pendingCount[lane] = 0;

            _draining[lane] = DrainBatchAsync(full, count);
        }
    }

    ValueTask IEventSink.PublishAsync(in TelemetryEvent evt, CancellationToken ct) =>
        PublishAsync(evt, ct);

    /// <summary>
    /// Aguarda as confirmacoes de um lote. Publicacao nao confirmada e falha de
    /// rodada, nao evento perdido em silencio: a excecao propaga no proximo
    /// await deste Task.
    /// </summary>
    private async Task DrainBatchAsync(ValueTask[] batch, int count)
    {
        for (var i = 0; i < count; i++)
        {
            await batch[i];
            Interlocked.Increment(ref _published);
        }
    }

    public async ValueTask FlushAsync(CancellationToken ct)
    {
        // Sem isso, eventos ainda em voo ficariam de fora da contagem.
        for (var lane = 0; lane < _channels.Length; lane++)
        {
            if (_draining[lane] is { } draining)
            {
                await draining;
                _draining[lane] = null;
            }

            await DrainBatchAsync(_pending[lane], _pendingCount[lane]);
            _pendingCount[lane] = 0;
        }
    }

    public async ValueTask DisposeAsync()
    {
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
    /// Funcao que escolhe a fila de cada carro. CRC32, a mesma do Kafka, para
    /// que os dois brokers recebam a mesma divisao; Modulo reproduz a matriz
    /// 7e283c2, com filas desbalanceadas (docs/IMPLEMENTACAO.md).
    /// </summary>
    public LaneHash LaneHash { get; init; } = LaneHash.Crc32;

    /// <summary>
    /// Mensagem persistida em disco. Ligado por padrao para equivaler ao log do
    /// Kafka, que sempre grava. O piloto mediu a diferenca entre persistente e
    /// transiente em 2,8%, dentro do ruido.
    /// </summary>
    public bool Persistent { get; init; } = true;

    /// <summary>
    /// Publicacoes em voo POR FAIXA antes da barreira de confirmacao. Com P
    /// canais o total em voo e P vezes este valor. O piloto mediu: com 250 o
    /// produtor travava em 42,6 mil ev/s; com 1.000 subiu para 59,2 mil, sem
    /// custo de latencia em carga normal (ver docs/PILOTO.md).
    /// </summary>
    public int ConfirmBatchSize { get; init; } = 1000;

    /// <summary>
    /// Uma conexao TCP por faixa, em vez de uma conexao compartilhada por todos
    /// os canais. E a recomendacao do RabbitMQ para publicacao em alta vazao.
    /// </summary>
    public bool ConnectionPerLane { get; init; } = true;
}
