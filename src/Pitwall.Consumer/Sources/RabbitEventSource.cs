using Pitwall.Consumer.Pipelines;
using Pitwall.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Pitwall.Consumer.Sources;

/// <summary>
/// Consome do RabbitMQ com um canal e um consumidor por fila.
///
/// Sao P filas, uma por faixa de carros, alimentadas pelo produtor com
/// routing key derivada do numero do carro. Uma fila unica com P consumidores
/// concorrentes entregaria as amostras de um mesmo carro a consumidores
/// diferentes e destruiria a ordem por carro -- garantia que o Kafka oferece
/// nativamente por particao e que o RabbitMQ so alcanca com esse arranjo.
/// </summary>
public sealed class RabbitEventSource(RabbitSourceOptions options) : IEventSource
{
    public string Broker => "rabbitmq";

    public int Lanes => options.Partitions;

    public string EffectiveConfig => RunSettings.Format(new Dictionary<string, string?>
    {
        ["prefetch"] = options.Prefetch == 0 ? "sem_limite" : options.Prefetch.ToString(),
        ["ack_batch"] = options.AckBatch.ToString(),
        ["dispatch_concurrency"] = "1"
    });

    public async Task<long> ConsumeAsync(IProcessingPipeline pipeline, CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            HostName = options.Host,
            Port = options.Port,
            UserName = options.User,
            Password = options.Password,

            // Um despachante por canal: o paralelismo vem das P filas, nao de
            // varias threads na mesma fila, que reordenariam as amostras.
            ConsumerDispatchConcurrency = 1
        };

        await using var connection = await factory.CreateConnectionAsync(ct);

        var idle = new IdleWatchdog(options.IdleTimeout, options.StartupTimeout);
        var counts = new long[options.Partitions];
        var channels = new IChannel[options.Partitions];
        var lastTags = new ulong[options.Partitions];
        var unacked = new int[options.Partitions];

        // O lote de confirmacao precisa caber folgado no prefetch: com lote
        // maior ou igual ao prefetch, o broker para de entregar esperando uma
        // confirmacao que so viria com a proxima entrega -- impasse. Com
        // prefetch 0 (sem limite, o padrao do RabbitMQ) nao ha impasse.
        if (options.Prefetch > 0 && options.AckBatch * 2 > options.Prefetch)
        {
            throw new ArgumentException(
                $"AckBatch ({options.AckBatch}) precisa ser no maximo metade do Prefetch ({options.Prefetch}).");
        }

        for (var lane = 0; lane < options.Partitions; lane++)
        {
            var index = lane;
            var channel = await connection.CreateChannelAsync(cancellationToken: ct);
            channels[lane] = channel;

            // Declaracao idempotente, tambem feita pelo produtor: o consumidor
            // pode subir primeiro, e sem isso falharia com NOT_FOUND. Os
            // parametros precisam ser identicos aos do produtor, senao o
            // RabbitMQ recusa a redeclaracao.
            await channel.ExchangeDeclareAsync(
                exchange: options.Exchange,
                type: "direct",
                durable: true,
                autoDelete: false,
                cancellationToken: ct);

            await channel.QueueDeclareAsync(
                queue: options.Queue + "." + index,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                cancellationToken: ct);

            await channel.QueueBindAsync(
                queue: options.Queue + "." + index,
                exchange: options.Exchange,
                routingKey: index.ToString(),
                cancellationToken: ct);

            // prefetch limita quantas mensagens nao confirmadas o broker
            // entrega adiante. E o analogo do limite de capacidade do Channels
            // e precisa constar na tabela de configuracao do artigo. Com 0, o
            // padrao do RabbitMQ, nao ha limite e a chamada e dispensada.
            if (options.Prefetch > 0)
            {
                await channel.BasicQosAsync(0, options.Prefetch, global: false, cancellationToken: ct);
            }

            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (_, ea) =>
            {
                // Os bytes sao lidos antes de SubmitAsync retornar, enquanto a
                // entrega ainda e valida. Com a fila interna cheia, aguarda aqui
                // sem bloquear a thread: o despachante do canal so passa a
                // proxima entrega quando esta termina, e isso e a contrapressao
                // (REVISAO-TECNICA §2.3).
                var submitted = pipeline.SubmitAsync(index, ea.Body.Span);

                if (!submitted.IsCompletedSuccessfully)
                {
                    await submitted;
                }

                Interlocked.Increment(ref counts[index]);
                idle.Touch();

                lastTags[index] = ea.DeliveryTag;

                // Confirmacao em lote com multiple=true, como recomenda a
                // documentacao do RabbitMQ para reduzir trafego: uma
                // confirmacao cobre todas as entregas ate a etiqueta indicada.
                // Confirmar mensagem a mensagem daria ao RabbitMQ um custo por
                // evento que o Kafka nao paga -- o Kafka registra o progresso
                // por offset, periodicamente.
                if (++unacked[index] >= options.AckBatch)
                {
                    unacked[index] = 0;
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: true, ct);
                }
            };

            await channel.BasicConsumeAsync(
                queue: options.Queue + "." + index,
                autoAck: false,
                consumer: consumer,
                cancellationToken: ct);
        }

        await idle.WaitForSilenceAsync(() => counts.Sum(), options.ExpectedEvents, ct);

        for (var lane = 0; lane < channels.Length; lane++)
        {
            // Confirma o lote parcial que sobrou. Sem isso, as ultimas entregas
            // voltariam para a fila ao fechar o canal.
            if (unacked[lane] > 0)
            {
                await channels[lane].BasicAckAsync(lastTags[lane], multiple: true, ct);
            }

            await channels[lane].CloseAsync(ct);
            await channels[lane].DisposeAsync();
        }

        return counts.Sum();
    }

    /// <summary>
    /// Encerra a rodada quando para de chegar mensagem. O RabbitMQ entrega por
    /// callback e nao avisa que a fila secou, entao o silencio e o sinal.
    /// </summary>
    private sealed class IdleWatchdog(TimeSpan timeout, TimeSpan startupTimeout)
    {
        private long _lastTicks = DateTime.UtcNow.Ticks;
        private int _sawMessage;

        public void Touch()
        {
            Interlocked.Exchange(ref _lastTicks, DateTime.UtcNow.Ticks);
            Interlocked.Exchange(ref _sawMessage, 1);
        }

        public async Task WaitForSilenceAsync(Func<long> received, long expected, CancellationToken ct)
        {
            var startupDeadline = DateTime.UtcNow + startupTimeout;

            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250), ct);

                // Com o total esperado conhecido, a rodada acaba quando todos
                // chegaram; o silencio vira apenas rede de seguranca.
                if (expected > 0 && received() >= expected)
                {
                    return;
                }

                // Antes da primeira mensagem o silencio so significa que o
                // produtor ainda nao comecou.
                if (Interlocked.CompareExchange(ref _sawMessage, 0, 0) == 0)
                {
                    if (DateTime.UtcNow < startupDeadline)
                    {
                        continue;
                    }

                    return;
                }

                var last = new DateTime(Interlocked.Read(ref _lastTicks), DateTimeKind.Utc);

                if (DateTime.UtcNow - last > timeout)
                {
                    return;
                }
            }
        }
    }
}

public sealed record RabbitSourceOptions
{
    public string Host { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string User { get; init; } = "pitwall";
    public string Password { get; init; } = "pitwall";
    public string Queue { get; init; } = "telemetry";
    public string Exchange { get; init; } = "telemetry";
    public int Partitions { get; init; } = 4;
    /// <summary>
    /// Mensagens nao confirmadas que o broker entrega adiante por canal. 0 e o
    /// padrao do RabbitMQ: sem limite, o mais proximo do padrao do consumidor
    /// Kafka, que busca ate 100 mil mensagens por particao
    /// (docs/AUDITORIA-CONFIG.md, assimetria C). A documentacao do RabbitMQ
    /// recomenda 100 a 300; as rodadas anteriores a 24/09 usavam 300.
    /// </summary>
    public ushort Prefetch { get; init; }

    /// <summary>Entregas cobertas por cada confirmacao com multiple=true.</summary>
    public int AckBatch { get; init; } = 100;
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Espera pela primeira mensagem, antes de o tempo ocioso valer.</summary>
    public TimeSpan StartupTimeout { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Total de eventos esperados; ver KafkaSourceOptions.ExpectedEvents.</summary>
    public long ExpectedEvents { get; init; }
}
