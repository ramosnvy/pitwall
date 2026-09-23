using Pitwall.Consumer.Pipelines;
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
            // e precisa constar na tabela de configuracao do artigo.
            await channel.BasicQosAsync(0, options.Prefetch, global: false, cancellationToken: ct);

            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (_, ea) =>
            {
                pipeline.Submit(index, ea.Body.Span);
                Interlocked.Increment(ref counts[index]);
                idle.Touch();

                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
            };

            await channel.BasicConsumeAsync(
                queue: options.Queue + "." + index,
                autoAck: false,
                consumer: consumer,
                cancellationToken: ct);
        }

        await idle.WaitForSilenceAsync(ct);

        foreach (var channel in channels)
        {
            await channel.CloseAsync(ct);
            await channel.DisposeAsync();
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

        public async Task WaitForSilenceAsync(CancellationToken ct)
        {
            var startupDeadline = DateTime.UtcNow + startupTimeout;

            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250), ct);

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
    public ushort Prefetch { get; init; } = 1000;
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Espera pela primeira mensagem, antes de o tempo ocioso valer.</summary>
    public TimeSpan StartupTimeout { get; init; } = TimeSpan.FromMinutes(5);
}
