using Confluent.Kafka;
using Pitwall.Contracts;

namespace Pitwall.Replayer.Sinks;

using Pitwall.Workload;

/// <summary>
/// Publica os eventos num topico do Kafka.
///
/// A configuracao e deliberadamente explicita: cada parametro daqui precisa
/// aparecer na tabela de configuracao do artigo, porque a comparacao com o
/// RabbitMQ so e valida se as garantias de entrega forem equivalentes
/// (at-least-once dos dois lados).
/// </summary>
public sealed class KafkaSink : IEventSink
{
    private readonly IProducer<int, byte[]> _producer;
    private readonly string _topic;

    private long _delivered;
    private long _failed;

    public KafkaSink(KafkaSinkOptions options)
    {
        _topic = options.Topic;

        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,

            // at-least-once: espera a confirmacao de todas as replicas em
            // sincronia. Equivale aos publisher confirms do RabbitMQ.
            Acks = Acks.All,

            // Sem compressao: as replicas da frota tem payload quase
            // identico e a compressao em lote renderia muito mais do que
            // renderia com telemetria real (ver docs/PLANO.md, secao 1a).
            CompressionType = CompressionType.None,

            // Agrupamento em lote. Sao os dois parametros que mais afetam a
            // troca entre latencia e vazao, entao ficam explicitos e viram
            // fatores caso o experimento precise varia-los.
            LingerMs = options.LingerMs,
            BatchSize = options.BatchSize,

            // Sem idempotencia: ela acrescenta sequenciamento e reordenacao
            // do lado do broker, o que mudaria a semantica em relacao ao
            // RabbitMQ e tornaria a comparacao desigual.
            EnableIdempotence = false,

            // Se a fila interna do cliente encher, bloqueia em vez de
            // descartar: perder evento silenciosamente invalidaria a rodada.
            QueueBufferingMaxMessages = options.QueueBufferingMaxMessages,
            QueueBufferingMaxKbytes = options.QueueBufferingMaxKbytes
        };

        _producer = new ProducerBuilder<int, byte[]>(config)
            .SetErrorHandler((_, e) => Console.Error.WriteLine($"[kafka] {e.Reason}"))
            .Build();
    }

    public string Name => "kafka";

    public long Delivered => Interlocked.Read(ref _delivered);
    public long Failed => Interlocked.Read(ref _failed);

    public ValueTask PublishAsync(in TelemetryEvent evt, CancellationToken ct)
    {
        // Um array novo por evento: o produtor e assincrono e mantem a
        // referencia ate a entrega, entao reaproveitar o buffer corromperia
        // mensagens ainda em voo. Sao 44 bytes em gen0, custo aceitavel.
        var payload = new byte[TelemetryCodec.Size];
        TelemetryCodec.Write(payload, evt);

        // A chave e o numero do carro: garante que todas as amostras de um
        // mesmo carro caiam na mesma particao, preservando a ordem por carro.
        // A agregacao em janela do modulo de processamento depende disso.
        var message = new Message<int, byte[]> { Key = evt.DriverNumber, Value = payload };

        // Produce e nao-bloqueante e entrega o resultado por callback. E o
        // que mantem o gerador em malha aberta: publicar nao pode esperar o
        // broker, senao a taxa passaria a ser ditada por ele.
        _producer.Produce(_topic, message, DeliveryHandler);

        return ValueTask.CompletedTask;
    }

    private void DeliveryHandler(DeliveryReport<int, byte[]> report)
    {
        if (report.Error.IsError)
        {
            Interlocked.Increment(ref _failed);
        }
        else
        {
            Interlocked.Increment(ref _delivered);
        }
    }

    public ValueTask FlushAsync(CancellationToken ct)
    {
        _producer.Flush(ct);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}

public sealed record KafkaSinkOptions
{
    public string BootstrapServers { get; init; } = "localhost:9092";
    public string Topic { get; init; } = "telemetry";
    public double LingerMs { get; init; } = 5;
    public int BatchSize { get; init; } = 65536;
    public int QueueBufferingMaxMessages { get; init; } = 1_000_000;
    public int QueueBufferingMaxKbytes { get; init; } = 1_048_576;
}
