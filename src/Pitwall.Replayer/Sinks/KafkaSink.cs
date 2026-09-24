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
    private long _queueFullWaits;

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

            // Sem idempotencia: ela acrescenta sequenciamento e reordenacao
            // do lado do broker, o que mudaria a semantica em relacao ao
            // RabbitMQ e tornaria a comparacao desigual.
            EnableIdempotence = false
        };

        // Os demais parametros so sao atribuidos quando pedidos: sem valor, vale
        // o padrao da librdkafka. Antes, SocketNagleDisable recebia false sempre
        // que a opcao nao era passada, e o Kafka rodou com o Nagle ligado contra
        // o padrao da biblioteca (docs/AUDITORIA-CONFIG.md, defeito 1).
        if (options.LingerMs is { } linger) config.LingerMs = linger;
        if (options.BatchSize is { } batch) config.BatchSize = batch;
        if (options.SocketNagleDisable is { } nagle) config.SocketNagleDisable = nagle;

        // Fila interna do cliente. Cheia, o produtor espera em vez de
        // descartar (PublishAsync): perder evento invalidaria a rodada.
        if (options.QueueBufferingMaxMessages is { } maxMessages) config.QueueBufferingMaxMessages = maxMessages;
        if (options.QueueBufferingMaxKbytes is { } maxKbytes) config.QueueBufferingMaxKbytes = maxKbytes;

        EffectiveConfig = RunSettings.Format(config
            .Where(kv => kv.Key != "bootstrap.servers")
            .Select(kv => new KeyValuePair<string, string?>(kv.Key, kv.Value)));

        _producer = new ProducerBuilder<int, byte[]>(config)
            .SetErrorHandler((_, e) => Console.Error.WriteLine($"[kafka] {e.Reason}"))
            .Build();
    }

    public string Name => "kafka";

    /// <summary>Parametros atribuidos explicitamente; o resto e padrao da librdkafka.</summary>
    public string EffectiveConfig { get; }

    public long Delivered => Interlocked.Read(ref _delivered);
    public long Failed => Interlocked.Read(ref _failed);

    /// <summary>Vezes em que a fila local encheu e o produtor esperou.</summary>
    public long QueueFullWaits => Interlocked.Read(ref _queueFullWaits);

    public ValueTask PublishAsync(in TelemetryEvent evt, CancellationToken ct)
    {
        // Um array novo por evento: o produtor e assincrono e mantem a
        // referencia ate a entrega, entao reaproveitar o buffer corromperia
        // mensagens ainda em voo. Sao 46 bytes em gen0, custo aceitavel.
        var payload = new byte[TelemetryCodec.Size];
        TelemetryCodec.Write(payload, evt);

        // A chave e o numero do carro: garante que todas as amostras de um
        // mesmo carro caiam na mesma particao, preservando a ordem por carro.
        // A agregacao em janela do modulo de processamento depende disso.
        var message = new Message<int, byte[]> { Key = evt.DriverNumber, Value = payload };

        // Produce e nao-bloqueante e entrega o resultado por callback. E o
        // que mantem o gerador em malha aberta: publicar nao pode esperar o
        // broker, senao a taxa passaria a ser ditada por ele.
        // Fila local cheia: contrapressao, nao excecao. Na matriz oficial, a
        // 400 mil ev/s com a maquina saturada, a fila da librdkafka (1 milhao
        // de mensagens) encheu, o Produce lancou Local_QueueFull e o produtor
        // morreu no meio da rodada. O padrao documentado e servir os
        // relatorios de entrega com Poll, liberando espaco, e tentar de novo.
        // O tempo de espera aparece como atraso do gerador, e a rodada e
        // invalidada pelo criterio de jitter -- que e o tratamento correto.
        while (true)
        {
            try
            {
                _producer.Produce(_topic, message, DeliveryHandler);
                break;
            }
            catch (ProduceException<int, byte[]> e) when (e.Error.Code == ErrorCode.Local_QueueFull)
            {
                Interlocked.Increment(ref _queueFullWaits);
                _producer.Poll(TimeSpan.FromMilliseconds(100));
            }
        }

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
    // Nulo = padrao da librdkafka 2.15.1, anotado ao lado de cada um.

    /// <summary>linger.ms; padrao 5.</summary>
    public double? LingerMs { get; init; }

    /// <summary>batch.size em bytes; padrao 1.000.000.</summary>
    public int? BatchSize { get; init; }

    /// <summary>
    /// socket.nagle.disable; padrao true (Nagle desligado), como o NoDelay do
    /// RabbitMQ.Client. false liga o Nagle, como nas rodadas anteriores a
    /// auditoria de 24/09.
    /// </summary>
    public bool? SocketNagleDisable { get; init; }

    /// <summary>queue.buffering.max.messages; padrao 100.000.</summary>
    public int? QueueBufferingMaxMessages { get; init; }

    /// <summary>queue.buffering.max.kbytes; padrao 1.048.576.</summary>
    public int? QueueBufferingMaxKbytes { get; init; }
}
