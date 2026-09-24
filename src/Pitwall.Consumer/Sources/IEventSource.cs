using Pitwall.Consumer.Pipelines;

namespace Pitwall.Consumer.Sources;

/// <summary>
/// De onde os eventos chegam. As duas implementacoes entregam os bytes crus
/// ao pipeline junto com a faixa de origem, sem desserializar -- a
/// desserializacao (ou a ausencia dela) e decisao de cada variante.
/// </summary>
public interface IEventSource
{
    string Broker { get; }

    /// <summary>Particoes do Kafka ou filas do RabbitMQ: o grau de paralelismo.</summary>
    int Lanes { get; }

    /// <summary>Consome ate a fonte esgotar ou o cancelamento. Devolve quantos eventos entregou.</summary>
    Task<long> ConsumeAsync(IProcessingPipeline pipeline, CancellationToken ct);

    /// <summary>Configuracao efetiva do cliente, no formato de RunSettings.</summary>
    string EffectiveConfig { get; }
}
