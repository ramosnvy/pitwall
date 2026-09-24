using Pitwall.Contracts;

namespace Pitwall.Workload;

/// <summary>
/// Destino dos eventos publicados. As implementacoes de broker (Kafka e
/// RabbitMQ) entram aqui; a <see cref="NullSink"/> permite medir o proprio
/// gerador de carga, sem broker nenhum no caminho.
/// </summary>
public interface IEventSink : IAsyncDisposable
{
    string Name { get; }

    /// <summary>
    /// Configuracao efetiva do cliente, no formato de <see cref="RunSettings"/>,
    /// para o relatorio da rodada.
    /// </summary>
    string EffectiveConfig => "padrao";

    ValueTask PublishAsync(in TelemetryEvent evt, CancellationToken ct);

    /// <summary>Garante que tudo que foi publicado saiu do buffer local.</summary>
    ValueTask FlushAsync(CancellationToken ct);
}

/// <summary>
/// Descarta os eventos, apenas contando. Serve para verificar que o gerador
/// sustenta a taxa alvo: se ele nao consegue nem com o destino vazio, um
/// resultado ruim com broker seria culpa do gerador, nao da arquitetura.
/// </summary>
public sealed class NullSink : IEventSink
{
    public string Name => "null";

    public long Count { get; private set; }

    public ValueTask PublishAsync(in TelemetryEvent evt, CancellationToken ct)
    {
        Count++;
        return ValueTask.CompletedTask;
    }

    public ValueTask FlushAsync(CancellationToken ct) => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
