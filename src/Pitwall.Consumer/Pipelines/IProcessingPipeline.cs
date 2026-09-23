namespace Pitwall.Consumer.Pipelines;

/// <summary>
/// O que muda entre as tres variantes da matriz: como o evento vai do
/// callback do broker ate o processamento. A logica de processamento em si e
/// a mesma nas tres (<c>Pitwall.Processing.Core</c>).
///
/// **Faixa (lane)** e a unidade de paralelismo: uma particao do Kafka ou uma
/// fila do RabbitMQ. Um carro cai sempre na mesma faixa, e cada faixa tem um
/// processador proprio -- e isso que preserva a ordem por carro e faz o
/// resultado independer do modo e do grau de paralelismo.
/// </summary>
public interface IProcessingPipeline : IAsyncDisposable
{
    string Mode { get; }

    /// <summary>
    /// Entrega os bytes crus de um evento na faixa indicada. Chamado pela
    /// thread que recebe do broker.
    /// </summary>
    void Submit(int lane, ReadOnlySpan<byte> payload);

    /// <summary>Drena o que ainda esta em transito e fecha as janelas abertas.</summary>
    Task CompleteAsync();
}
