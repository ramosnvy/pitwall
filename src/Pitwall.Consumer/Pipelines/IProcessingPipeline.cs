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
    ///
    /// Os bytes sao lidos antes de o metodo retornar, entao o chamador pode
    /// liberar o buffer logo depois. A tarefa devolvida so fica pendente
    /// quando a fila interna esta cheia: e a contrapressao, e quem recebe do
    /// broker deve aguarda-la antes de entregar o proximo evento. Antes ela
    /// era aguardada aqui dentro, de forma sincrona, o que bloqueava o
    /// despachante assincrono do RabbitMQ.Client (REVISAO-TECNICA §2.3).
    /// </summary>
    ValueTask SubmitAsync(int lane, ReadOnlySpan<byte> payload);

    /// <summary>
    /// Registros incompletos encontrados ao fechar a faixa. Qualquer valor
    /// acima de zero invalida a rodada (REVISAO-TECNICA §2.4).
    /// </summary>
    long IncompleteRecords => 0;

    /// <summary>Drena o que ainda esta em transito e fecha as janelas abertas.</summary>
    Task CompleteAsync();
}
