namespace Pitwall.Processing.Core;

public sealed record ProcessingOptions
{
    /// <summary>
    /// Tamanho da janela de agregacao, em tempo de evento. Tempo de evento e
    /// nao de processamento: e o que torna o resultado identico nas seis
    /// variantes, independentemente de quando cada uma processou o dado.
    /// </summary>
    public TimeSpan WindowSize { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Valor de freio a partir do qual a amostra conta como frenagem. Na
    /// OpenF1 o canal e binario (0 ou 100), mas o limiar fica explicito para
    /// o caso de outra fonte usar escala continua.
    /// </summary>
    public short BrakeThreshold { get; init; } = 50;

    /// <summary>
    /// Custo sintetico por evento, em microssegundos. Zero por padrao.
    /// So deve ser usado se o experimento piloto mostrar que a agregacao e
    /// leve demais e as seis variantes empatam -- nesse caso a intensidade
    /// de processamento vira um fator explicito do experimento
    /// (ver docs/PLANO.md, secao 1d).
    /// </summary>
    public double SyntheticCostMicros { get; init; }
}
