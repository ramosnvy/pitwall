namespace Pitwall.Contracts;

/// <summary>
/// Um ponto de telemetria de um carro, na forma em que trafega pelo broker.
///
/// E um struct para que o caminho quente do replayer nao aloque por evento:
/// a taxas de dezenas de milhares de eventos por segundo, a pressao de GC
/// causada por uma alocacao por evento apareceria nas medicoes e mascararia
/// a diferenca entre as arquiteturas.
/// </summary>
public readonly record struct TelemetryEvent
{
    /// <summary>Sequencial atribuido pelo replayer. Identifica o evento de ponta a ponta.</summary>
    public required long Sequence { get; init; }

    /// <summary>Sessao da OpenF1 de onde o dado veio.</summary>
    public required int SessionKey { get; init; }

    /// <summary>
    /// Numero do carro. E int e nao short porque a multiplicacao de frota gera
    /// dezenas de milhares de carros: com short, um fator de frota acima de 327
    /// estourava o tipo e produzia numero negativo.
    /// </summary>
    public required int DriverNumber { get; init; }

    /// <summary>Instante original da amostra na corrida.</summary>
    public required DateTimeOffset EventTime { get; init; }

    /// <summary>
    /// Instante em que o replayer publicou no broker, em ticks de
    /// <see cref="System.Diagnostics.Stopwatch"/>. E a base do calculo de
    /// latencia: produtor e consumidor rodam na mesma maquina, entao o
    /// contador e comparavel entre os processos.
    /// </summary>
    public required long PublishedTicks { get; init; }

    public required short Speed { get; init; }
    public required int Rpm { get; init; }
    public required short Gear { get; init; }
    public required short Throttle { get; init; }
    public required short Brake { get; init; }
    public required short Drs { get; init; }
}
