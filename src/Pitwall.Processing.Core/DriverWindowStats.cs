namespace Pitwall.Processing.Core;

/// <summary>
/// Resultado de uma janela fechada de um carro. E a unidade de saida do
/// processamento e o que vai para o PostgreSQL.
/// </summary>
public readonly record struct DriverWindowStats
{
    public required int DriverNumber { get; init; }
    public required DateTimeOffset WindowStart { get; init; }
    public required DateTimeOffset WindowEnd { get; init; }
    public required int EventCount { get; init; }
    public required float AvgSpeed { get; init; }
    public required short MaxSpeed { get; init; }

    /// <summary>Transicoes de freio solto para freio acionado dentro da janela.</summary>
    public required int HardBrakings { get; init; }

    /// <summary>Trocas de marcha dentro da janela.</summary>
    public required int GearChanges { get; init; }
}
