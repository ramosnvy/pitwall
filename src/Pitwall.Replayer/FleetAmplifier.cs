using Pitwall.Contracts;

namespace Pitwall.Replayer;

/// <summary>
/// Transforma a telemetria de 20 carros na de uma frota de N carros.
///
/// Existem duas formas de tirar 100 mil eventos/s de uma corrida que produz
/// 75: comprimir o tempo 1341 vezes, ou simular 1341 vezes mais carros. A
/// segunda e a que corresponde ao cenario que motiva o trabalho -- telemetria,
/// IoT e frotas -- e preserva a cadencia real de cada sensor (3,7 Hz por
/// carro). A primeira distorceria o intervalo entre amostras de um mesmo
/// carro, que e justamente a caracteristica do dado.
///
/// Cada replica recebe um numero de carro proprio e um deslocamento de fase,
/// para que as replicas nao emitam todas no mesmo instante. Sem o
/// deslocamento, a frota inteira publicaria em rajadas sincronizadas, o que
/// favoreceria artificialmente o broker que agrupa melhor em lote.
/// </summary>
public sealed class FleetAmplifier(int factor)
{
    /// <summary>
    /// Numero de carros originais no dataset. Usado para que o numero do
    /// carro replicado nao colida com o de outra replica.
    /// </summary>
    private const int DriverNumberSpace = 100;

    public int Factor { get; } = factor > 0
        ? factor
        : throw new ArgumentOutOfRangeException(nameof(factor), "O fator precisa ser maior que zero.");

    /// <summary>
    /// Produz a replica <paramref name="replica"/> do evento. A replica 0 e o
    /// evento original, sem alteracao.
    /// </summary>
    public TelemetryEvent Replicate(in TelemetryEvent source, int replica, long sequence)
    {
        if (replica == 0)
        {
            return source with { Sequence = sequence };
        }

        return source with
        {
            Sequence = sequence,

            // Cada replica vira uma faixa propria de numeracao: a replica 3
            // do carro 44 e o carro 344. Mantem o carro de origem legivel,
            // o que ajuda a depurar o pipeline.
            DriverNumber = (short)(source.DriverNumber + replica * DriverNumberSpace),

            // Desloca a amostra no tempo dentro do intervalo de amostragem,
            // espalhando as replicas em vez de sincroniza-las.
            EventTime = source.EventTime + PhaseOffset(replica)
        };
    }

    /// <summary>
    /// Deslocamento de fase da replica dentro do intervalo entre amostras.
    /// A OpenF1 amostra a 3,7 Hz, ou seja, uma amostra a cada 270 ms.
    /// </summary>
    private TimeSpan PhaseOffset(int replica) =>
        TimeSpan.FromMilliseconds(270.0 * replica / Factor);

    /// <summary>
    /// Quantos carros a frota tem, dado o numero de carros do dataset.
    /// </summary>
    public int FleetSize(int distinctDrivers) => distinctDrivers * Factor;
}
