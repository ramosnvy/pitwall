using System.Runtime.InteropServices;

namespace Pitwall.Contracts;

/// <summary>
/// Solicita ao Windows uma resolucao de timer mais fina para o processo.
///
/// O relogio padrao do Windows tem granularidade de 15,6 ms: esperas
/// temporizadas curtas sao arredondadas para cima ate o proximo tique. Isso
/// atinge de forma desigual as duas bibliotecas de cliente. A librdkafka,
/// usada pelo Confluent.Kafka, e nativa e agenda seus envios e buscas com
/// esperas temporizadas (linger.ms, fetch.wait.max.ms). O RabbitMQ.Client e
/// .NET assincrono, dirigido por conclusao de I/O, e praticamente nao depende
/// de timer. Sem este ajuste, o Kafka seria penalizado pelo sistema
/// operacional, nao pela sua arquitetura.
///
/// Desde o Windows 10 2004 a solicitacao vale apenas para o proprio processo,
/// entao produtor e consumidor precisam pedir cada um.
/// </summary>
public static class TimerResolution
{
    [DllImport("winmm.dll", EntryPoint = "timeBeginPeriod")]
    private static extern uint TimeBeginPeriod(uint milliseconds);

    [DllImport("winmm.dll", EntryPoint = "timeEndPeriod")]
    private static extern uint TimeEndPeriod(uint milliseconds);

    /// <summary>
    /// Solicita a resolucao indicada ate o descarte do objeto devolvido. Zero
    /// ou sistema que nao seja Windows nao altera nada.
    /// </summary>
    public static IDisposable Request(uint milliseconds)
    {
        if (milliseconds == 0 || !OperatingSystem.IsWindows())
        {
            return new Releaser(0);
        }

        TimeBeginPeriod(milliseconds);
        return new Releaser(milliseconds);
    }

    private sealed class Releaser(uint milliseconds) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed || milliseconds == 0)
            {
                return;
            }

            TimeEndPeriod(milliseconds);
            _disposed = true;
        }
    }
}
