using System.Diagnostics;

namespace Pitwall.Consumer;

/// <summary>
/// Amostra CPU e memoria do proprio processo durante a rodada.
///
/// O cAdvisor mede os containers (brokers e banco); este amostrador mede o
/// processo .NET, que roda fora do Docker. Sem ele, o custo do consumidor
/// -- justamente onde Channels e Pipelines diferem -- ficaria invisivel.
///
/// Intervalo de 250 ms: fino o bastante para registrar picos de coleta de
/// lixo, largo o bastante para o proprio amostrador nao aparecer na medicao.
/// </summary>
public sealed class ResourceSampler : IAsyncDisposable
{
    private readonly Process _process = Process.GetCurrentProcess();
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;
    private readonly List<double> _cpuSamples = [];
    private readonly List<long> _memorySamples = [];
    private readonly Lock _gate = new();

    private TimeSpan _lastCpuTime;
    private long _lastTimestamp;

    public ResourceSampler(TimeSpan? interval = null)
    {
        _lastCpuTime = _process.TotalProcessorTime;
        _lastTimestamp = Stopwatch.GetTimestamp();
        _loop = Task.Run(() => SampleLoopAsync(interval ?? TimeSpan.FromMilliseconds(250)));
    }

    /// <summary>Uso medio de CPU do processo, em percentual de um nucleo.</summary>
    public double AverageCpuPercent
    {
        get { lock (_gate) { return _cpuSamples.Count > 0 ? _cpuSamples.Average() : 0; } }
    }

    public double PeakCpuPercent
    {
        get { lock (_gate) { return _cpuSamples.Count > 0 ? _cpuSamples.Max() : 0; } }
    }

    public double AverageMemoryMb
    {
        get { lock (_gate) { return _memorySamples.Count > 0 ? _memorySamples.Average() / 1024.0 / 1024.0 : 0; } }
    }

    public double PeakMemoryMb
    {
        get { lock (_gate) { return _memorySamples.Count > 0 ? _memorySamples.Max() / 1024.0 / 1024.0 : 0; } }
    }

    /// <summary>Coletas de lixo por geracao: explicam picos de latencia na cauda.</summary>
    public (int Gen0, int Gen1, int Gen2) Collections =>
        (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));

    public long AllocatedMb => GC.GetTotalAllocatedBytes() / 1024 / 1024;

    private async Task SampleLoopAsync(TimeSpan interval)
    {
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, _cts.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            _process.Refresh();

            var now = Stopwatch.GetTimestamp();
            var cpuTime = _process.TotalProcessorTime;

            var elapsedSeconds = (now - _lastTimestamp) / (double)Stopwatch.Frequency;
            var cpuSeconds = (cpuTime - _lastCpuTime).TotalSeconds;

            _lastTimestamp = now;
            _lastCpuTime = cpuTime;

            if (elapsedSeconds <= 0)
            {
                continue;
            }

            lock (_gate)
            {
                _cpuSamples.Add(cpuSeconds / elapsedSeconds * 100.0);
                _memorySamples.Add(_process.WorkingSet64);
            }
        }
    }

    public string Summary() =>
        $"CPU media={AverageCpuPercent:0.0}% pico={PeakCpuPercent:0.0}% | " +
        $"memoria media={AverageMemoryMb:0}MB pico={PeakMemoryMb:0}MB | " +
        $"GC {Collections.Gen0}/{Collections.Gen1}/{Collections.Gen2} | " +
        $"alocado={AllocatedMb}MB";

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        try
        {
            await _loop;
        }
        catch (OperationCanceledException)
        {
            // Encerramento normal.
        }

        _cts.Dispose();
        _process.Dispose();
    }
}
