using System.Diagnostics;
using System.Globalization;

namespace Pitwall.Contracts;

/// <summary>
/// Linha do tempo de uma rodada, para a investigacao da fase 4
/// (docs/DESENVOLVIMENTO.md): a cada intervalo, uma linha com o horario UTC,
/// o que o runtime .NET fez (pausas e coletas de lixo, alocacao, CPU do
/// processo, pool de threads) e os contadores registrados pelo programa.
///
/// O horario UTC e o que permite cruzar produtor, consumidor e o log de coleta
/// de lixo do broker. Roda numa thread propria, que dorme entre amostras: o
/// custo no caminho quente e so o dos contadores que o programa ja mantem.
/// </summary>
public sealed class RunTracer : IDisposable
{
    private readonly StreamWriter _writer;
    private readonly TimeSpan _interval;
    private readonly List<(string Name, Func<long> Read, bool Cumulative)> _probes = [];
    private readonly Dictionary<string, long> _previous = [];
    private readonly Thread _thread;
    private readonly Process _process = Process.GetCurrentProcess();
    private readonly long _start = Stopwatch.GetTimestamp();
    private volatile bool _stopping;

    private TimeSpan _lastPause;
    private long _lastAllocated;
    private TimeSpan _lastCpu;
    private readonly int[] _lastCollections = new int[3];

    public RunTracer(string path, TimeSpan interval)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        _writer = new StreamWriter(path, append: false);
        _interval = interval;
        _thread = new Thread(Loop) { IsBackground = true, Name = "run-tracer" };
    }

    /// <summary>Contador cumulativo; a linha traz o quanto ele andou no intervalo.</summary>
    public RunTracer Counter(string name, Func<long> read)
    {
        _probes.Add((name, read, true));
        return this;
    }

    /// <summary>Valor lido como esta, por exemplo um maximo que o leitor zera a cada leitura.</summary>
    public RunTracer Gauge(string name, Func<long> read)
    {
        _probes.Add((name, read, false));
        return this;
    }

    /// <summary>
    /// Estado da maquina virtual inteira, nao so do container: paginas sujas
    /// e em gravacao (/proc/meminfo) e tempo em que todas as tarefas ficaram
    /// paradas esperando disco ou memoria (PSI, /proc/pressure, linha "full").
    /// Dentro do container esses arquivos mostram a VM do WSL2. Sem eles
    /// (Windows), nao acrescenta nada.
    /// </summary>
    public RunTracer VmPressure()
    {
        if (!File.Exists("/proc/meminfo")) return this;

        Gauge("vm_dirty_kb", () => MemInfo("Dirty:"));
        Gauge("vm_writeback_kb", () => MemInfo("Writeback:"));

        if (File.Exists("/proc/pressure/io"))
        {
            Counter("vm_io_full_us", () => PressureFullTotal("/proc/pressure/io"));
            Counter("vm_mem_full_us", () => PressureFullTotal("/proc/pressure/memory"));
        }

        return this;
    }

    private static long MemInfo(string key)
    {
        foreach (var line in File.ReadLines("/proc/meminfo"))
        {
            if (line.StartsWith(key, StringComparison.Ordinal))
            {
                return long.Parse(line.AsSpan(key.Length).Trim().ToString().Split(' ')[0], CultureInfo.InvariantCulture);
            }
        }

        return -1;
    }

    // "full avg10=0.00 avg60=0.00 avg300=0.00 total=188553523": microssegundos
    // acumulados em que todas as tarefas ativas esperavam o recurso.
    private static long PressureFullTotal(string path)
    {
        foreach (var line in File.ReadLines(path))
        {
            if (line.StartsWith("full", StringComparison.Ordinal))
            {
                var total = line[(line.LastIndexOf("total=", StringComparison.Ordinal) + 6)..];
                return long.Parse(total, CultureInfo.InvariantCulture);
            }
        }

        return 0;
    }

    public void Start()
    {
        _lastPause = GC.GetTotalPauseDuration();
        _lastAllocated = GC.GetTotalAllocatedBytes();
        _lastCpu = _process.TotalProcessorTime;
        for (var g = 0; g < 3; g++) _lastCollections[g] = GC.CollectionCount(g);
        foreach (var probe in _probes.Where(p => p.Cumulative)) _previous[probe.Name] = probe.Read();

        _writer.WriteLine(string.Join(',',
            new[] { "utc", "elapsed_ms", "gc_pause_ms", "gc0", "gc1", "gc2", "alloc_mb", "cpu_ms",
                    "pool_threads", "pool_pending" }
            .Concat(_probes.Select(p => p.Name))));

        _thread.Start();
    }

    private void Loop()
    {
        while (!_stopping)
        {
            Thread.Sleep(_interval);
            Sample();
        }
    }

    private void Sample()
    {
        var pause = GC.GetTotalPauseDuration();
        var allocated = GC.GetTotalAllocatedBytes();
        _process.Refresh();
        var cpu = _process.TotalProcessorTime;

        var fields = new List<string>
        {
            DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture),
            ((Stopwatch.GetTimestamp() - _start) * 1000 / Stopwatch.Frequency).ToString(CultureInfo.InvariantCulture),
            (pause - _lastPause).TotalMilliseconds.ToString("0.00", CultureInfo.InvariantCulture)
        };

        for (var g = 0; g < 3; g++)
        {
            var count = GC.CollectionCount(g);
            fields.Add((count - _lastCollections[g]).ToString(CultureInfo.InvariantCulture));
            _lastCollections[g] = count;
        }

        fields.Add(((allocated - _lastAllocated) / 1e6).ToString("0.00", CultureInfo.InvariantCulture));
        fields.Add((cpu - _lastCpu).TotalMilliseconds.ToString("0", CultureInfo.InvariantCulture));
        fields.Add(ThreadPool.ThreadCount.ToString(CultureInfo.InvariantCulture));
        fields.Add(ThreadPool.PendingWorkItemCount.ToString(CultureInfo.InvariantCulture));

        _lastPause = pause;
        _lastAllocated = allocated;
        _lastCpu = cpu;

        foreach (var (name, read, cumulative) in _probes)
        {
            var value = read();

            if (cumulative)
            {
                fields.Add((value - _previous[name]).ToString(CultureInfo.InvariantCulture));
                _previous[name] = value;
            }
            else
            {
                fields.Add(value.ToString(CultureInfo.InvariantCulture));
            }
        }

        _writer.WriteLine(string.Join(',', fields));
    }

    public void Dispose()
    {
        if (_thread.IsAlive)
        {
            _stopping = true;
            _thread.Join();
            Sample();
        }

        _writer.Dispose();
    }
}
