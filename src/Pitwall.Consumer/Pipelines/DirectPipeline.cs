using Pitwall.Contracts;
using Pitwall.Processing.Core;

namespace Pitwall.Consumer.Pipelines;

/// <summary>
/// Baseline da matriz: o evento e processado na propria thread que o recebeu
/// do broker, sem fila intermediaria e sem troca de contexto.
///
/// E o grupo de controle do experimento. Sem ele nao se sabe se Channels e
/// Pipelines acrescentam ou custam desempenho -- so se sabe qual broker e
/// mais rapido com cada um deles.
/// </summary>
public sealed class DirectPipeline : IProcessingPipeline
{
    private readonly TelemetryProcessor[] _processors;
    private readonly LatencyRecorder _latency;

    public DirectPipeline(int lanes, ProcessingOptions options, Action<DriverWindowStats> onWindow, LatencyRecorder latency)
    {
        _latency = latency;
        _processors = Enumerable.Range(0, lanes)
            .Select(_ => new TelemetryProcessor(options, onWindow))
            .ToArray();
    }

    public string Mode => "direct";

    public ValueTask SubmitAsync(int lane, ReadOnlySpan<byte> payload)
    {
        var evt = TelemetryCodec.Read(payload);

        _processors[lane].Process(evt);
        _latency.Record(lane, evt.PublishedTicks);

        return ValueTask.CompletedTask;
    }

    public Task CompleteAsync()
    {
        foreach (var processor in _processors)
        {
            processor.Complete();
        }

        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
