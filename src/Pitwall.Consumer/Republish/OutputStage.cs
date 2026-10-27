using Pitwall.Contracts;
using Pitwall.Processing.Core;

namespace Pitwall.Consumer.Republish;

/// <summary>Volume da saida do segundo cenario (docs/APROFUNDAMENTO.md secao 8.6).</summary>
public enum OutputKind
{
    /// <summary>Uma mensagem por evento de entrada (1 para 1).</summary>
    Enriquecida,

    /// <summary>Uma mensagem por janela fechada, cerca de 1% da entrada.</summary>
    Resumida
}

/// <summary>Recebe cada saida pronta, com o identificador da pendencia que ela fecha.</summary>
public delegate void OutputCallback(long id, ReadOnlySpan<byte> payload);

/// <summary>
/// Processamento de uma faixa no segundo cenario: o mesmo TelemetryProcessor
/// dos tres modos, mais a montagem da saida e as pendencias da faixa. O que
/// muda entre os modos e o que fazem com a saida (OutputCallback).
///
/// Na saida resumida, uma entrada so pode ser liberada quando a janela que a
/// contem fechou e a saida dessa janela foi confirmada. Por isso cada janela
/// aberta e uma pendencia que comeca na posicao do seu primeiro evento. Na
/// enriquecida, cada evento e uma pendencia.
/// </summary>
public sealed class OutputStage
{
    private readonly int _lane;
    private readonly OutputKind _kind;
    private readonly ReleaseTracker _tracker;
    private readonly Action<DriverWindowStats> _onWindow;
    private readonly TelemetryProcessor _processor;
    private readonly long _windowTicks;

    // Resumida: a janela aberta de cada carro e a pendencia dela.
    private readonly Dictionary<int, (long WindowStart, long Id)> _open = new(capacity: 1024);

    private OutputCallback? _emit;
    private long _triggerTicks;

    public OutputStage(int lane, OutputKind kind, ProcessingOptions options, Action<DriverWindowStats> onWindow, ReleaseTracker tracker)
    {
        _lane = lane;
        _kind = kind;
        _tracker = tracker;
        _onWindow = onWindow;
        _windowTicks = options.WindowSize.Ticks;
        _processor = new TelemetryProcessor(options, OnWindowClosed);
    }

    public int OutputSize => _kind == OutputKind.Enriquecida ? OutputCodec.EnrichedSize : OutputCodec.SummarySize;

    public void Process(in TelemetryEvent evt, long position, OutputCallback emit)
    {
        _emit = emit;
        _triggerTicks = evt.PublishedTicks;

        if (_kind == OutputKind.Enriquecida)
        {
            _processor.Process(evt);

            var id = _tracker.Open(_lane, position);
            Span<byte> buffer = stackalloc byte[OutputCodec.EnrichedSize];
            OutputCodec.WriteEnriched(buffer, evt, _processor.Snapshot(evt.DriverNumber));
            emit(id, buffer);
        }
        else
        {
            var ticks = evt.EventTime.UtcTicks;
            var windowStart = ticks - ticks % _windowTicks;

            // Se a janela do carro virou, o processador emite a anterior aqui
            // dentro (OnWindowClosed), e a pendencia dela ainda esta em _open.
            _processor.Process(evt);

            if (!_open.TryGetValue(evt.DriverNumber, out var current) || windowStart > current.WindowStart)
            {
                _open[evt.DriverNumber] = (windowStart, _tracker.Open(_lane, position));
            }
        }

        _tracker.Processed(_lane, position);
    }

    /// <summary>Fecha as janelas abertas no fim da rodada.</summary>
    public void Complete(OutputCallback emit)
    {
        _emit = emit;

        // Sem evento que feche a janela, nao ha horario de publicacao de
        // referencia: o painel conta a saida, mas nao mede a latencia dela.
        _triggerTicks = 0;
        _processor.Complete();
    }

    private void OnWindowClosed(DriverWindowStats stats)
    {
        _onWindow(stats);

        if (_kind != OutputKind.Resumida)
        {
            return;
        }

        var id = _open[stats.DriverNumber].Id;
        Span<byte> buffer = stackalloc byte[OutputCodec.SummarySize];
        OutputCodec.WriteSummary(buffer, stats, _triggerTicks);
        _emit!(id, buffer);
    }
}
