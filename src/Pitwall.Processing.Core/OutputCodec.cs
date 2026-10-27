using System.Buffers.Binary;
using Pitwall.Contracts;

namespace Pitwall.Processing.Core;

/// <summary>Estado da janela corrente de um carro (ver TelemetryProcessor.Snapshot).</summary>
public readonly record struct WindowSnapshot(
    long WindowStartTicks,
    int EventCount,
    float AvgSpeed,
    short MaxSpeed,
    int HardBrakings,
    int GearChanges);

/// <summary>
/// Saida do segundo cenario (fase 9A, docs/APROFUNDAMENTO.md secao 8.6): o
/// consumidor republica o resultado num segundo topico do mesmo broker, e o
/// painel o le. Dois formatos, porque o volume da saida decide o resultado:
///
///   enriquecida  uma mensagem por evento de entrada (1 para 1): o evento
///                original mais o estado da janela do carro naquele instante;
///   resumida     uma mensagem por janela fechada, cerca de 1% da entrada.
///
/// As duas carregam o horario de publicacao original do evento de entrada, o
/// que permite medir a latencia de ponta a ponta no painel com o mesmo relogio.
/// Binario, little-endian e de tamanho fixo, como a entrada.
/// </summary>
public static class OutputCodec
{
    /// <summary>Evento de entrada (46 bytes) mais o estado da janela.</summary>
    public const int EnrichedSize = TelemetryCodec.Size + 4 + 4 + 2 + 4 + 4;

    /// <summary>
    /// Janela fechada. O horario de publicacao e o do evento que fechou a
    /// janela: a latencia medida e a do caminho, e nao a duracao da janela.
    /// </summary>
    public const int SummarySize = 4 + 8 + 4 + 4 + 2 + 4 + 4 + 8;

    public static void WriteEnriched(Span<byte> destination, in TelemetryEvent evt, in WindowSnapshot window)
    {
        TelemetryCodec.Write(destination, evt);

        var tail = destination[TelemetryCodec.Size..];
        BinaryPrimitives.WriteInt32LittleEndian(tail[0..], window.EventCount);
        BinaryPrimitives.WriteSingleLittleEndian(tail[4..], window.AvgSpeed);
        BinaryPrimitives.WriteInt16LittleEndian(tail[8..], window.MaxSpeed);
        BinaryPrimitives.WriteInt32LittleEndian(tail[10..], window.HardBrakings);
        BinaryPrimitives.WriteInt32LittleEndian(tail[14..], window.GearChanges);
    }

    public static (TelemetryEvent Event, WindowSnapshot Window) ReadEnriched(ReadOnlySpan<byte> source)
    {
        var evt = TelemetryCodec.Read(source);
        var tail = source[TelemetryCodec.Size..];

        var window = new WindowSnapshot(
            WindowStartTicks: 0,
            EventCount: BinaryPrimitives.ReadInt32LittleEndian(tail[0..]),
            AvgSpeed: BinaryPrimitives.ReadSingleLittleEndian(tail[4..]),
            MaxSpeed: BinaryPrimitives.ReadInt16LittleEndian(tail[8..]),
            HardBrakings: BinaryPrimitives.ReadInt32LittleEndian(tail[10..]),
            GearChanges: BinaryPrimitives.ReadInt32LittleEndian(tail[14..]));

        return (evt, window);
    }

    public static void WriteSummary(Span<byte> destination, in DriverWindowStats stats, long triggerPublishedTicks)
    {
        BinaryPrimitives.WriteInt32LittleEndian(destination[0..], stats.DriverNumber);
        BinaryPrimitives.WriteInt64LittleEndian(destination[4..], stats.WindowStart.UtcTicks);
        BinaryPrimitives.WriteInt32LittleEndian(destination[12..], stats.EventCount);
        BinaryPrimitives.WriteSingleLittleEndian(destination[16..], stats.AvgSpeed);
        BinaryPrimitives.WriteInt16LittleEndian(destination[20..], stats.MaxSpeed);
        BinaryPrimitives.WriteInt32LittleEndian(destination[22..], stats.HardBrakings);
        BinaryPrimitives.WriteInt32LittleEndian(destination[26..], stats.GearChanges);
        BinaryPrimitives.WriteInt64LittleEndian(destination[30..], triggerPublishedTicks);
    }

    public static (DriverWindowStats Stats, long PublishedTicks) ReadSummary(ReadOnlySpan<byte> source, TimeSpan windowSize)
    {
        var start = new DateTimeOffset(BinaryPrimitives.ReadInt64LittleEndian(source[4..]), TimeSpan.Zero);

        var stats = new DriverWindowStats
        {
            DriverNumber = BinaryPrimitives.ReadInt32LittleEndian(source[0..]),
            WindowStart = start,
            WindowEnd = start + windowSize,
            EventCount = BinaryPrimitives.ReadInt32LittleEndian(source[12..]),
            AvgSpeed = BinaryPrimitives.ReadSingleLittleEndian(source[16..]),
            MaxSpeed = BinaryPrimitives.ReadInt16LittleEndian(source[20..]),
            HardBrakings = BinaryPrimitives.ReadInt32LittleEndian(source[22..]),
            GearChanges = BinaryPrimitives.ReadInt32LittleEndian(source[26..])
        };

        return (stats, BinaryPrimitives.ReadInt64LittleEndian(source[30..]));
    }
}

/// <summary>
/// Conferencia da saida enriquecida, no mesmo espirito do ResultDigest: soma e
/// XOR, independentes da ordem de chegada. Com a mesma entrada, as seis
/// combinacoes precisam chegar ao mesmo valor no painel. A velocidade media
/// fica de fora, por ser ponto flutuante.
/// </summary>
public sealed class EnrichedDigest
{
    private readonly Lock _gate = new();
    private long _messages;
    private long _windowEventsSum;
    private ulong _hash;

    public long Messages => _messages;
    public ulong Hash => _hash;

    public void Add(in TelemetryEvent evt, in WindowSnapshot window)
    {
        var h = Mix(
            (ulong)evt.Sequence,
            (ulong)evt.DriverNumber,
            (ulong)evt.EventTime.UtcTicks,
            (ulong)window.EventCount,
            (ulong)window.MaxSpeed,
            (ulong)window.HardBrakings,
            (ulong)window.GearChanges);

        lock (_gate)
        {
            _messages++;
            _windowEventsSum += window.EventCount;
            _hash ^= h;
        }
    }

    public override string ToString() =>
        $"mensagens={_messages:N0} somaEventosJanela={_windowEventsSum:N0} hash={_hash:X16}";

    private static ulong Mix(params ulong[] values)
    {
        // FNV-1a sobre os bytes de cada valor, como o ResultDigest.
        var hash = 14695981039346656037UL;

        foreach (var value in values)
        {
            for (var shift = 0; shift < 64; shift += 8)
            {
                hash ^= (byte)(value >> shift);
                hash *= 1099511628211UL;
            }
        }

        return hash;
    }
}
