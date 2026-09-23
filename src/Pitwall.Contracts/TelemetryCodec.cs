using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Pitwall.Contracts;

/// <summary>
/// Serializacao binaria de tamanho fixo do evento.
///
/// A escolha por binario, e nao JSON, e deliberada: o objeto do estudo e o
/// custo das arquiteturas de transporte e processamento, nao o custo do
/// parser. Um payload JSON faria o tempo de desserializacao dominar a medicao
/// e achataria a diferenca entre Channels e Pipelines.
///
/// O formato e o mesmo para as tres variantes (direct, channels e pipelines),
/// o que mantem a comparacao justa.
/// </summary>
public static class TelemetryCodec
{
    /// <summary>Tamanho de um evento serializado, em bytes.</summary>
    public const int Size = 8 + 4 + 2 + 8 + 8 + 2 + 4 + 2 + 2 + 2 + 2;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Write(Span<byte> destination, in TelemetryEvent e)
    {
        if (destination.Length < Size)
        {
            throw new ArgumentException($"Buffer precisa de ao menos {Size} bytes.", nameof(destination));
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination[0..], e.Sequence);
        BinaryPrimitives.WriteInt32LittleEndian(destination[8..], e.SessionKey);
        BinaryPrimitives.WriteInt16LittleEndian(destination[12..], e.DriverNumber);
        BinaryPrimitives.WriteInt64LittleEndian(destination[14..], e.EventTime.UtcTicks);
        BinaryPrimitives.WriteInt64LittleEndian(destination[22..], e.PublishedTicks);
        BinaryPrimitives.WriteInt16LittleEndian(destination[30..], e.Speed);
        BinaryPrimitives.WriteInt32LittleEndian(destination[32..], e.Rpm);
        BinaryPrimitives.WriteInt16LittleEndian(destination[36..], e.Gear);
        BinaryPrimitives.WriteInt16LittleEndian(destination[38..], e.Throttle);
        BinaryPrimitives.WriteInt16LittleEndian(destination[40..], e.Brake);
        BinaryPrimitives.WriteInt16LittleEndian(destination[42..], e.Drs);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TelemetryEvent Read(ReadOnlySpan<byte> source)
    {
        if (source.Length < Size)
        {
            throw new ArgumentException($"Buffer precisa de ao menos {Size} bytes.", nameof(source));
        }

        return new TelemetryEvent
        {
            Sequence = BinaryPrimitives.ReadInt64LittleEndian(source[0..]),
            SessionKey = BinaryPrimitives.ReadInt32LittleEndian(source[8..]),
            DriverNumber = BinaryPrimitives.ReadInt16LittleEndian(source[12..]),
            EventTime = new DateTimeOffset(BinaryPrimitives.ReadInt64LittleEndian(source[14..]), TimeSpan.Zero),
            PublishedTicks = BinaryPrimitives.ReadInt64LittleEndian(source[22..]),
            Speed = BinaryPrimitives.ReadInt16LittleEndian(source[30..]),
            Rpm = BinaryPrimitives.ReadInt32LittleEndian(source[32..]),
            Gear = BinaryPrimitives.ReadInt16LittleEndian(source[36..]),
            Throttle = BinaryPrimitives.ReadInt16LittleEndian(source[38..]),
            Brake = BinaryPrimitives.ReadInt16LittleEndian(source[40..]),
            Drs = BinaryPrimitives.ReadInt16LittleEndian(source[42..])
        };
    }

    /// <summary>
    /// Le de uma sequencia possivelmente fragmentada, como a que o
    /// <c>PipeReader</c> entrega. Devolve false quando ainda nao chegaram
    /// bytes suficientes para um evento completo.
    /// </summary>
    public static bool TryRead(ref ReadOnlySequence<byte> buffer, out TelemetryEvent value)
    {
        if (buffer.Length < Size)
        {
            value = default;
            return false;
        }

        var slice = buffer.Slice(0, Size);

        if (slice.IsSingleSegment)
        {
            value = Read(slice.First.Span);
        }
        else
        {
            Span<byte> scratch = stackalloc byte[Size];
            slice.CopyTo(scratch);
            value = Read(scratch);
        }

        buffer = buffer.Slice(Size);
        return true;
    }
}
