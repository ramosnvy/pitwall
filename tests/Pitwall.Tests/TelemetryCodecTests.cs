using System.Buffers;
using Pitwall.Contracts;

namespace Pitwall.Tests;

/// <summary>
/// O codec e o contrato entre produtor e consumidor. Um erro aqui corrompe
/// silenciosamente todos os resultados, entao cada campo e verificado.
/// </summary>
public class TelemetryCodecTests
{
    private static TelemetryEvent Sample(int driverNumber = 44) => new()
    {
        Sequence = 123_456_789_012,
        SessionKey = 9472,
        DriverNumber = driverNumber,
        EventTime = new DateTimeOffset(2024, 3, 2, 15, 30, 0, TimeSpan.Zero).AddTicks(1234),
        PublishedTicks = 987_654_321_098,
        Speed = 312,
        Rpm = 11_850,
        Gear = 8,
        Throttle = 100,
        Brake = 0,
        Drs = 12
    };

    [Fact]
    public void Size_is_46_bytes()
    {
        Assert.Equal(46, TelemetryCodec.Size);
    }

    [Fact]
    public void Round_trip_preserves_every_field()
    {
        var original = Sample();
        var buffer = new byte[TelemetryCodec.Size];

        TelemetryCodec.Write(buffer, original);
        var decoded = TelemetryCodec.Read(buffer);

        Assert.Equal(original, decoded);
    }

    /// <summary>
    /// Regressao do estouro encontrado no piloto: com fator de frota 671, o
    /// numero do carro chegava a 67.081 e o campo short virava negativo. Estes
    /// valores precisam atravessar o codec intactos.
    /// </summary>
    [Theory]
    [InlineData(32_767)]
    [InlineData(32_768)]
    [InlineData(67_081)]
    [InlineData(536_381)]
    public void Driver_numbers_above_short_range_survive_round_trip(int driverNumber)
    {
        var buffer = new byte[TelemetryCodec.Size];

        TelemetryCodec.Write(buffer, Sample(driverNumber));
        var decoded = TelemetryCodec.Read(buffer);

        Assert.Equal(driverNumber, decoded.DriverNumber);
    }

    /// <summary>
    /// O PipeReader entrega sequencias fragmentadas: um registro pode cruzar a
    /// fronteira entre dois segmentos. TryRead precisa remontar o registro.
    /// </summary>
    [Fact]
    public void TryRead_reassembles_a_record_split_across_segments()
    {
        var original = Sample();
        var bytes = new byte[TelemetryCodec.Size];
        TelemetryCodec.Write(bytes, original);

        var first = new Segment(bytes.AsMemory(0, 17));
        var last = first.Append(bytes.AsMemory(17));
        var sequence = new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);

        Assert.False(sequence.IsSingleSegment);
        Assert.True(TelemetryCodec.TryRead(ref sequence, out var decoded));
        Assert.Equal(original, decoded);
        Assert.True(sequence.IsEmpty);
    }

    [Fact]
    public void TryRead_returns_false_for_an_incomplete_record()
    {
        var sequence = new ReadOnlySequence<byte>(new byte[TelemetryCodec.Size - 1]);

        Assert.False(TelemetryCodec.TryRead(ref sequence, out _));
        Assert.Equal(TelemetryCodec.Size - 1, sequence.Length);
    }

    [Fact]
    public void TryRead_consumes_consecutive_records_in_order()
    {
        var bytes = new byte[TelemetryCodec.Size * 3];

        for (var i = 0; i < 3; i++)
        {
            TelemetryCodec.Write(bytes.AsSpan(i * TelemetryCodec.Size), Sample() with { Sequence = i });
        }

        var sequence = new ReadOnlySequence<byte>(bytes);

        for (var i = 0; i < 3; i++)
        {
            Assert.True(TelemetryCodec.TryRead(ref sequence, out var decoded));
            Assert.Equal(i, decoded.Sequence);
        }

        Assert.True(sequence.IsEmpty);
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
