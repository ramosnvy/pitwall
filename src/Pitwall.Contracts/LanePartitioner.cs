using System.Buffers.Binary;

namespace Pitwall.Contracts;

/// <summary>Como o numero do carro vira faixa (fila do RabbitMQ, particao do Kafka).</summary>
public enum LaneHash
{
    /// <summary>
    /// CRC32 da chave, a mesma funcao do partitioner padrao da librdkafka
    /// (consistent_random): crc32(chave) % particoes, com a chave int32 em
    /// big-endian, como o Serializers.Int32 do Confluent.Kafka a grava. Com
    /// ela, os dois brokers recebem exatamente a mesma divisao dos carros.
    /// </summary>
    Crc32,

    /// <summary>
    /// carro % faixas. Foi a funcao da matriz 7e283c2. Com a frota
    /// multiplicada (replica = carro + r * 100, e 100 = 0 mod 4), a faixa
    /// depende so do piloto original e as filas recebem 25/15/25/35% da carga
    /// (docs/IMPLEMENTACAO.md, secao 1). Mantida para reproduzir aquela matriz.
    /// </summary>
    Modulo
}

public static class LanePartitioner
{
    private static readonly uint[] Table = BuildTable();

    public static int Lane(int driverNumber, int lanes, LaneHash hash) => hash switch
    {
        LaneHash.Crc32 => (int)(KeyCrc32(driverNumber) % (uint)lanes),
        LaneHash.Modulo => (driverNumber & int.MaxValue) % lanes,
        _ => throw new ArgumentOutOfRangeException(nameof(hash), hash, null)
    };

    public static LaneHash Parse(string value) => value.ToLowerInvariant() switch
    {
        "crc32" => LaneHash.Crc32,
        "modulo" or "mod" => LaneHash.Modulo,
        _ => throw new ArgumentException($"Funcao de faixa '{value}' desconhecida. Use crc32 ou modulo.")
    };

    /// <summary>CRC32 da chave como o Kafka a ve: int32 em big-endian.</summary>
    public static uint KeyCrc32(int key)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, key);
        return Crc32(bytes);
    }

    /// <summary>
    /// CRC-32 IEEE 802.3 (polinomio refletido 0xEDB88320), o mesmo do zlib e
    /// do rd_crc32 da librdkafka.
    /// </summary>
    public static uint Crc32(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var b in data)
        {
            crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        }
        return ~crc;
    }

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (var i = 0u; i < 256; i++)
        {
            var c = i;
            for (var k = 0; k < 8; k++)
            {
                c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            }
            table[i] = c;
        }
        return table;
    }
}
