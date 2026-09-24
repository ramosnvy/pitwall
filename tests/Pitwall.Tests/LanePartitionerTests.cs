using System.Text;
using Pitwall.Contracts;

namespace Pitwall.Tests;

public class LanePartitionerTests
{
    // Numeros dos 20 carros do Bahrein 2024, a corrida da matriz.
    private static readonly int[] BahrainDrivers =
        [1, 2, 3, 4, 10, 11, 14, 16, 18, 20, 22, 23, 24, 27, 31, 44, 55, 63, 77, 81];

    [Fact]
    public void Crc32_matches_the_standard_check_value()
    {
        // Valor de verificacao do CRC-32 IEEE: crc32("123456789") = 0xCBF43926.
        Assert.Equal(0xCBF43926u, LanePartitioner.Crc32(Encoding.ASCII.GetBytes("123456789")));
    }

    [Fact]
    public void Key_is_hashed_as_big_endian_int32()
    {
        Assert.Equal(
            LanePartitioner.Crc32([0x00, 0x00, 0x01, 0x2C]),
            LanePartitioner.KeyCrc32(300));
    }

    [Fact]
    public void Same_car_always_gets_the_same_lane()
    {
        foreach (var hash in new[] { LaneHash.Crc32, LaneHash.Modulo })
        {
            Assert.Equal(
                LanePartitioner.Lane(1234, 4, hash),
                LanePartitioner.Lane(1234, 4, hash));
        }
    }

    [Fact]
    public void Modulo_reproduces_the_imbalance_of_the_original_matrix()
    {
        // Replicas somam multiplos de 100 = 0 (mod 4): a faixa depende so do
        // piloto original, e os 20 pilotos caem 5/3/5/7 nas quatro faixas.
        var counts = CountLanes(LaneHash.Modulo, replicas: 1000);

        Assert.Equal([5000, 3000, 5000, 7000], counts);
    }

    [Fact]
    public void Crc32_balances_the_multiplied_fleet()
    {
        var counts = CountLanes(LaneHash.Crc32, replicas: 1000);
        var total = counts.Sum();

        foreach (var c in counts)
        {
            Assert.InRange(c / (double)total, 0.24, 0.26);
        }
    }

    private static int[] CountLanes(LaneHash hash, int replicas)
    {
        var counts = new int[4];
        foreach (var driver in BahrainDrivers)
        {
            for (var r = 0; r < replicas; r++)
            {
                counts[LanePartitioner.Lane(driver + r * 100, 4, hash)]++;
            }
        }
        return counts;
    }
}
