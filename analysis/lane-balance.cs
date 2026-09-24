// Parte da carga que cai em cada faixa: RabbitMQ (carro % P, RabbitMqSink.Lane)
// contra Kafka (partitioner padrao da librdkafka, consistent_random = CRC32 da
// chave; Serializers.Int32 grava a chave em big-endian). Ver docs/IMPLEMENTACAO.md.
//   dotnet run lane-balance.cs -- ../data/raw/9472/car_data.jsonl

#:package System.IO.Hashing@9.0.0
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text.Json;

var path = args[0];
var perDriver = new Dictionary<int, long>();
foreach (var line in File.ReadLines(path))
{
    using var d = JsonDocument.Parse(line);
    var n = d.RootElement.GetProperty("driver_number").GetInt32();
    perDriver[n] = perDriver.GetValueOrDefault(n) + 1;
}
var total = perDriver.Values.Sum();
const int P = 4;

foreach (var rate in new[] { 10_000, 60_000, 200_000 })
{
    var fleet = (int)Math.Ceiling(rate / (total / (99 * 60.0)));   // mesma ideia do --fleet auto
    var rabbit = new double[P];
    var kafka = new double[P];
    Span<byte> key = stackalloc byte[4];
    foreach (var (driver, count) in perDriver)
    {
        for (var r = 0; r < fleet; r++)
        {
            var n = driver + r * 100;
            rabbit[(n & int.MaxValue) % P] += count;
            BinaryPrimitives.WriteInt32BigEndian(key, n);
            kafka[Crc32.HashToUInt32(key) % P] += count;
        }
    }
    var all = rabbit.Sum();
    Console.WriteLine($"{rate,7} ev/s (frota x{fleet}): " +
        $"RabbitMQ [{string.Join(" ", rabbit.Select(v => $"{100 * v / all,5:0.0}%"))}]  " +
        $"Kafka [{string.Join(" ", kafka.Select(v => $"{100 * v / all,5:0.0}%"))}]");
}
