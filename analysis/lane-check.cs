// Confere, contra a librdkafka real, que LanePartitioner.Lane(..., Crc32)
// escolhe para cada chave a mesma particao que o partitioner padrao do Kafka.
// Se conferir, as filas do RabbitMQ e as particoes do Kafka recebem
// exatamente os mesmos carros. Ver docs/IMPLEMENTACAO.md, secao 1.
//
//   docker exec pitwall-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
//     --create --topic lane-check --partitions 4 --replication-factor 1
//   dotnet run lane-check.cs -- localhost:9092 lane-check

#:package Confluent.Kafka@2.15.1
#:project ../src/Pitwall.Contracts/Pitwall.Contracts.csproj

using Confluent.Kafka;
using Pitwall.Contracts;

var bootstrap = args.Length > 0 ? args[0] : "localhost:9092";
var topic = args.Length > 1 ? args[1] : "lane-check";
const int Partitions = 4;

using var producer = new ProducerBuilder<int, byte[]>(new ProducerConfig { BootstrapServers = bootstrap }).Build();

// Os mesmos numeros de carro da frota multiplicada: piloto + replica * 100.
int[] drivers = [1, 2, 3, 4, 10, 11, 14, 16, 18, 20, 22, 23, 24, 27, 31, 44, 55, 63, 77, 81];
var mismatches = 0;
var checkedKeys = 0;

foreach (var driver in drivers)
{
    for (var replica = 0; replica < 250; replica++)
    {
        var key = driver + replica * 100;
        var result = await producer.ProduceAsync(topic, new Message<int, byte[]> { Key = key, Value = [] });
        var expected = LanePartitioner.Lane(key, Partitions, LaneHash.Crc32);

        checkedKeys++;
        if (result.Partition.Value != expected)
        {
            mismatches++;
            if (mismatches <= 5)
            {
                Console.WriteLine($"divergencia: chave {key} -> Kafka {result.Partition.Value}, LanePartitioner {expected}");
            }
        }
    }
}

Console.WriteLine($"{checkedKeys} chaves conferidas, {mismatches} divergencias.");
return mismatches == 0 ? 0 : 1;
