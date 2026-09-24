# Impactos da implementação

Achados sobre como decisões de implementação e de ambiente afetam os resultados da matriz `7e283c2`. Estão separados por grau de evidência:
- **medido:** dado da matriz ou do Prometheus;
- **calculado:** derivado do código e do dataset;
- **hipótese:** ainda a testar.

Os três primeiros achados são assimetrias entre os brokers, o que é o tipo mais grave de problema num trabalho comparativo.

## 1. As faixas do RabbitMQ são desbalanceadas; as do Kafka, não (medido)

**O que acontece.** O `RabbitMqSink` escolhe a fila por `carro % 4`. O `FleetAmplifier` numera as réplicas somando múltiplos de 100 ao número original do piloto, e 100 ≡ 0 (mod 4). Com isso, a fila de cada réplica é decidida só pelo número do piloto original: 20 pilotos em 4 filas, sem garantia de equilíbrio. No Kafka, o partitioner padrão da librdkafka (`consistent_random`) aplica CRC32 à chave ([CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)), e os ~26 mil carros distintos se espalham de forma uniforme.

**Evidência.**

| | Fila/partição 0 | 1 | 2 | 3 |
| --- | --- | --- | --- | --- |
| RabbitMQ, calculado (`analysis/lane-balance.cs`) | 25,0% | 15,0% | 25,0% | 35,0% |
| RabbitMQ, medido nas rodadas (Prometheus, mensagens publicadas por fila) | 84,1 M (25,0%) | 50,3 M (14,9%) | 83,9 M (24,9%) | 117,3 M (34,9%) |
| Kafka, calculado | 24,9% | 25,0% | 25,0% | 25,0% |

O máximo de mensagens prontas acompanhou a distribuição: 69.948 na fila 3, 24,7 mil e 25,5 mil nas filas 0 e 2, 14,7 mil na fila 1.

**Por que importa.** No RabbitMQ, cada fila é uma unidade de concorrência e roda num único processo. A documentação do plugin de sharding diz que as filas "are units of concurrency (and, if there are enough cores available, parallelism)" ([rabbitmq_sharding](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_sharding)). A fila mais carregada recebe 1,4× a parte justa e satura antes das outras. Com isso:
- o teto de ~60 mil ev/s pode ser o teto da fila 3, que recebe ~21 mil ev/s nessa carga, e não o do broker;
- pela fórmula de Kingman, a cauda é dominada pela fila de maior utilização.

A comparação equalizou o paralelismo (P = 4 nos dois) mas não a **distribuição** dentro dele, e isso favorece o Kafka.

**Correção proposta.** Usar no sink do RabbitMQ o mesmo CRC32 da chave que a librdkafka usa, para que os dois brokers recebam exatamente a mesma partição dos carros. A alternativa nativa do RabbitMQ é o [consistent hash exchange](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_consistent_hash_exchange), feito para distribuir por chave mantendo a ordem por chave. O CRC32 no sink é preferível aqui porque dá a mesma atribuição de carro para faixa nos dois brokers.

**Custo.** Refazer as células do RabbitMQ (60 rodadas, ~2,5 h) e a varredura de saturação. **Hipótese a confirmar:** com filas equilibradas, o teto do RabbitMQ sobe até ~1,4×, para perto de 84 mil ev/s, se o limite for mesmo por fila.

## 2. O broker RabbitMQ é estrangulado pela cota de CPU; o Kafka, não (medido)

**O que acontece.** Os containers têm CPU limitada por `--cpus`, que o Linux implementa como cota CFS: um tempo de CPU por período de 100 ms. Um processo com várias threads pode gastar a cota inteira no começo do período e ficar congelado até o período seguinte ([Luu, The container throttling problem](https://danluu.com/cgroup-throttling/)).

A Erlang VM do RabbitMQ respeita a cota ao escolher os escalonadores: conferido no container, são **4 online de 12**, como a documentação prevê desde o Erlang 23 ([Runtime Tuning](https://www.rabbitmq.com/docs/runtime)). Mesmo assim, o runtime tem outras threads além dos escalonadores normais, e o broker chega perto do limite.

**Evidência** (`analysis/throttling.ps1`: fração dos períodos CFS estrangulados em cada rodada; média por célula):

| Carga | Broker RabbitMQ | P99 RabbitMQ | Broker Kafka | P99 Kafka |
| --- | --- | --- | --- | --- |
| 10 mil | 4,0% | 1,4 ms | 0% | 5,8 ms |
| 20 mil | 5,1% | 2,3 ms | 0% | 5,7 ms |
| 40 mil | 6,5% | 9,4 ms | 0% | 5,7 ms |
| 60 mil | 8,4% | 22,8 ms | 0% | 5,7 ms |
| 200 mil | — | — | 0,01% | 5,6 ms |

O consumidor e o produtor quase não foram estrangulados até 200 mil ev/s. A 400 mil, o consumidor do Kafka foi estrangulado em 4,3% dos períodos, junto com o salto do P99 para 172 ms.

**Por que importa.** Um congelamento dura até o fim do período de 100 ms: dezenas de milissegundos, a ordem de grandeza do P99 do RabbitMQ a 60 mil ev/s. O estrangulamento e a cauda crescem juntos. Parte da cauda atribuída ao RabbitMQ pode ser efeito da forma de limitar CPU, que afeta um runtime com muitas threads (a Erlang VM) mais do que a JVM do Kafka nesta carga.

**Isso é correlação.** A carga move as duas grandezas, e o desbalanceamento do §1 também. O teste que decide é um A/B: RabbitMQ a 40 e 60 mil ev/s com `--cpuset-cpus` (núcleos fixos, sem cota) contra `--cpus`, 5 repetições cada.
- Se o P99 cair com `cpuset`, a cota é parte do resultado.
- Nesse caso, o protocolo precisa mudar para os dois brokers, porque a regra tem de ser a mesma, e o Kafka também precisa ser refeito.

## 3. A biblioteca cliente aloca ~20× o tamanho do evento (medido)

**Evidência** (colunas `allocated_mb` e `events` da matriz):

| | Bytes alocados por evento | Coleções gen0 a 60 mil ev/s |
| --- | --- | --- |
| Kafka (três modos) | 918 a 928 | 588 por rodada |
| RabbitMQ (três modos) | 699 a 717 | 447 por rodada |

O evento tem 46 bytes. A alocação é **idêntica nos três modos**, então ela acontece antes do mecanismo, na biblioteca cliente. No Confluent.Kafka, cada `Consume` entrega um `ConsumeResult`, uma `Message` e o `byte[]` do valor. No RabbitMQ.Client, cada entrega também gera seus objetos.

**Por que importa.**
- **Para o Pipelines:** a promessa de menos cópias e menos alocação ([Fowler, 2018](https://devblogs.microsoft.com/dotnet/system-io-pipelines-high-performance-io-in-net/)) não tem onde agir. Os ~900 bytes por evento já foram alocados quando o dado chega ao pipe. É a comprovação medida de que o Pipelines não chega ao socket nesta arquitetura (APROFUNDAMENTO §2).
- **Para o texto:** a diferença de alocação entre os brokers é da biblioteca cliente, não do broker.

**Melhoria possível.** O Confluent.Kafka aceita um `IDeserializer<T>` que recebe `ReadOnlySpan<byte>`, o que evitaria o `byte[]` do valor. É uma otimização do cliente e não muda a comparação entre mecanismos, por isso fica como nota.

## 4. O Kafka guarda um terceiro buffer no cliente (calculado)

Por padrão, a librdkafka tenta manter `queued.min.messages` = 100.000 mensagens por partição buscadas antecipadamente, até `queued.max.messages.kbytes` = 64 MB ([CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)). Com o consumidor lento, o acúmulo pode não aparecer como lag no broker: ele já está na memória do processo consumidor, antes do canal e do pipe.

**Por que importa para a Q4 do APROFUNDAMENTO.** No Kafka, o acúmulo tem três lugares possíveis: o log (lag), a fila local da librdkafka e o canal ou pipe. No RabbitMQ são dois: a fila, com o prefetch limitando as mensagens não confirmadas, e o canal ou pipe. Os experimentos de contrapressão precisam fixar e registrar `queued.max.messages.kbytes`, senão o buffer do cliente absorve o efeito que se quer medir. É a mesma troca que o RabbitMQ descreve para o prefetch: um buffer grande no cliente mantém o consumidor ocupado, mas aumenta a latência ([RabbitMQ, Some queuing theory](https://www.rabbitmq.com/blog/2012/05/11/some-queuing-theory-throughput-latency-and-bandwidth)).

## 5. Contraste de padrões: espera ativa (hipótese)

- **RabbitMQ:** desliga a espera ativa dos escalonadores por padrão (`+sbwt none`), porque ela "reduces CPU usage on systems with limited or burstable CPU resources". A documentação avisa que a espera ativa aparece como uso de CPU nas ferramentas comuns ([Runtime Tuning](https://www.rabbitmq.com/docs/runtime)).
- **ThreadPool do .NET:** faz espera ativa por padrão (APROFUNDAMENTO §2.1).

Num ambiente com cota de CPU, a espera ativa do consumidor .NET gasta cota que o sistema poderia usar. Se a H1e do APROFUNDAMENTO se confirmar, parte da CPU dos consumidores na matriz é espera ativa, e comparar CPU entre broker Erlang e consumidor .NET exige essa ressalva.

## 6. Prioridades

| # | Ação | Por quê | Custo |
| --- | --- | --- | --- |
| 1 | Balancear as faixas do RabbitMQ com CRC32 e refazer as células do RabbitMQ e a varredura | Assimetria que favorece o Kafka no teto e na cauda | ~3 h de máquina |
| 2 | A/B de `cpuset` contra cota no RabbitMQ a 40 e 60 mil | Decide se a cauda do RabbitMQ é, em parte, efeito da cota | ~1 h; se confirmar, mudar o protocolo e refazer tudo |
| 3 | Fixar e registrar `queued.max.messages.kbytes` nos experimentos de contrapressão | Senão o buffer do cliente esconde o efeito | configuração |
| 4 | Registrar as variáveis `DOTNET_*` e a H1e | Sem isso, a CPU do consumidor mistura trabalho e espera ativa | já previsto no APROFUNDAMENTO |
| 5 | Deserializer por `ReadOnlySpan` | Otimização do cliente, não da comparação | opcional |

Os itens 1 e 2 afetam a conclusão principal da matriz e devem vir **antes** do aprofundamento: os dois podem mudar o teto e a cauda do RabbitMQ. Faz sentido rodá-los juntos: um A/B com faixas balanceadas, com e sem `cpuset`.

## Fontes

- librdkafka. [CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) e [INTRODUCTION.md](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md).
- RabbitMQ. [Consistent hash exchange](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_consistent_hash_exchange), [Sharding plugin](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_sharding), [Runtime Tuning](https://www.rabbitmq.com/docs/runtime), [Some queuing theory: throughput, latency and bandwidth](https://www.rabbitmq.com/blog/2012/05/11/some-queuing-theory-throughput-latency-and-bandwidth) (2012).
- CloudAMQP. [RabbitMQ Best Practice for High Performance](https://www.cloudamqp.com/blog/part2-rabbitmq-best-practice-for-high-performance.html): uma fila limitada a um núcleo; distribuir entre filas.
- Luu, D. [The container throttling problem](https://danluu.com/cgroup-throttling/).
- Netdata. [Docker CPU throttling: the hidden cause of container latency](https://www.netdata.cloud/guides/docker/docker-cpu-throttling/).
- Microsoft. [Preparing for the .NET 10 GC (DATAS)](https://devblogs.microsoft.com/dotnet/preparing-for-dotnet-10-gc/).
- Fowler, D. [System.IO.Pipelines: High performance IO in .NET](https://devblogs.microsoft.com/dotnet/system-io-pipelines-high-performance-io-in-net/), 2018.
