# Impactos da implementação

Achados sobre como decisões de implementação e de ambiente afetam os resultados da matriz `7e283c2`. Estão separados por grau de evidência:
- **medido:** dado da matriz ou do Prometheus;
- **calculado:** derivado do código e do dataset;
- **hipótese:** ainda a testar.

Os três primeiros achados são assimetrias entre os brokers, o que é o tipo mais grave de problema num trabalho comparativo.

> **Atualização (experimento 2×2, §7):** a cota de CPU se confirmou como fator de peso: com núcleos fixos, o P99 do RabbitMQ caiu de 40% a 64%. Já a hipótese do §1 foi **refutada** a 40 e 60 mil ev/s: equilibrar as filas aumentou a CPU do broker e o P99, em vez de reduzir. O desbalanceamento não prejudicava o RabbitMQ nessas cargas; ajudava. O efeito sobre o teto de vazão ainda não foi medido.

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

## 7. Resultado do experimento 2×2 (medido)

`experiments/asymmetry-ab.ps1`, commit `247fd7c`, análise em `analysis/asymmetry-effects.ps1`. RabbitMQ em modo direct, 40 e 60 mil ev/s, 5 repetições por célula, ordem aleatória, persistência ligada.
- **Fatores:** faixas (módulo, como na matriz, × CRC32, igual ao Kafka) e CPU do broker (cota `--cpus 4`, como na matriz, × núcleos fixos 4-7 sem cota).
- **Rodadas:** 40 executadas, 35 válidas; as 5 inválidas foram por jitter do produtor, 4 delas a 60 mil ev/s.
- **Corretude:** digest idêntico nas 20 rodadas de cada carga. Mudar a fila de cada carro não alterou o resultado do processamento.
- **Painel:** o painel local ficou ligado a partir da 3ª rodada, preso nos núcleos 0-1 (`results/asym-ab-dash.txt`). Como a ordem é aleatória, as quatro células ficaram igualmente expostas.

**Medianas das rodadas válidas:**

| Carga | Faixas | CPU do broker | P99 | P50 | Estrangulado | CPU do broker |
| --- | --- | --- | --- | --- | --- | --- |
| 40 mil | módulo | cota | 10,2 ms | 0,62 ms | 6,8% | 259% |
| 40 mil | módulo | núcleos fixos | **6,1 ms** | 0,72 ms | 0% | 245% |
| 40 mil | crc32 | cota | 30,5 ms | 0,86 ms | 99,7% | 392% |
| 40 mil | crc32 | núcleos fixos | 11,0 ms | 1,37 ms | 0% | 306% |
| 60 mil | módulo | cota | 24,4 ms | 1,00 ms | 8,7% | 302% |
| 60 mil | módulo | núcleos fixos | **14,1 ms** | 1,24 ms | 0% | 271% |
| 60 mil | crc32 | cota | 36,0 ms | 1,81 ms | 98,0% | 389% |
| 60 mil | crc32 | núcleos fixos | 30,0 ms | 3,41 ms | 0% | 322% |

**Reprodução da matriz.** A célula módulo + cota é a configuração da matriz `7e283c2`, e deu 10,2 e 24,4 ms, contra 9,4 e 22,8 ms na matriz. A diferença, de 7% a 9%, fica dentro da variação entre dias e confirma que o experimento mede o mesmo sistema.

**Quanto cada fator explica da variação do P99** (fatorial 2² com repetição; Jain, 1991, cap. 18):

| Carga | Faixas | CPU | Interação | Erro |
| --- | --- | --- | --- | --- |
| 40 mil | 43,9% | 39,6% | 14,4% | 2,0% |
| 60 mil | 49,8% | 19,9% | 0,7% | 29,5% |

**1. A cota de CPU é um fator de peso: a regra de decisão foi atingida.** Com núcleos fixos, o P99 caiu 40% (40 mil, módulo), 64% (40 mil, crc32) e 42% (60 mil, módulo). Só na célula 60 mil com crc32 a queda ficou abaixo do limiar (−17%). Pela regra fixada antes de medir, o protocolo passa a usar núcleos fixos nos brokers, e o Kafka precisa ser conferido. Parte relevante da cauda atribuída ao RabbitMQ na matriz era efeito da cota.

**2. Equilibrar as filas piorou o RabbitMQ nessas cargas, ao contrário da hipótese do §1.** Com CRC32, o broker gastou de 50 a 130 pontos percentuais a mais de CPU e o P99 subiu, com cota e com núcleos fixos. Sob cota, a combinação é a pior: o broker passa a pedir quase 4 núcleos e fica estrangulado em ~99% dos períodos. A interação responde por 14% da variação a 40 mil.

Não há ainda explicação medida para o gasto maior com filas equilibradas. A hipótese mais coerente com os dados é a **amortização por lote**, a mesma ideia da Q1 do APROFUNDAMENTO, agora dentro do broker:
- **Com módulo:** a fila de 35% trabalha com acúmulo e atende em lotes maiores, e as de 15% e 25% têm folga. O custo por mensagem cai.
- **Com CRC32:** as quatro filas operam no mesmo ponto intermediário. Cada uma paga custo por mensagem sem acúmulo que o amortize, e todas ao mesmo tempo.

Para testar, é preciso medir a CPU por fila e o tamanho das entregas (`rabbitmq-diagnostics`) nas duas funções de faixa.

**3. O que muda na comparação.**
- **Justiça:** o CRC32 continua sendo a escolha correta, porque dá aos dois brokers a mesma divisão dos carros.
- **Direção do efeito:** a correção **não favorece** o RabbitMQ nessas cargas; ao contrário, piora sua latência. O §1 dizia que o desbalanceamento favorecia o Kafka, e isso estava errado para latência a 40 e 60 mil ev/s.
- **Teto:** a varredura de saturação com as duas funções dirá se o equilíbrio ao menos eleva o teto de vazão.

## 8. Medições de decisão da fase 2 (medido)

25/09/2026, commit `caf07be`, roteiro `experiments/decisoes-fase2.ps1`. Modo Direct, 100 Hz, 10 s de aquecimento e 90 s de medição, 3 rodadas por lado, intercaladas. 42 rodadas em 82 minutos, dados em `results/fase2-*.csv`. As regras de decisão estavam escritas em DESENVOLVIMENTO antes de medir.

**Validade:** todas as rodadas de cada carga deram o mesmo resumo do resultado, sem registro incompleto e sem janela descartada. As 6 inválidas foram por atraso de envio, todas no Kafka a 100 e 200 mil ev/s.

### 8.1 Nagle no Kafka

| Carga | Lado | P99 (ms), por rodada | Mediana | Atraso de envio (ms) |
| --- | --- | --- | --- | --- |
| 20 mil | ligado | 5,81; 5,69; 6,53 | 5,81 | 7,2; 6,1; 7,9 |
| 20 mil | desligado | 5,72; 6,03; 5,74 | 5,74 | 4,3; 4,2; 4,1 |
| 100 mil | ligado | inválida; 5,68; 5,57 | 5,63 | 571,6; 4,7; 5,0 |
| 100 mil | desligado | inválida; 5,68; 11,47 | 8,58 | 113,0; 12,0; 9,5 |

- **Latência:** sem diferença relevante (1% a 20 mil ev/s). A 100 mil, uma rodada sem Nagle chegou a 11,47 ms; com duas rodadas válidas por lado, não dá para separar esse valor do ruído. As rodadas anteriores a 24/09, com o Nagle ligado, não tiveram a latência afetada.
- **Gerador:** com o Nagle ligado, o atraso de envio foi cerca de 3 ms maior nas três rodadas a 20 mil.
- **As duas primeiras rodadas a 100 mil foram inválidas,** uma de cada lado. Na com Nagle, a fila local do produtor encheu 17 vezes: o broker parou de confirmar por perto de 1 s. É a instabilidade do Kafka a 100 e 200 mil ev/s que a fase 4 investiga, e aparece com os dois valores de Nagle.
- **Decisão:** o padrão fica com o Nagle desligado, o padrão da biblioteca, como estava previsto.

### 8.2 Mensagem persistente no RabbitMQ, a 40 mil ev/s

| Lado | P99 (ms), por rodada | Mediana P99 | Mediana P50 |
| --- | --- | --- | --- |
| persistente | 8,30; 8,50; 8,37 | 8,37 ms | 1,03 ms |
| transitória | 4,50; 4,39; 4,22 | 4,39 ms | 0,30 ms |

- **A persistência custa 48% no P99 e 70% no P50.** É o custo da gravação em disco antes de confirmar, a assimetria A da AUDITORIA-CONFIG, agora medido: o Kafka confirma sem forçar essa gravação.
- **Decisão:** pela regra, o padrão fica persistente, por equivalência de garantia com o `acks=all` do Kafka, e o perfil ajustado usa mensagem transitória (a redução passou dos 10%).

### 8.3 `prefetch` no RabbitMQ

| Carga | Lado | P99 (ms), por rodada | Mediana | Memória do consumidor |
| --- | --- | --- | --- | --- |
| 40 mil | 300 | 8,86; 8,49; 9,75 | 8,86 | 91 a 92 MB |
| 40 mil | sem limite | 9,48; 9,63; 9,46 | 9,48 | 91 a 92 MB |
| 60 mil | 300 | 32,99; 25,30; 21,54 | 25,30 | 91 a 96 MB |
| 60 mil | sem limite | 26,06; 24,93; 30,82 | 26,06 | 91 a 92 MB |

- Diferença de 7% e 3% no P99, abaixo do tamanho relevante de 20%, sem rodada inválida e sem diferença de memória.
- **Decisão:** o padrão fica sem limite, o valor de fábrica do RabbitMQ. O 300 vai para o perfil ajustado como recomendação do fornecedor.

### 8.4 Quanto o gerador segura antes de travar

**RabbitMQ, a 60 mil ev/s:**

| Lado | P99 (ms), por rodada | Mediana | Atraso de envio (ms) |
| --- | --- | --- | --- |
| 8 mil em voo | 30,10; 24,62; 24,34 | 24,62 | 43,1; 41,6; 38,7 |
| 100 mil em voo | 40,86; 32,75; 18,08 | 32,75 | 18,9; 12,3; 12,5 |

- Com 8 mil em voo, o atraso de envio ficou perto do limite de 50 ms nas três rodadas; com 100 mil, caiu para 12 a 19 ms.
- O P99 mediano foi maior com 100 mil, mas as faixas se sobrepõem. Parte da diferença é artefato: com a janela pequena, o gerador fica parado esperando confirmações, e esse tempo não entra na latência medida, porque o horário de envio é marcado quando o evento sai do gerador (omissão coordenada, AMEACAS-VALIDADE).
- **Decisão:** o padrão fica com 100 mil em voo, como previsto. Nenhuma rodada foi invalidada, e a margem de validade melhorou.

**Kafka, a 200 mil ev/s:**

| Lado | P99 (ms), por rodada | Atraso de envio (ms) | Vezes que a fila encheu | Válidas |
| --- | --- | --- | --- | --- |
| fila de 1 milhão | 889,86; 16,21; 42,59 | 2.773; 22,0; 22,9 | 0; 0; 0 | 2 de 3 |
| fila de 100 mil (padrão) | 6,47; 119,62; 359,17 | 1.139; 1.265; 1.437 | 0; 0; 6 | 0 de 3 |

- A regra previa ficar com 100 mil, a menos que o valor invalidasse rodadas abaixo da saturação. O resultado não permite aplicá-la de forma limpa:
  - 200 mil ev/s é justamente a carga em que o Kafka satura; não se sabe se está abaixo da saturação no perfil padrão;
  - a fila cheia só explica uma das três rodadas inválidas com 100 mil. Nas outras duas, a fila nunca encheu e o atraso passou de 1 s, como na rodada de 1 milhão que chegou a 2,8 s.
- Os atrasos de envio de mais de 1 s a 200 mil, e o da primeira rodada a 100 mil, são a mesma instabilidade que a fase 4 investiga.
- **Decisão provisória:** o padrão fica com 100 mil, o valor de fábrica e o mesmo total em voo do RabbitMQ. A fase 4 repete essa comparação com mais rodadas, ao investigar os atrasos. A fila de 1 milhão vai para os candidatos do perfil ajustado.

## Fontes

- librdkafka. [CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) e [INTRODUCTION.md](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md).
- RabbitMQ. [Consistent hash exchange](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_consistent_hash_exchange), [Sharding plugin](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_sharding), [Runtime Tuning](https://www.rabbitmq.com/docs/runtime), [Some queuing theory: throughput, latency and bandwidth](https://www.rabbitmq.com/blog/2012/05/11/some-queuing-theory-throughput-latency-and-bandwidth) (2012).
- CloudAMQP. [RabbitMQ Best Practice for High Performance](https://www.cloudamqp.com/blog/part2-rabbitmq-best-practice-for-high-performance.html): uma fila limitada a um núcleo; distribuir entre filas.
- Luu, D. [The container throttling problem](https://danluu.com/cgroup-throttling/).
- Netdata. [Docker CPU throttling: the hidden cause of container latency](https://www.netdata.cloud/guides/docker/docker-cpu-throttling/).
- Microsoft. [Preparing for the .NET 10 GC (DATAS)](https://devblogs.microsoft.com/dotnet/preparing-for-dotnet-10-gc/).
- Fowler, D. [System.IO.Pipelines: High performance IO in .NET](https://devblogs.microsoft.com/dotnet/system-io-pipelines-high-performance-io-in-net/), 2018.
