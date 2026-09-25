# Auditoria das configurações

24/09/2026. Pedida pelo orientador: primeiro um cenário com a configuração padrão dos brokers, depois um ajustado, com limites de armazenamento equivalentes (PLANO §1g).

**Resultado em uma linha:** dois defeitos (e um terceiro, achado na fase 4), três assimetrias de equivalência e seis valores fora do padrão sem motivo forte. Nenhum dos defeitos invalida o que já foi medido, mas os dois precisam ser corrigidos antes da matriz.

Os valores de fábrica foram lidos na fonte, não de memória:
- librdkafka 2.15.1: `CONFIGURATION.md` do pacote `librdkafka.redist` usado pelo projeto;
- Kafka 4.1.0: `kafka-configs --describe --all` no broker em execução, que mostra o valor efetivo e o padrão;
- RabbitMQ 4.3.6: um container descartável da mesma imagem, sem o nosso `rabbitmq.conf`, consultado com `rabbitmqctl eval`;
- RabbitMQ.Client 7.2.2: `ConnectionFactory.DefaultSocketFactory` inspecionado em execução.

## Situação depois da fase 1 (24/09/2026)

Os dois defeitos e os valores sem justificativa foram corrigidos. A fase 2 (IMPLEMENTACAO §8) decidiu o `prefetch` e a janela do RabbitMQ, mediu o custo da mensagem persistente (48% no P99 a 40 mil ev/s) e deixou a fila do produtor Kafka provisória até a fase 4.

| Item | Antes | Agora | Conferido |
| --- | --- | --- | --- |
| Defeito 1: Nagle no Kafka | forçado ligado | padrão da librdkafka (desligado); `--nagle on\|off` para medir | configuração gravada na rodada |
| Defeito 2: memória do RabbitMQ | freio em 4.706 MiB, sobre a VM | `total_memory_available_override_value`; freio em 1.843 MiB de 3.072 | log do broker |
| Versão do RabbitMQ | tag flutuante `4-management-alpine` | `4.3.6-management-alpine` | `docker inspect` |
| `batch.size`, `fetch.wait.max.ms` | 65.536 e 10 | padrão (1.000.000 e 500) | configuração gravada |
| `compression.type` do broker Kafka | `uncompressed` | padrão, `producer` | `kafka-configs` |
| `disk_free_limit` | 2 GB | padrão, 50 MB | log do broker |
| Heap do Kafka | 1,5 GB | padrão, 1 GB | processo Java |
| B: fila do produtor Kafka | 1.000.000 | padrão, 100.000; decidido na fase 4 (IMPLEMENTACAO §9.1) | configuração gravada |
| Defeito 3: diretório do log do Kafka | camada do container | volume `kafka-data` (`KAFKA_LOG_DIRS`) | `log.dirs` no log do broker |
| Exclusão dos arquivos do tópico | 60 s depois, no meio da rodada seguinte | antes da rodada seguinte (protocolo) | `Reset-KafkaTopic` |
| B: janela do produtor RabbitMQ | 8.000 no total | 100.000 no total (12.500 × 2 × 4); decidido na fase 2 | configuração gravada |
| C: `prefetch` | 300 | 0, sem limite; decidido na fase 2 | configuração gravada |

**Perfis no roteiro:** `-Profile padrao` (o de cima) e `-Profile legado` (os clientes como até 24/09, para comparar). O lado dos brokers não muda com o perfil: heap, compressão e memória estão no compose.

**Fumaça:** 4 rodadas curtas a 100 Hz, uma em cada caminho alterado (RabbitMQ com Channels e com Pipelines no `padrao`, Kafka com Channels no `padrao`, Kafka com Direct no `legado`). As 4 foram válidas, com atraso de envio entre 5 e 11 ms e o mesmo resumo do resultado entre brokers e modos na mesma carga. Arquivos em `results/fase1-fumaca-*.csv`.

## Defeitos

### 1. O Kafka rodou com Nagle ligado; o padrão é desligado

`KafkaSink` e `KafkaEventSource` atribuem `SocketNagleDisable = options.SocketNagleDisable`, que vale `false` quando `--nagle-disable` não é passado. O roteiro nunca passa essa opção. O padrão da librdkafka 2.15.1 é `socket.nagle.disable=true`, e o do RabbitMQ.Client é `NoDelay=true`: os dois desligam o Nagle. Só o Kafka rodou com ele ligado, e por escolha nossa.

- **Efeito já medido:** o A/B de 10 rodadas por lado da REVISAO-TECNICA §1.1 não mostrou diferença na frequência do regime lento. Não mediu o efeito na latência típica.
- **Correção:** não atribuir a propriedade, deixando o padrão da biblioteca. A premissa errada da REVISAO-TECNICA foi corrigida.

### 2. O freio de memória do RabbitMQ nunca poderia agir

O `rabbitmq.conf` diz que `vm_memory_high_watermark.relative = 0.6` é "0.6 do mem_limit do container". Não é: o RabbitMQ calcula sobre a memória da máquina virtual do WSL2. O log mostra `Memory high watermark set to 4706 MiB ... of 7844 MiB total`, e o container tem limite de 3.072 MiB. Com fila acumulando, o kernel mataria o RabbitMQ antes de o freio de memória, que segura os produtores, entrar em ação.

- **Efeito até agora:** nenhum. O container nunca foi morto por memória (`OOMKilled=false`, 0 reinícios) e o log não tem alarme de memória.
- **Correção:** `total_memory_available_override_value = 3GB` no `rabbitmq.conf`, para que os 60% sejam calculados sobre os 3 GB do container (cerca de 1,8 GB).

### 3. O log do Kafka ficava na camada do container, não no volume

Achado na fase 4 (IMPLEMENTACAO §9.3). A imagem `apache/kafka` grava em `/tmp/kraft-broker-logs` quando `log.dirs` não é definido, e o compose não o definia. O log ficava no sistema de arquivos em camadas do container, e o volume `kafka-data` ficava vazio; o RabbitMQ sempre gravou no volume `rabbit-data`.

- **Correção:** `KAFKA_LOG_DIRS=/var/lib/kafka/data` no compose. Conferido: o broker lista `log.dirs = /var/lib/kafka/data`, e os diretórios das partições aparecem no volume.
- **Efeito:** as medições do Kafka até a primeira bateria da fase 4 foram feitas com o log na camada do container.

**Protocolo de exclusão do tópico.** O roteiro exclui o tópico a cada rodada, e o Kafka apaga os arquivos 60 s depois (`log.segment.delete.delay.ms`), no meio da rodada seguinte. Desde a fase 4, `Use-Broker` muda esse atraso para 0 no broker em execução, e `Reset-KafkaTopic` espera os arquivos sumirem antes de começar. É isolamento entre rodadas, não ajuste de desempenho: afeta só a limpeza dos dados da rodada anterior. Não reduziu os episódios na medição (IMPLEMENTACAO §9.3).

## Assimetrias de equivalência

São os "limites de armazenamento" de que o orientador falou. Em cada caso, um broker pode ganhar ou perder por configuração, e não por arquitetura.

### A. Garantia de gravação em disco

- **RabbitMQ:** com mensagem persistente e confirmação ao produtor, o broker só confirma depois de gravar em disco (fsync).
- **Kafka:** com `acks=all` e um único nó, o broker confirma quando a mensagem chega ao cache de disco do sistema operacional. O padrão `log.flush.interval.messages` é o máximo de um inteiro de 64 bits: o Kafka nunca força o fsync por mensagem e conta com a replicação para a durabilidade.

Com um nó só, o RabbitMQ paga um custo de durabilidade que o Kafka não paga. O piloto mediu 2,8% de diferença entre persistente e transitório no RabbitMQ, mas com o protocolo antigo.

**Proposta:** manter as duas como estão, porque é o padrão de cada um para essa garantia, e declarar no texto. Medir de novo persistente contra transitório no protocolo atual, a 40 mil ev/s. Se a diferença for grande, o cenário ajustado usa mensagens transitórias no RabbitMQ.

### B. Quanto o produtor segura antes de travar

| | Limite | Quem define |
| --- | --- | --- |
| Kafka | 1.000.000 mensagens na fila local do cliente | nós; o padrão é 100.000 |
| RabbitMQ | 8.000 mensagens sem confirmação: 2 lotes de 1.000 × 4 faixas | nós; não há padrão |

Na saturação, os dois falham de jeitos diferentes:
- **Kafka:** acumula até 1 milhão de mensagens na fila local. A latência sobe, mas a rodada continua válida.
- **RabbitMQ:** trava o gerador depois de 8 mil. O atraso de envio passa de 50 ms e a rodada é descartada.

O tempo de cada evento é marcado quando ele é entregue ao cliente (`PublishedTicks`), e não no instante programado. Por isso o atraso de quem trava só aparece pelo critério de descarte.

**Proposta:** no cenário padrão, a fila do Kafka volta ao padrão de 100.000 mensagens, e a janela do RabbitMQ passa a segurar o mesmo total: 12.500 por lote × 2 lotes × 4 faixas. Medir o efeito na saturação antes de fixar.

### C. Quanto o consumidor puxa antecipadamente

| | Limite | Padrão |
| --- | --- | --- |
| Kafka | até 100.000 mensagens ou 64 MB por partição (`queued.min.messages`, `queued.max.messages.kbytes`) | é o padrão |
| RabbitMQ | 300 mensagens por canal (`prefetch`) | sem limite |

O Kafka traz blocos grandes para a memória do consumidor. O RabbitMQ, com 300, entrega aos poucos. O valor de 300 segue a faixa que a documentação do RabbitMQ recomenda (100 a 300), mas não é o padrão.

**Proposta:** cenário padrão com `prefetch` sem limite, o padrão do RabbitMQ e o mais próximo do comportamento padrão do Kafka. O valor de 300 vai para o cenário ajustado, como recomendação do fornecedor. Medir antes de fixar.

## Tabela completa

Situação: **padrão** = igual ao de fábrica; **justificado** = diferente por um motivo que vale para os dois brokers; **rever** = diferente sem motivo forte; **defeito** = ver acima.

### Kafka, produtor (librdkafka 2.15.1)

| Parâmetro | Padrão | Usado | Situação |
| --- | --- | --- | --- |
| `acks` | -1 (todas) | all | padrão |
| `linger.ms` | 5 | 5 | padrão |
| `batch.size` | 1.000.000 | 65.536 | rever: não limita até 300 mil ev/s (lote de ~20 KB por partição em 5 ms), mas foge do padrão |
| `batch.num.messages` | 10.000 | padrão | padrão |
| `compression.type` | none | none | padrão |
| `enable.idempotence` | false | false | padrão |
| `partitioner` | consistent_random (CRC32) | padrão | padrão |
| `queue.buffering.max.messages` | 100.000 | 1.000.000 | assimetria B |
| `queue.buffering.max.kbytes` | 1.048.576 | 1.048.576 | padrão |
| `socket.nagle.disable` | true | false | defeito 1 |

### Kafka, consumidor

| Parâmetro | Padrão | Usado | Situação |
| --- | --- | --- | --- |
| `enable.auto.commit` | true | true | padrão |
| `enable.auto.offset.store` | true | false | justificado: guarda o offset só depois de entregar ao processamento, o padrão documentado para at-least-once |
| `auto.commit.interval.ms` | 5.000 | 5.000 | padrão |
| `fetch.min.bytes` | 1 | 1 | padrão |
| `fetch.wait.max.ms` | 500 | 10 | rever: com `fetch.min.bytes=1` só pesa com o tópico ocioso, mas foge do padrão |
| `queued.min.messages` | 100.000 | padrão | assimetria C |
| `socket.nagle.disable` | true | false | defeito 1 |
| Consumidores | — | 1 por partição, 4 no total | justificado: igual às 4 faixas do RabbitMQ |

### Kafka, broker (4.1.0)

| Parâmetro | Padrão | Usado | Situação |
| --- | --- | --- | --- |
| Nós e replicação | — | 1 nó, fator 1 | justificado: máquina única (ameaça à validade declarada) |
| `log.flush.interval.messages` | 2^63−1 | padrão | assimetria A |
| `num.io.threads` / `num.network.threads` | 8 / 3 | padrão | padrão |
| `log.segment.bytes` | 1 GiB | padrão | padrão |
| `log.retention` | 168 h | 15 min | justificado: só espaço em disco; o tópico é recriado a cada rodada |
| `compression.type` | producer | uncompressed | rever: equivale ao padrão, porque o produtor não comprime; voltar ao padrão tira uma diferença da tabela |
| `auto.create.topics.enable` | true | false | justificado: garante as 4 partições declaradas |
| Heap da JVM | 1 GB (script de início) | 1,5 GB | rever: decidir junto com a memória do RabbitMQ (defeito 2) |

### RabbitMQ, produtor (RabbitMQ.Client 7.2.2)

| Parâmetro | Padrão | Usado | Situação |
| --- | --- | --- | --- |
| Confirmação ao produtor | desligada | ligada | justificado: é o equivalente do `acks=all` |
| Mensagem persistente | não | sim | justificado, com a assimetria A |
| Janela de confirmação | — | 1.000 por lote, 2 lotes por faixa | assimetria B |
| Conexões do produtor | 1 | 4, uma por faixa | justificado: recomendação do RabbitMQ para publicação em alta vazão; com canal único o piloto mediu o teto do gerador, não do broker (PILOTO). O Kafka usa uma conexão por broker, com vários pedidos em voo nela |
| TCP `NoDelay` | true | padrão | padrão |
| Faixa de cada carro | — | CRC32, igual ao Kafka | justificado: decidido com o orientador |

### RabbitMQ, consumidor

| Parâmetro | Padrão | Usado | Situação |
| --- | --- | --- | --- |
| `prefetch` | sem limite | 300 | assimetria C |
| Confirmação | por mensagem | em lote de 100 (`multiple=true`) | justificado: recomendação da documentação; equivale ao commit periódico do Kafka |
| `ConsumerDispatchConcurrency` | 1 | 1 | padrão |

### RabbitMQ, servidor (4.3.6)

| Parâmetro | Padrão | Usado | Situação |
| --- | --- | --- | --- |
| `vm_memory_high_watermark` | 0,6 | 0,6 | padrão, mas calculado sobre a memória errada: defeito 2 |
| `disk_free_limit` | 50 MB | 2 GB | rever: sem efeito no desempenho, é só segurança; voltar ao padrão |
| Tipo de fila | clássica, versão 2 | clássica, durável | padrão |
| `credit_flow_default_credit` | {400, 200} | padrão | padrão |
| Versão da imagem | — | `rabbitmq:4-management-alpine` | rever: tag flutuante; fixar em `4.3.6-management-alpine`, como o Kafka já está em 4.1.0 |

### Containers e .NET

| Item | Valor | Situação |
| --- | --- | --- |
| Memória dos brokers | 3 GB cada | justificado: igual nos dois |
| Núcleos | exclusivos: broker nos núcleos 4 a 7 | justificado: PLANO §1a |
| Capacidade do Channels | 10.000 eventos por faixa | justificado: igual à pausa do Pipelines (10.000 × 46 bytes) |
| Pausa do Pipelines | 460.000 bytes por faixa, retomada na metade | justificado: idem |

## Cenários propostos

**Padrão (linha de base).**
- Valores de fábrica em tudo o que está marcado como padrão ou rever.
- Os dois defeitos corrigidos.
- `prefetch` sem limite no RabbitMQ.
- Fila do produtor Kafka em 100.000 e janela do RabbitMQ com o mesmo total.
- Ficam fora do padrão só os itens justificados, e cada um vai para a tabela de configuração do texto.

**Ajustado.** A recomendação de cada fornecedor para vazão e latência, definida depois de medir o cenário padrão. Candidatos:
- RabbitMQ: `prefetch` de 100 a 300, e mensagens transitórias se a assimetria A pesar;
- Kafka: `linger.ms` e `batch.size`.

## Medições antes de congelar

Pequenas, 2 a 3 rodadas por lado, só para decidir:

1. Kafka com e sem Nagle, a 20 mil e a 100 mil ev/s, com o modo Direct.
2. RabbitMQ com mensagem persistente e transitória, a 40 mil ev/s.
3. `prefetch` 300 contra sem limite, a 40 mil e a 60 mil ev/s.
4. Janelas do produtor (1 milhão contra 100 mil no Kafka; 8 mil contra 100 mil no RabbitMQ) na carga de saturação de cada um.
