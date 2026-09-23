# Experimento piloto — varredura de saturação

Segunda varredura, 23/09/2026, após as correções. Uma execução por ponto, 10 s de medição, 4 faixas, sem persistência. Critério de saturação: vazão abaixo de 90% da taxa alvo, **ou** P99 acima de 1 s, **ou** jitter de emissão do produtor acima de 50 ms.

**Estes números não são resultado do trabalho.** É uma execução única por ponto, e a variância entre execuções nesta máquina chega a três ordens de grandeza. O piloto serve para calibrar os níveis de carga e expor defeitos antes das rodadas que valem — e cumpriu esse papel: encontrou um estouro de tipo que contaminava toda carga acima de 30 mil ev/s.

## Resultado

| Arquitetura | Vazão máxima sustentada | P99 no topo |
| --- | --- | --- |
| kafka-direct | > 200.000 ev/s | 11 ms |
| kafka-channels | > 200.000 ev/s | 11 ms |
| kafka-pipelines | > 200.000 ev/s | 9 ms |
| rabbit-direct | 20.000 ev/s | 8 ms |
| rabbit-channels | 20.000 ev/s | 7 ms |
| rabbit-pipelines | 30.000 ev/s | 6 ms |

O Kafka **não saturou**: 200 mil era o teto da varredura, não o dele. O jitter do produtor ficou entre 4 e 15 ms em todos os níveis, e o P99 do consumidor nunca passou de 16 ms.

## Leitura correta do lado do RabbitMQ

O limite do RabbitMQ está na **publicação**, e o produtor é o mesmo código nas três variantes — ele não sabe qual mecanismo o consumidor usa. Logo:

**A diferença entre 20.000 e 30.000 nas três linhas do RabbitMQ é ruído em torno do limiar, não diferença entre Direct, Channels e Pipelines.** O jitter nas rodadas de 30 mil ficou em 37, 52 e 57 ms, com o corte em 50 ms.

| Taxa | Jitter observado (3 rodadas) |
| --- | --- |
| 10.000 | 16, 22, 23 ms |
| 20.000 | 23, 28, 30 ms |
| 30.000 | 38, 52, 57 ms |
| 40.000 | 194 ms |

**Consequência para o desenho experimental:** com RabbitMQ, os três mecanismos internos só podem ser comparados em cargas até ~20 mil ev/s. Acima disso o produtor satura antes e o consumidor nunca é pressionado — qualquer diferença medida ali seria ruído.

## Níveis de carga propostos

| Nível | Brokers | Propósito |
| --- | --- | --- |
| 5.000 | ambos | Carga baixa, referência |
| 10.000 | ambos | Ambos confortáveis |
| 20.000 | ambos | Limite superior do RabbitMQ válido |
| 50.000 | só Kafka | Além do alcance do RabbitMQ |
| 100.000 | só Kafka | |
| 200.000 | só Kafka | Topo medido |

A comparação 2 × 3 completa acontece nos três primeiros níveis. Os três últimos caracterizam a faixa em que apenas o Kafka opera — o que é resultado legítimo e, em si, uma das conclusões do trabalho.

## Correções que o piloto motivou

**Canal único no produtor RabbitMQ.** Publicar nas quatro filas por um canal só dava ao produtor um quarto do paralelismo do consumidor. Com um canal por faixa, o teto subiu de 27,7 mil para 42,7 mil ev/s. O valor anterior era limite do instrumento.

**Estouro do número do carro.** A multiplicação de frota soma `réplica × 100` ao número original; com fator 671 (necessário para 50 mil ev/s) o valor passava de 32.767 e o `short` virava negativo, quebrando o cálculo da faixa. Afetava silenciosamente toda carga acima de ~30 mil ev/s. O campo passou a `int`, o codec de 44 para 46 bytes e as colunas do banco para `INTEGER`.

**Jitter no critério de saturação.** Sem ele, o script classificava como válidas rodadas em que o gerador não sustentou a taxa — exatamente o que mascarou o problema do RabbitMQ na primeira varredura.

## Ajuste de configuração dos dois brokers

Regra adotada: **aplicar o que cada fornecedor documenta como prática padrão para vazão, medir, e parar aí.** Sem busca por configuração ótima — isso levaria a ajustar um lado até ele ganhar.

### RabbitMQ: o gargalo era a barreira de confirmação, não a conexão

A 60 mil ev/s alvo:

| Configuração | Taxa publicada | Jitter |
| --- | --- | --- |
| 1 conexão, barreira 250 | 42.396 ev/s | 4.150 ms |
| 4 conexões, barreira 250 | 42.603 ev/s | 4.082 ms |
| 4 conexões, barreira 1000 | **59.227 ev/s** | 353 ms |
| 4 conexões, barreira 4000 | 59.945 ev/s | 324 ms |

**A conexão por faixa não teve efeito mensurável** (42,4 mil contra 42,6 mil), embora seja a recomendação do fornecedor. Foi mantida por ser a prática documentada, e o fato de não alterar nada é em si um dado a registrar.

**A barreira de confirmação era o limite real.** Com 250 publicações em voo por faixa, o produtor passava o tempo esperando confirmação. Em 1.000, o teto foi de 42,6 mil para 59,2 mil. De 1.000 para 4.000 o ganho foi marginal.

Custo em latência: **nenhum em carga normal.** A 10 mil ev/s, barreira 250 e 1.000 deram a mesma média de 1,11 ms.

Teto do RabbitMQ com a configuração congelada: **cerca de 60 mil ev/s** (a 80 mil, o produtor trava em 60,3 mil com 3,3 s de atraso).

### Kafka: `linger.ms` é tão crítico quanto

| Configuração | Taxa | Publicado | Latência média do consumidor |
| --- | --- | --- | --- |
| `linger.ms=0` | 10.000 | 6.497 ev/s | **3.005 ms** |
| `linger.ms=5` | 10.000 | 9.999 ev/s | 3,73 ms |
| `linger.ms=5` | 200.000 | 199.741 ev/s | 7,53 ms |
| `linger.ms=5` | 300.000 | 299.855 ev/s | 4,34 ms |
| `linger.ms=5` | 400.000 | 399.838 ev/s | 5,41 ms |

Com `linger.ms=0`, o Kafka desaba para 6,5 mil ev/s e 3 segundos de latência: cada mensagem vira uma ida e volta com `acks=all`. O parâmetro de agrupamento é tão determinante para o Kafka quanto a barreira de confirmação é para o RabbitMQ — **os dois são sensíveis a agrupamento, e comparar sem ajustar ambos produziria resultado arbitrário.**

Teto do Kafka: **acima de 400 mil ev/s**, sem saturar. O gerador foi validado até 500 mil.

## Configuração congelada

| Parâmetro | Kafka | RabbitMQ |
| --- | --- | --- |
| Garantia de entrega | `acks=all` | publisher confirms |
| Agrupamento | `linger.ms=5`, `batch.size=64 KB` | barreira de 1.000 por faixa |
| Compressão | desligada | não aplicável |
| Paralelismo | 4 partições | 4 filas, 4 canais, 4 conexões |
| Durabilidade | log (padrão) | mensagens persistentes |
| Idempotência | desligada | não aplicável |

## O que a documentação dos fornecedores acrescentou

A [comparação oficial do RabbitMQ com o Kafka](https://www.rabbitmq.com/docs/compare/kafka) traz três pontos que afetam este trabalho.

**O princípio, que vale citar na metodologia.** O próprio fornecedor adverte que um benchmark que ajusta um lado com afinco e deixa o outro no padrão está comparando esforço de ajuste, não capacidade dos sistemas. É exatamente o que o `linger.ms=0` demonstrou aqui: com um parâmetro mal escolhido, o Kafka fica sete vezes pior que o RabbitMQ.

**A expectativa de vazão declarada.** O fornecedor cita filas clássicas na ordem de 100 mil mensagens/s, filas quorum em 80 mil, e streams em vários milhões. Nosso teto de 60 mil está em torno de 60% do valor declarado para filas clássicas — diferença atribuível ao rastreamento de confirmação por mensagem, à persistência, ao fato de produtor, consumidor e broker dividirem a mesma máquina, e a mensagens de 46 bytes, em que o custo fixo por mensagem domina.

**Tentativas adicionais, com ganho marginal** (alvo de 80 mil ev/s):

| Configuração | Publicado |
| --- | --- |
| 4 faixas, persistente (congelada) | 59.411 ev/s |
| 4 faixas, transiente | 63.773 ev/s (+7,3%) |
| 8 faixas, persistente | 62.875 ev/s (+5,8%) |

Descritores de arquivo estão em 1.048.576, muito acima dos 50 mil recomendados, e não são limitante. Persistência mantida por equivaler ao log do Kafka, que sempre grava; o custo de 7,3% fica documentado. Número de faixas mantido em 4 porque precisa ser igual nos dois brokers, e aumentá-lo exigiria refazer também o lado do Kafka por um ganho de 6%.

**Parada de ajuste declarada aqui.** Os ganhos caíram para a casa de 5 a 7%, dentro da variância do ambiente.

## A questão de fundo: fila não é log

O ponto mais relevante da documentação oficial não é de ajuste, é conceitual.

> "a super stream corresponds to a Kafka topic, and a stream to one of its partitions"

O tópico do Kafka é um **log**: append-only, com leitura não destrutiva e retenção independente do consumo. A fila clássica do RabbitMQ é uma **fila**: a mensagem sai na confirmação. São estruturas de dados diferentes, e o trabalho atual compara uma com a outra.

O RabbitMQ tem um log desde a versão 3.9, em 2021: os **streams**. O equivalente honesto de um tópico do Kafka com 4 partições é um *super stream* com 4 streams.

**Isso é uma lacuna dos comparativos que o TCC1 cita** — Dobbelaere e Esmaili (2017) é anterior aos streams, e os trabalhos posteriores seguem comparando com filas clássicas. Incluir streams responderia à pergunta que a literatura ainda não respondeu: quanto da diferença entre Kafka e RabbitMQ vem do broker e quanto vem de se estar comparando um log com uma fila.

**Custo:** cliente .NET próprio (`RabbitMQ.Stream.Client`, protocolo distinto do AMQP), um sink e uma fonte novos. A matriz passaria de 2 × 3 para 3 × 3.

**Decisão pendente do orientador.** Não é ajuste de configuração, é ampliação de escopo — e das boas, porque transformaria o trabalho de "mais um comparativo entre brokers" em um que separa o efeito do broker do efeito da abstração.

## Níveis de carga definidos

| Nível | Brokers | Papel |
| --- | --- | --- |
| 5.000 | ambos | Carga baixa |
| 10.000 | ambos | Ambos confortáveis |
| 20.000 | ambos | |
| 30.000 | ambos | Próximo do joelho do RabbitMQ |
| 100.000 | só Kafka | Além do alcance do RabbitMQ |
| 200.000 | só Kafka | |
| 400.000 | só Kafka | Topo medido |

A comparação 2 × 3 completa ocorre nos quatro primeiros níveis. A razão entre os tetos é de cerca de **7×** (60 mil contra mais de 400 mil) — e esse número só é defensável porque foi obtido depois de ajustar os dois lados de boa-fé.
