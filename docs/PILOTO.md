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

## Questão em aberto

O teto de publicação do RabbitMQ pode subir mais com uma conexão TCP por faixa (hoje são quatro canais sobre uma conexão) ou com barreira de confirmação maior. Vale um teste limitado antes da matriz oficial: se o teto subir muito, a faixa de comparação entre os dois brokers aumenta, e o trabalho ganha alcance.
