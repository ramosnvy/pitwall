# Experimento piloto — varredura de saturação

Primeira varredura, 23/09/2026. Uma execução por ponto, 10 s de medição, 4 faixas, sem persistência.

**Estes números não são resultado do trabalho.** São uma execução única por ponto, e já medimos que a variância entre execuções nesta máquina chega a três ordens de grandeza. Servem para calibrar os níveis de carga da matriz oficial e — principalmente — para expor defeitos de configuração antes das rodadas que valem.

## Resultado bruto

| Arquitetura | 10k | 25k | 50k | 100k | 200k |
| --- | --- | --- | --- | --- | --- |
| kafka-direct | ok | ok | ok | ok | ok |
| kafka-channels | ok | ok | ok | ok | ok |
| kafka-pipelines | ok | ok | ok | ok | ok |
| rabbit-direct | ok | ok | saturou | — | — |
| rabbit-channels | ok | ok | saturou | — | — |
| rabbit-pipelines | ok | ok | saturou | — | — |

O Kafka sustentou 200 mil ev/s nas três variantes, com P99 de 26 a 151 ms. O RabbitMQ saturou entre 25 mil e 50 mil.

## O que o lado do produtor revelou

O relatório do produtor muda a leitura. O jitter de emissão (atraso máximo entre o instante previsto e o real):

| Broker | Taxa | Taxa obtida | Jitter |
| --- | --- | --- | --- |
| Kafka | 200.000 | 199.967 | 4,65 ms |
| RabbitMQ | 25.000 | 24.994 | 110 a 162 ms |
| RabbitMQ | 50.000 | 27.730 | 8.047 ms |

**A saturação do RabbitMQ está na publicação, não no consumo.** A 50 mil ev/s o produtor entregou 27,7 mil com 8 segundos de atraso acumulado: o consumidor nunca teve a chance de ficar para trás, porque as mensagens não chegaram.

Pelo critério de validade do próprio projeto — descartar rodada com jitter acima de 50 ms — as rodadas do RabbitMQ a 25 mil **já são inválidas**. O teto válido medido do RabbitMQ nesta configuração fica entre 10 mil e 25 mil ev/s.

## Causa investigada

**Hipótese 1: mensagens persistentes.** Testada e descartada. A 50 mil ev/s, persistente entregou 45.960 ev/s e transiente 47.239 — diferença de 2,8%, dentro do ruído. O custo de disco não é o limitante.

**Hipótese 2: canal único no produtor.** É a explicação provável. O `RabbitMqSink` publica nas quatro filas por **um único canal AMQP**, enquanto o consumidor usa quatro canais, um por fila. Canais do RabbitMQ serializam as operações, então o produtor tem um quarto do paralelismo do consumidor — e nenhum do paralelismo que o produtor Kafka obtém internamente ao agrupar por partição.

**Consequência:** o teto de 25 mil ev/s atribuído ao RabbitMQ pode ser artefato da implementação do produtor, não limite do broker. Publicar esse número sem corrigir seria atribuir à arquitetura um defeito do instrumento.

## Ações antes da matriz oficial

1. **Um canal por faixa no produtor RabbitMQ**, espelhando o consumidor, e repetir a varredura.
2. **Reavaliar a barreira de confirmação**: hoje são 1.000 publicações em voo, aguardadas em sequência. Com um canal por faixa, o valor precisa ser revisto.
3. **Incluir o jitter do produtor no critério de saturação** do `sweep.ps1`. Hoje o script decide apenas pela vazão e pelo P99 do consumidor, e por isso classificou como "ok" rodadas que o próprio projeto considera inválidas.

## Níveis de carga propostos (provisórios)

Dependem da nova varredura. Se o teto do RabbitMQ subir para a faixa de 50 a 100 mil, uma escolha que cobre o joelho das duas famílias seria **10k, 25k, 50k, 100k e 200k**, com as duas maiores servindo para caracterizar apenas o Kafka — o que é resultado legítimo, desde que a saturação do RabbitMQ esteja bem medida e não seja artefato.
