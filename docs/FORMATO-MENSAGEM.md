# Formato da mensagem

A mensagem que trafega pelos brokers é um registro binário de tamanho fixo de **44 bytes**, idêntico nas seis variantes da matriz.

## Por que binário e não JSON

**Motivo metodológico.** O objeto do estudo é o custo do transporte e do mecanismo de processamento. Com JSON, o tempo de interpretação do texto passaria a dominar a medição e achataria justamente a diferença entre Channels e Pipelines, que é metade da matriz. O formato precisa ser barato o bastante para que o que sobra na medição seja arquitetura.

**Motivo de realismo.** Telemetria real trafega em binário: barramento CAN em veículos, cargas compactas em MQTT, Protobuf ou Avro em frotas conectadas, formato proprietário na Fórmula 1. JSON aparece na borda de integração — que é exatamente o papel da OpenF1, que serve JSON por HTTP. O dado nasce binário, vira JSON para atravessar uma API pública, e o replayer o devolve à forma binária em que ele circula na prática.

As duas razões apontam para o mesmo lado, o que é raro e conveniente.

## Layout

Little-endian, campos em posição fixa, sem cabeçalho e sem delimitador.

| Deslocamento | Tamanho | Campo | Observação |
| --- | --- | --- | --- |
| 0 | 8 | `Sequence` | Sequencial do replayer, identifica o evento de ponta a ponta |
| 8 | 4 | `SessionKey` | Sessão da OpenF1 de origem |
| 12 | 2 | `DriverNumber` | Número do carro; também é a chave de particionamento |
| 14 | 8 | `EventTime` | Instante original da amostra, em ticks UTC |
| 22 | 8 | `PublishedTicks` | Instante da publicação, em ticks de `Stopwatch` |
| 30 | 2 | `Speed` | km/h |
| 32 | 4 | `Rpm` | |
| 36 | 2 | `Gear` | |
| 38 | 2 | `Throttle` | 0 a 100 |
| 40 | 2 | `Brake` | 0 ou 100 na OpenF1 |
| 42 | 2 | `Drs` | |

Total: 44 bytes. Verificado em execução real — a fila do RabbitMQ registrou 15.840.000 bytes para 360.000 mensagens, exatamente 44 por mensagem.

## Duas decisões embutidas no layout

**`PublishedTicks` viaja dentro da mensagem.** É o que permite medir latência sem sincronizar relógios: o consumidor compara o valor recebido com seu próprio `Stopwatch.GetTimestamp()`. Só funciona porque produtor e consumidor rodam na mesma máquina e compartilham o mesmo contador monotônico — o que é, ao mesmo tempo, a razão de a medição ser válida e o motivo de o experimento ser de nó único (ver [AMEACAS-VALIDADE.md](AMEACAS-VALIDADE.md)).

**Tamanho fixo dispensa enquadramento por prefixo na variante Pipelines.** Como todo registro tem 44 bytes, o `PipeReader` sabe quando há um evento completo apenas pelo tamanho do buffer. Isso mantém a variante Pipelines no seu terreno natural — fluxo de bytes, sem alocação por mensagem — sem introduzir um cabeçalho que o Channels não pagaria.

## Implementação

`TelemetryCodec` em `src/Pitwall.Contracts`:

- `Write(Span<byte>, in TelemetryEvent)` — escreve os 44 bytes.
- `Read(ReadOnlySpan<byte>)` — lê de um buffer contíguo.
- `TryRead(ref ReadOnlySequence<byte>, out TelemetryEvent)` — lê de uma sequência possivelmente fragmentada, como a que o `PipeReader` entrega, copiando para a pilha apenas quando o registro cruza a fronteira de dois segmentos.

O evento é um `readonly record struct`, não uma classe: a taxas de dezenas de milhares de eventos por segundo, uma alocação por evento apareceria como pressão de coletor de lixo dentro das medições.

## O nível "mensagem grande"

O tamanho da mensagem é um fator do experimento com dois níveis. O segundo nível é um **envelope enriquecido de aproximadamente 1 KB** — o evento acrescido de contexto de volta, setor, condições e identificação, no formato em que um sistema de integração realmente o transportaria.

Isso responde a duas perguntas de uma vez: onde fica o ponto de virada entre os brokers conforme a mensagem cresce (o Dobbelaere e Esmaili e o Umam et al. indicam que RabbitMQ leva vantagem em mensagem pequena e Kafka em carga volumosa), e como as duas famílias de formato se comportam, já que o envelope grande é naturalmente textual.
