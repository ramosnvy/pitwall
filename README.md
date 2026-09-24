# PitWall

Avaliação experimental de arquiteturas compostas de processamento de eventos em alta frequência utilizando .NET.

O nome vem do *pit wall*, o posto à beira da pista de onde os engenheiros de Fórmula 1 acompanham a telemetria dos carros em tempo real.

> Trabalho de Conclusão de Curso — Engenharia de Software, UTFPR Dois Vizinhos.
> Autor: Pedro Augusto Ramos de Sousa.

## O que este projeto mede

O experimento compara arquiteturas que combinam um **sistema de mensageria distribuída** com um **mecanismo de processamento concorrente interno do .NET**, sob cargas de eventos crescentes:

|                | Direct (baseline) | Channels | Pipelines |
| -------------- | ----------------- | -------- | --------- |
| **RabbitMQ**   | RD                | RC       | RP        |
| **Kafka**      | KD                | KC       | KP        |

A variante *Direct* processa o evento no próprio callback do consumer, sem mecanismo interno. Ela existe para separar o efeito do broker do efeito do mecanismo de processamento.

**Métricas coletadas:** latência (média e percentis P50/P95/P99), throughput (eventos/s), uso de CPU e uso de memória.

## Workload

Telemetria real de Fórmula 1 da API pública [OpenF1](https://openf1.org) (dados históricos, gratuitos, de 2023 em diante).

Os dados são baixados **uma única vez** para disco e reproduzidos por um *replayer* local — a API não é chamada durante os experimentos. Como a telemetria original é amostrada a 3,7 Hz por carro (cerca de 75 eventos/s com o grid completo), o replayer aplica fatores de aceleração e multiplicação de carros para atingir as taxas-alvo de cada nível de carga.

## Stack

- .NET 10 (LTS) / C#
- Apache Kafka (modo KRaft) via `Confluent.Kafka`
- RabbitMQ via `RabbitMQ.Client` 7.x
- PostgreSQL (persistência em lote com `COPY` binário do Npgsql)
- Docker Compose, com limites de CPU e memória fixos por container
- HdrHistogram para percentis de latência; Prometheus e cAdvisor para métricas de recursos

## Estrutura pretendida

```
src/
  Pitwall.Contracts/        modelo do evento e serialização
  Pitwall.Replayer/         ingestão: lê o dataset local e publica na taxa-alvo
  Pitwall.Processing.Core/  lógica de processamento (idêntica em todas as variantes)
  Pitwall.Consumer.Kafka/   modos: direct | channels | pipelines
  Pitwall.Consumer.Rabbit/  modos: direct | channels | pipelines
  Pitwall.Persistence/      escrita em lote no PostgreSQL
  Pitwall.Metrics/          HdrHistogram e exportação de resultados
tools/openf1-downloader/    coleta única do dataset
infra/                      docker-compose, prometheus, grafana
experiments/                scripts que executam a matriz de experimentos
analysis/                   notebooks de análise estatística
results/                    CSVs brutos das execuções
docs/                       plano de trabalho e decisões de projeto
```

## Estado atual

Matriz oficial executada. Resultados em [docs/RESULTADOS.md](docs/RESULTADOS.md); revisão técnica e correções do instrumento em [docs/REVISAO-TECNICA.md](docs/REVISAO-TECNICA.md); decisões de projeto em [docs/PLANO.md](docs/PLANO.md). Próxima fase, o uso ajustado de Channels e Pipelines, em [docs/APROFUNDAMENTO.md](docs/APROFUNDAMENTO.md).

## Licença

Código sob licença MIT (ver [LICENSE](LICENSE)). O texto do TCC segue a licença indicada no próprio documento.
