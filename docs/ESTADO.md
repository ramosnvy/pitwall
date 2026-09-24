# Estado do projeto

Retrato em `e36b3f8`, 24/09/2026. Decisões em [PLANO.md](PLANO.md); números em [RESULTADOS.md](RESULTADOS.md) e [IMPLEMENTACAO.md](IMPLEMENTACAO.md); configurações em [AUDITORIA-CONFIG.md](AUDITORIA-CONFIG.md).

## 1. Em que momento estamos

**Calibração do protocolo, antes da matriz oficial (v3).** A carga atual está implementada e já foi medida três vezes: a primeira matriz, o experimento 2×2 e a varredura de saturação a 100 Hz. O orientador respondeu às perguntas em 24/09 (PLANO §1g), e a auditoria das configurações encontrou dois defeitos e três assimetrias.

Antes da matriz, falta corrigir o que a auditoria achou, medir o que ainda está em dúvida e congelar o **cenário padrão**.

## 2. Ambiente

| Ferramenta | Situação |
| --- | --- |
| .NET SDK 10.0.401 | pronto |
| Docker Desktop 4.91 | 12 CPUs e 8 GB na VM do WSL2 |
| Kafka 4.1.0, RabbitMQ 4.3.6, PostgreSQL 17 | em containers, um broker ligado por vez |
| Repositório | https://github.com/ramosnvy/pitwall (público) |

## 3. O que está decidido

| Tema | Decisão | Onde |
| --- | --- | --- |
| Combinações | 2 brokers × 3 modos (Direct, Channels, Pipelines); o Direct é o grupo de controle | PLANO §1c |
| Cenários de configuração | **Padrão** como linha de base, depois **ajustado** com a recomendação de cada fornecedor | PLANO §1g, pedido do orientador |
| Carga | Telemetria da OpenF1, frota multiplicada, **100 Hz com pontos interpolados**; 250 s da corrida a partir do minuto 10 | PLANO §1a; aprovado pelo orientador |
| Sensibilidade | Bateria a 3,7 Hz, o dado sem interpolação | PLANO §1a |
| No texto | O trabalho não analisa a corrida; a conferência do resultado mostra que o processamento está certo | PLANO §1g |
| Mensagem | Binária de 46 bytes, sem compressão, entrega at-least-once nos dois brokers | PLANO §1a, §1f |
| Divisão em filas | 4 faixas, CRC32 nos dois, cada carro sempre na mesma faixa | PLANO §1g; aprovado pelo orientador |
| Equivalência | Limites de armazenamento equivalentes entre os brokers | AUDITORIA-CONFIG |
| Ambiente de medição | Núcleos exclusivos (gerador 0–3, broker 4–7, consumidor 8–10, banco e métricas 11), painéis desligados, máquina sem uso | PLANO §1a; IMPLEMENTACAO §7 |
| Rodada | 10 s de aquecimento e 90 s de medição | PLANO §2 |
| Validade | Atraso de envio até 50 ms e resultado idêntico entre as seis combinações; descartes registrados | AMEACAS-VALIDADE |
| Faixas de carga | Kafka de 10 mil a 300 mil ev/s; RabbitMQ de 10 mil a 60 mil | proposta sem objeção do orientador |
| Estatística | Descritiva: 10 repetições, mediana, desvio padrão e descarte de discrepantes; Kruskal-Wallis como apoio; sem ANOVA | PLANO §1g |
| Instabilidade do Kafka com Channels | Investigar antes da matriz | decisão de 24/09 |
| Figura 1 do texto | Formato da Figura 1 do TCC1, com os passos de cada etapa dentro das caixas | mapa da arquitetura, layout C |
| Fora do escopo | RabbitMQ Streams (trabalho futuro), tamanho de mensagem, rodadas longas, mais corridas, 8 filas no RabbitMQ | decisões de 24/09 |

## 4. O que já foi medido

| Experimento | Commit | Resultado |
| --- | --- | --- |
| Primeira matriz: 3,7 Hz, cota de CPU, 165 rodadas | `7e283c2` | Vira calibração: a cota distorcia o RabbitMQ |
| 2×2: cota × núcleos exclusivos, divisão antiga × CRC32 | `7cee9c2` | Núcleos exclusivos reduzem o P99 do RabbitMQ em 40% a 64%. O CRC32 aumenta a CPU do broker em 50 a 130 pontos percentuais e o P99. 35 de 40 rodadas válidas |
| Varredura de saturação a 100 Hz | `7b47f88` | Kafka sustenta de 200 mil a 300 mil ev/s; RabbitMQ, de 50 mil a 80 mil. Banco abaixo de 2% de CPU |
| Kafka com Channels | `7b47f88` | Instável: a 100 mil ev/s, um P99 de 200 ms que não se repetiu (5,9 ms duas vezes); a 200 mil, 78 ms e 20 ms |
| Custo dos mecanismos internos | RESULTADOS | Cerca de 3,6 µs de CPU por evento, sem ganho de latência sob processamento leve |
| Auditoria das configurações | `e36b3f8` | Nagle ligado só no Kafka, por escolha nossa; limite de memória do RabbitMQ calculado sobre a VM, não sobre o container; assimetrias em gravação em disco, fila do produtor e busca antecipada do consumidor |

## 5. Planejado no TCC1 × implementado

| Módulo do TCC1 | Implementação | Relação com o TCC1 |
| --- | --- | --- |
| Ingestão: consome a OpenF1 e publica | `openf1-downloader` offline + `Pitwall.Replayer` em malha aberta, com multiplicação de frota e interpolação a 100 Hz | mudou: a API ao vivo não gera alta frequência (PLANO §1a) |
| Mensageria: RabbitMQ ou Kafka | Kafka 4.1 com 4 partições; RabbitMQ 4.3.6 com 4 filas clássicas; CRC32 nos dois | como planejado |
| Processamento: Channels ou Pipelines | Direct, Channels e Pipelines sobre o mesmo `Processing.Core` | mudou: 2 × 3 em vez de 2 × 2 (PLANO §1c, §1d) |
| Persistência em PostgreSQL | Janelas agregadas por `COPY` binário, tabelas UNLOGGED | como planejado |
| Coleta de métricas | HdrHistogram no consumidor; cAdvisor e Prometheus; alocações e coletas de lixo por rodada | como planejado, mais atraso de envio e conferência como critérios de validade |
| Tudo em Docker | Containers com núcleos exclusivos | como planejado, com o protocolo de núcleos de IMPLEMENTACAO §7 |
| — | Painel local (Grafana, Loki) e replay 2D das corridas | acréscimo; fora da medição |

## 6. O que falta, em ordem de dependência

| # | Item | Depende de | Situação |
| --- | --- | --- | --- |
| 1 | Corrigir os defeitos da auditoria e voltar ao padrão os valores sem justificativa | — | pronto para começar |
| 2 | Medições pequenas: Nagle, mensagem persistente, `prefetch`, janelas do produtor | 1 | não iniciado |
| 3 | Congelar o cenário padrão | 2 | não iniciado |
| 4 | Investigar a instabilidade do Kafka com Channels | 3 | não iniciado |
| 5 | Corrigir a espera síncrona na contrapressão e o registro incompleto no Pipe; registrar as variáveis `DOTNET_*` por rodada | — | abertos (REVISAO §2.3, §2.4) |
| 6 | Fixar a regra de descarte de discrepantes e o critério de saturação | — | a decidir |
| 7 | Matriz v3 no cenário padrão | 3, 4, 5, 6 | não iniciado |
| 8 | Cenário ajustado | 7 | não iniciado |
| 9 | Justificativa da parte .NET: memória e coleta de lixo; segundo cenário de carga | 7 | segundo cenário ainda não proposto ao orientador |
| 10 | Análise, figuras e texto; nova Figura 1 e objetivos; pendências do documento do TCC1 | 7, 8, 9 | não iniciado |

## 7. Pontos de atenção

- **A parte de .NET é a mais frágil aos olhos do orientador.** Sob processamento leve, Channels e Pipelines não ganham latência e custam CPU. Sem o segundo cenário, a conclusão sobre eles fica estreita.
- **Tempo de máquina.** Com 10 repetições, a matriz fica perto de 370 rodadas contando a sensibilidade. Se apertar, cortar pontos de carga do Kafka, não repetições.
- **Sem resposta do orientador:** núcleos exclusivos (mantidos) e RabbitMQ Streams (trabalho futuro). O critério de saturação e a regra de descarte não foram enviados a ele.
