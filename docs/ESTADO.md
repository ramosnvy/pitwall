# Estado do projeto

Retrato em `953341c`. Decisões e sequência de trabalho em [PLANO.md](PLANO.md); números em [RESULTADOS.md](RESULTADOS.md).

## 1. Ambiente

| Ferramenta | Situação |
| --- | --- |
| .NET SDK 10.0.401 | pronto |
| Docker Desktop 4.91 | 12 CPUs e 8 GB na VM do WSL2 |
| Repositório | https://github.com/ramosnvy/pitwall (público) |

## 2. Etapas do plano

| Etapa | Entrega | Situação |
| --- | --- | --- |
| A | Dataset e gerador de carga | concluída: 9 corridas baixadas, replayer validado até 500 mil ev/s |
| B | Infraestrutura | concluída |
| C | Sinks de Kafka e RabbitMQ | concluída |
| D | `Processing.Core` | concluída, com verificador de corretude |
| E | Consumidores nos três modos | concluída |
| F | Persistência e métricas | concluída |
| G | Runner e piloto | concluída; piloto refeito com os clientes em containers (PILOTO.md) |
| H | Rodadas oficiais | concluída: 165 rodadas, 154 válidas, commit `7e283c2` |
| I | Análise e gráficos | em andamento: consolidação e Kruskal-Wallis prontos; faltam as figuras |

## 3. Planejado × implementado

Módulos da Figura 1 do TCC1 e o que cada um virou:

| Módulo do TCC1 | Implementação | Relação com o TCC1 |
| --- | --- | --- |
| Ingestão: consome a OpenF1 e publica | `openf1-downloader` offline + `Pitwall.Replayer` em malha aberta com multiplicação de frota | mudou: a API ao vivo não gera alta frequência (PLANO 1a) |
| Mensageria: RabbitMQ ou Kafka | Kafka 4.1 com 4 partições; RabbitMQ 4.3.6 com 4 filas clássicas | como planejado |
| Processamento: Channels ou Pipelines | Direct, Channels e Pipelines sobre o mesmo `Processing.Core` | mudou: 2 × 3 em vez de 2 × 2; o controle Direct e a definição do processamento foram acrescentados (PLANO 1c, 1d) |
| Persistência em PostgreSQL | janelas agregadas por `COPY` binário, tabelas UNLOGGED | como planejado; `processed_event` existe no esquema mas não é gravada |
| Coleta de métricas | HdrHistogram no consumidor; cAdvisor e Prometheus nos quatro containers | como planejado, mais jitter e digest como critérios de validade |
| Tudo em Docker | produtor, consumidor, broker e banco em containers com limites fixos | como planejado, depois da correção da latência bimodal (REVISAO-TECNICA §1.1) |

## 4. Previsto e ainda não feito

| Item | Origem | Situação |
| --- | --- | --- |
| Experimento de evento lento | PLANO 1d; REVISAO §4.2 | não iniciado; próximo recomendado |
| Pipelines em lote | REVISAO §2.1 | não iniciado |
| Execução longa de 10 a 15 min por arquitetura | PLANO §2 | não feita |
| Ruído por réplica e teste de sensibilidade | versão anterior deste documento | não feito |
| Teto do RabbitMQ com 8 filas | RESULTADOS §9 | não iniciado |
| Tamanho da mensagem como fator | PLANO §2 | não iniciado; decidir com o orientador |
| Bateria sem persistência | PLANO 1e | `-NoPersist` pronto, não rodado na matriz |
| Figuras do artigo | etapa I | pendente |
| Fatorial 2^k / ANOVA | PLANO §2 | só Kruskal-Wallis feito |
| Mais corridas no workload | REVISAO §2.7 | 9 baixadas, 1 usada |
| Espera síncrona na contrapressão; registro incompleto no Pipe | REVISAO §2.3, §2.4 | abertos |
| RabbitMQ Streams | REVISAO §4.3 | decisão com o orientador |
| Nova Figura 1 e objetivos específicos do texto | PLANO §6 | pendente |
