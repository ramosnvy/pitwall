# Estado do projeto

Para as decisões de projeto e a sequência de trabalho, ver [PLANO.md](PLANO.md).

## 1. Ambiente

| Ferramenta | Situação |
| --- | --- |
| Git 2.55 | pronto |
| GitHub CLI 2.101 | autenticado como `ramosnvy` |
| .NET SDK 10.0.401 | pronto |
| Docker Desktop 4.91 | engine 29.8.0 no ar, 12 CPUs e 8 GB na VM |
| WSL2 | ativo |
| Repositório | https://github.com/ramosnvy/pitwall (público) |

## 2. O que já funciona

### Coleta de dados (`tools/openf1-downloader`)

Testado contra a API real. Baixa a telemetria histórica de uma sessão para JSONL, fatiando por piloto e por janela de tempo, respeitando 3 req/s e 30 req/min.

- Corrida do Bahrein 2024 (`session_key` 9472): **443.940 eventos de `car_data`**, 75 MB, coletados em 8,0 min.
- Coleta de 9 corridas com `car_data` e `location` **em andamento** (~2 h, limitada pela API).

### Infraestrutura (`infra/`)

Compose com Kafka 4.1 (KRaft), RabbitMQ 4.3.6, PostgreSQL 17, Prometheus e cAdvisor. **Executado e validado:** 5 containers saudáveis, as 3 tabelas criadas, tópico `telemetry` com 4 partições e o Prometheus coletando CPU e memória de todos os containers.

### Contrato e codec (`src/Pitwall.Contracts`)

`TelemetryEvent` como struct e serialização binária de 44 bytes de tamanho fixo. Payload binário em vez de JSON para que o custo de parse não domine a medição.

### Gerador de carga (`src/Pitwall.Replayer`)

Malha aberta, com multiplicação de frota. Medições com destino nulo:

| Taxa alvo | Taxa obtida | Erro | Atraso máximo |
| --- | --- | --- | --- |
| 50.000 ev/s | 49.999 | 0,00% | 2,27 ms |
| 100.000 ev/s | 99.998 | 0,00% | 0,29 ms |
| 200.000 ev/s | 199.996 | 0,00% | 4,30 ms |
| 500.000 ev/s | 499.986 | 0,00% | 2,13 ms |

A 100 mil ev/s com o dataset do Bahrein: frota de 26.820 carros, compressão temporal residual de 1,00×, zero reciclagem do dataset.

**O gerador tem folga de 5× sobre o nível de carga mais alto do plano.** Qualquer saturação medida nos experimentos será da arquitetura, não do instrumento. Estes números servem como seção de validação do instrumento no artigo.

## 3. Bloqueios

### 3.1 Decisões a fechar com o orientador

Detalhadas em [PLANO.md](PLANO.md), seção 1. As que mudam o escopo declarado no TCC1:

| Decisão | Impacto no TCC1 |
| --- | --- |
| Baseline *Direct* | A matriz vira 2 × 3, não 2 × 2; muda os objetivos específicos e a Figura 1 |
| Carga por multiplicação de frota | Muda a descrição do workload na metodologia |
| Payload binário de 44 B | Precisa constar na metodologia |
| Tamanho da mensagem como fator | Acrescenta um fator ao desenho experimental |
| Compressão desligada | Entra na tabela de configuração dos brokers |
| Definição de processamento (1d) | O TCC1 não define o que o módulo processa; agora está definido e fundamentado |
| Grau de paralelismo | **Decidido:** mesmo grau P nos dois, cada broker atingindo P pelo seu mecanismo |
| Semântica de entrega | **Decidido:** at-least-once dos dois lados, sem idempotência |

### 3.2 Lacunas do TCC1 identificadas na implementação

Pontos que o TCC1 não trata e que precisam entrar no texto do TCC2.

**Tudo roda em uma máquina só.** **Decidido: aceita e declarada** (ver [AMEACAS-VALIDADE.md](AMEACAS-VALIDADE.md)). Produtor, broker, processamento e banco estão no mesmo host — não há rede entre os componentes. Isso é uma ameaça à validade que precisa ser declarada, e limita o alcance da conclusão: os resultados descrevem uma implantação de nó único, não um cenário distribuído. Parte das vantagens do Kafka (replicação, tolerância a falhas, múltiplos consumidores em máquinas distintas) não aparece nessa configuração. Também é o que torna a medição de latência válida, já que os relógios do produtor e do consumidor são o mesmo contador.

**Ponto de saturação como métrica principal.** O TCC1 lista latência, throughput, CPU e memória. A vazão máxima sustentável de cada arquitetura — a carga em que a latência dispara — é o número mais forte que o trabalho pode produzir e não está na lista.

**Correção do resultado como critério.** O DEBS Grand Challenge avalia submissões também por correção. O `ResultDigest` cumpre esse papel: as seis variantes têm de produzir o mesmo resultado. Não é métrica de desempenho, é critério de validade da rodada.

**Jitter.** O RIoTBench mede a diferença entre a taxa esperada e a real. O replayer já registra isso do lado do produtor.

**Literatura de benchmarks ausente nos Trabalhos Relacionados.** O TCC1 cita apenas comparações entre brokers. Falta a linha de benchmarks de stream processing (YSB, Linear Road, DEBS, RIoTBench, ESPBench), que é o que fundamenta a escolha das operações e do protocolo experimental.

**A OpenF1 entrega uma fração da telemetria real.** São 3,7 Hz por carro, contra 150 a 300 sensores a até 100 Hz num carro de F1 real. Melhor declarar no texto do que deixar a banca apontar; a multiplicação de frota é a resposta.

**Protocolo de isolamento entre rodadas.** Tópico recriado e tabelas truncadas a cada rodada. Afeta a reprodutibilidade e precisa estar descrito na metodologia.

**Níveis de carga e repetições.** O TCC1 diz "diferentes níveis de carga" sem valores. Definir: 1k, 10k, 50k e 100k ev/s mais o teste de saturação, com no mínimo 10 repetições em ordem aleatória.

## 4. Pendências de implementação, em ordem

| # | Item | Depende de |
| --- | --- | --- |
| 1 | Ruído por réplica + teste de sensibilidade | — |
| 2 | ~~`Pitwall.Processing.Core`~~ concluído e verificado | — |
| 3 | ~~Sink Kafka e sink RabbitMQ no replayer~~ concluído | — |
| 4 | Consumers nos 3 modos (direct, channels, pipelines) | itens 2 e 3 |
| 5 | `Pitwall.Persistence` — `COPY` binário em lote | item 2 |
| 6 | `Pitwall.Metrics` — HdrHistogram e exportação CSV | item 4 |
| 7 | Script da matriz de experimentos | itens 4 a 6 |
| 8 | Experimento piloto e calibração das cargas | item 7 |
| 9 | Rodadas oficiais | item 8 |
| 10 | Análise estatística e gráficos | item 9 |

## 5. Questões em aberto

**Motor generativo de telemetria.** Avaliado e **adiado por decisão**. A diversidade que afeta a medição é entropia de payload, não realismo comportamental — e a cardinalidade de chave (26.820 carros) já é alta. A decisão fica condicionada ao teste de sensibilidade do item 1: se o ruído por réplica não alterar latência nem throughput, a replicação está justificada empiricamente e o motor não se paga. Se alterar, a extensão viável é reamostragem empírica por carro, nunca simulação de dinâmica veicular.

**Armazenamento durante os experimentos.** A 100 mil ev/s, uma rodada de 5 min gera ~2,1 GB de log no Kafka e ~2,6 GB em `processed_event`. Com 300 rodadas previstas, é obrigatório recriar o tópico e truncar as tabelas entre rodadas. Retenção do Kafka já reduzida para 15 min. Considerar persistir todos os eventos apenas nas cargas baixas e só as agregações nas altas.

## 6. Decisões tomadas até aqui

| Decisão | Motivo |
| --- | --- |
| Carga por multiplicação de frota | Preserva a cadência real do sensor; "26 mil carros a 3,7 Hz" é mais defensável que "corrida 1341× mais rápida" |
| Payload binário de 44 B | Com JSON, o parse dominaria a medição e achataria a diferença entre Channels e Pipelines |
| Compressão desligada | Réplicas têm payload quase idêntico; a compressão em lote inflaria o throughput do Kafka por artefato |
| Baseline *Direct* | Sem ela não dá para separar o efeito do broker do efeito do mecanismo interno |
| Tabelas UNLOGGED | Se o banco satura, as seis variantes parecem iguais |
| Um broker por vez (profiles) | O broker ocioso disputaria CPU com o que está sendo medido |
| Replayer sinaliza rodada inválida | Uma rodada em que a máquina engasgou entraria na análise como resultado ruim da arquitetura |
