# Estado do projeto

Atualizado em 22/09/2026. Para as decisões de projeto e o cronograma completo, ver [PLANO.md](PLANO.md).

## 1. Ambiente

| Ferramenta | Situação |
| --- | --- |
| Git 2.55 | pronto |
| GitHub CLI 2.101 | autenticado como `ramosnvy` |
| .NET SDK 10.0.401 | pronto |
| Docker Desktop 4.91 | instalado, **aguardando reinicialização do Windows** |
| WSL2 | habilitado, ativa no próximo boot |
| Repositório | https://github.com/ramosnvy/pitwall (público) |

## 2. O que já funciona

### Coleta de dados (`tools/openf1-downloader`)

Testado contra a API real. Baixa a telemetria histórica de uma sessão para JSONL, fatiando por piloto e por janela de tempo, respeitando 3 req/s e 30 req/min.

- Corrida do Bahrein 2024 (`session_key` 9472): **443.940 eventos de `car_data`**, 75 MB, coletados em 8,0 min.
- Coleta de 9 corridas com `car_data` e `location` **em andamento** (~2 h, limitada pela API).

### Infraestrutura (`infra/`)

Compose com Kafka 4.1 (KRaft), RabbitMQ 4, PostgreSQL 17, Prometheus e cAdvisor. Validado com `docker compose config`. **Ainda não foi executado** — depende do reboot.

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

### 3.1 Reinicialização do Windows

Nada que envolva broker ou banco pode ser testado até o WSL2 ativar. Depois de reiniciar: abrir o Docker Desktop (na primeira execução ele pede aceite dos termos) e validar com `docker run --rm hello-world`.

### 3.2 Decisões a fechar com o orientador

Detalhadas em [PLANO.md](PLANO.md), seção 1. As que mudam o escopo declarado no TCC1:

| Decisão | Impacto no TCC1 |
| --- | --- |
| Baseline *Direct* | A matriz vira 2 × 3, não 2 × 2; muda os objetivos específicos e a Figura 1 |
| Carga por multiplicação de frota | Muda a descrição do workload na metodologia |
| Payload binário de 44 B | Precisa constar na metodologia |
| Tamanho da mensagem como fator | Acrescenta um fator ao desenho experimental |
| Compressão desligada | Entra na tabela de configuração dos brokers |

## 4. Pendências de implementação, em ordem

| # | Item | Depende de | Estimativa |
| --- | --- | --- | --- |
| 1 | Ruído por réplica + teste de sensibilidade | — | meio dia |
| 2 | `Pitwall.Processing.Core` — lógica idêntica às 6 variantes | decisão 1d | 1 dia |
| 3 | Sink Kafka e sink RabbitMQ no replayer | reboot | 1 dia |
| 4 | Consumers nos 3 modos (direct, channels, pipelines) | itens 2 e 3 | 3 a 4 dias |
| 5 | `Pitwall.Persistence` — `COPY` binário em lote | item 2 | 1 dia |
| 6 | `Pitwall.Metrics` — HdrHistogram e exportação CSV | item 4 | 1 dia |
| 7 | Script da matriz de experimentos | itens 4 a 6 | 1 dia |
| 8 | Experimento piloto e calibração das cargas | item 7 | 2 dias |
| 9 | Rodadas oficiais | item 8 | 1 semana |
| 10 | Análise estatística e gráficos | item 9 | 1 semana |

## 5. Questões em aberto

**Motor generativo de telemetria.** Avaliado e **adiado por decisão**. A diversidade que afeta a medição é entropia de payload, não realismo comportamental — e a cardinalidade de chave (26.820 carros) já é alta. A decisão fica condicionada ao teste de sensibilidade do item 1: se o ruído por réplica não alterar latência nem throughput, a replicação está justificada empiricamente e o motor não se paga. Se alterar, a extensão viável é reamostragem empírica por carro (2 a 3 dias), nunca simulação de dinâmica veicular.

**Data da defesa.** O cronograma assume início de dezembro de 2026. **Não confirmado.**

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
