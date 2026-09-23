# Plano de trabalho — TCC2

Documento vivo. Registra as decisões de projeto e a sequência de trabalho. Atualizar conforme as decisões forem fechadas com o orientador.

O trabalho é organizado por ordem de dependência (seção 5), não por calendário. O cronograma do TCC1 (março a setembro de 2026) ficou para trás e será refeito quando houver uma data de defesa definida.

## 1. Decisões em aberto

Pontos que não estavam definidos na proposta do TCC1 e que mudam a implementação.

### 1a. A OpenF1 não é, por si só, uma fonte de alta frequência

A telemetria é amostrada a 3,7 Hz por carro. Medido na corrida do Bahrein 2024: 443.940 eventos de `car_data` em 99 minutos, ou seja 75 eventos/s com os 20 carros. Os limites do plano gratuito são 3 requisições/s e 30/min, e os dados ao vivo exigem assinatura paga.

**Decidido:** os dados históricos são baixados uma vez para disco e reproduzidos por um *replayer*. A API não é chamada durante os experimentos.

**A carga vem da multiplicação de carros, não da aceleração do tempo.** Para tirar 100 mil eventos/s de uma corrida que produz 75, há duas saídas: comprimir o tempo 1341 vezes, ou simular 1341 vezes mais carros. A segunda foi a escolhida, porque preserva a cadência real de cada sensor (3,7 Hz por carro) e corresponde ao cenário de telemetria, IoT e frota que motiva o trabalho. Acelerar o tempo distorceria o intervalo entre amostras de um mesmo carro, que é a característica central do dado.

Cada réplica recebe número de carro próprio e deslocamento de fase dentro do intervalo de amostragem, para que a frota não publique em rajadas sincronizadas — o que favoreceria artificialmente o broker que agrupa melhor em lote.

Efeito colateral útil: com fator de frota, o dataset deixa de ser reciclado. A 100 mil ev/s com fator 1341, dez segundos de medição consomem menos de 750 das 443.940 amostras.

**Compressão desligada nos dois brokers.** As réplicas de uma mesma amostra têm payload quase idêntico, então a compressão em lote do Kafka renderia muito mais do que renderia com telemetria real, inflando o throughput do Kafka por artefato do workload. Se a compressão virar objeto de estudo, entra como fator explícito do experimento.

### 1b. Channels e Pipelines não são equivalentes

`System.Threading.Channels` é uma fila produtor-consumidor de **objetos**. `System.IO.Pipelines` opera sobre **fluxos de bytes**.

**Decisão proposta:**
- **Channels:** o consumer desserializa a mensagem para objeto, escreve num `Channel<T>` limitado (bounded) e N workers processam.
- **Pipelines:** o consumer escreve os bytes crus no `PipeWriter`, com enquadramento por prefixo de tamanho; o processamento lê do `PipeReader` e faz o parse sem alocação (`Utf8JsonReader` ou formato binário).

A comparação deve ser apresentada no artigo como "processamento orientado a objetos com backpressure" contra "processamento de bytes com zero-copy", e não como duas formas intercambiáveis da mesma coisa.

### 1c. Baseline

Incluir a variante *Direct* (processamento no próprio callback do consumer, sem mecanismo interno). Sem ela não é possível separar o efeito do broker do efeito do mecanismo de processamento. A matriz passa a ser 2 brokers × 3 modos.

### 1d. Definição de "processamento"

A lógica precisa ser idêntica nas seis variantes e ter custo de CPU realista. A escolha não é arbitrária: os benchmarks consagrados de stream processing convergem para um conjunto pequeno de operações de referência.

| Operação | Onde aparece |
| --- | --- |
| Filtro e projeção | presente em praticamente todos |
| **Agregação em janela, agrupada por chave** | alvo principal do Yahoo Streaming Benchmark; segmentos congestionados no Linear Road |
| **Detecção de padrão sobre eventos consecutivos** | detecção de acidente no Linear Road; *shot on goal* no DEBS 2013 |
| Junção com dado de referência | join anúncio–campanha no YSB |
| Escore preditivo / ML | RIoTBench |

**Decidido:** o `Processing.Core` implementa agregação em janela por carro (média e máxima de velocidade, contagem de eventos) mais detecção de padrão sobre eventos consecutivos do mesmo carro (frenagem forte e troca de marcha). Cobre as duas operações centrais da tabela sem entrar em ML, que deslocaria o gargalo para fora da arquitetura.

Três propriedades que essa escolha garante:

- **Exige ordem por carro**, o que dá sentido à chave de particionamento do Kafka. Se a ordem quebrar, a contagem de frenagens sai errada — o processamento vira detector de defeito no pipeline.
- **Estado proporcional à frota** (uma janela por carro), o que faz o uso de memória ser uma métrica com significado.
- **Determinística**: as seis variantes têm de produzir exatamente os mesmos números para a mesma entrada. O DEBS Grand Challenge avalia as submissões por vazão, latência **e correção do resultado**; a verificação cruzada entre as seis variantes cumpre esse papel aqui.

**Precedente para o domínio:** o DEBS 2013 Grand Challenge usou telemetria esportiva (sensores a 200 Hz nos jogadores e 2000 Hz na bola, cerca de 15 mil eventos/s) como carga de referência para sistemas de processamento de eventos. Isso sustenta o uso de telemetria de F1 como proxy para processamento de alta frequência.

**Relevância prática:** um carro de F1 tem de 150 a 300 sensores amostrando até 100 Hz, e decisões de pit wall são tomadas sobre dados com menos de 50 ms de idade. No mercado de telemetria de frotas, detecção de frenagem brusca e alertas de comportamento são função de produto, não exercício acadêmico.

**Extensão condicionada:** se o experimento piloto mostrar que a agregação é leve demais e as seis variantes empatam, acrescentar custo sintético calibrado (por exemplo 5 e 20 µs por evento) e tratar "intensidade de processamento" como fator explícito. Não fazer antes de o dado pedir.

**Métrica extra sugerida:** o RIoTBench mede também *jitter*, a diferença entre a taxa de saída esperada e a real. O replayer já registra o atraso máximo de emissão, que é a mesma ideia do lado do produtor — vale nomear assim no artigo e citar a fonte.

Referências das fontes citadas nesta seção:

- [A Survey of Stream Processing System Benchmarks (TPCTC 2024)](https://hpi.de/fileadmin/user_upload/fachgebiete/rabl/publications/2024/streamsurvey_tpctc_2024.pdf)
- [DEBS 2013 Grand Challenge — Soccer monitoring](https://debs.org/grand-challenges/2013/)
- [DEBS 2015 Grand Challenge — Taxi trips](https://debs.org/grand-challenges/2015/)
- [RIoTBench: A Real-time IoT Benchmark for Distributed Stream Processing Platforms](https://arxiv.org/abs/1701.08530)
- [ESPBench: The Enterprise Stream Processing Benchmark](https://arxiv.org/pdf/2103.06775)
- [Linear Road: A Stream Data Management Benchmark](https://www.researchgate.net/publication/2949008_Linear_Road_A_Stream_Data_Management_Benchmark)

### 1e. O PostgreSQL pode mascarar as diferenças

Se o banco saturar, todas as arquiteturas parecem iguais.

**Decisão proposta:** persistir em lote com `COPY` binário do Npgsql; medir a latência até o fim do processamento, com a persistência medida em separado; executar também uma bateria com persistência desligada.

### 1f. Equivalência de configuração entre os brokers

Usar a mesma garantia de entrega (at-least-once) nos dois e publicar as configurações numa tabela do artigo.

- Kafka: `acks=all`, `linger.ms` e `batch.size` documentados, partições = número de consumidores.
- RabbitMQ: publisher confirms, `prefetch` definido, tipo de fila (quorum ou classic) documentado.

## 2. Desenho experimental

- **Fatores:** broker (2) × mecanismo (3) × nível de carga (4 a 5).
- **Por execução:** warm-up de 30 a 60 s descartado, janela de medição fixa de 3 a 5 min, no mínimo 10 repetições por combinação, em ordem aleatória.
- **Gerador de carga em malha aberta** (taxa fixa, independente do término do evento anterior), para evitar *coordinated omission* nos percentis.
- **Análise:** projeto fatorial 2^k com replicação (Jain, 1991) para atribuir a variação a cada fator; ANOVA ou Kruskal-Wallis e intervalos de confiança.
- **Ambiente:** hardware documentado, limites de CPU e memória fixos nos containers, execuções sem outros programas ativos. Preferência por máquina Linux dedicada; em Docker Desktop no Windows, registrar a configuração do WSL2.

## 3. Medição

- **Latência:** timestamp gravado na mensagem no momento da publicação, comparado no fim do processamento. Produtor e consumidor rodam na mesma máquina, então os relógios são comparáveis. Registrar em HdrHistogram.
- **Recursos:** `System.Diagnostics.Metrics` com OpenTelemetry nos processos .NET; cAdvisor e Prometheus para todos os containers, inclusive os brokers.
- **Microbenchmarks (opcional):** BenchmarkDotNet para isolar o custo de parse das variantes Channels e Pipelines.

## 4. Riscos

| Risco | Mitigação |
| --- | --- |
| Persistência vira o gargalo | Lote com `COPY`, bateria sem persistência, latência medida antes da escrita |
| Variância do ambiente (Docker Desktop / WSL2) | Repetições, ordem aleatória, limites fixos de recurso, hardware documentado |
| Configurações dos brokers não comparáveis | Garantias de entrega equivalentes e tabela de parâmetros no artigo |
| Implementação demorar e comprimir a janela de experimentos | Reduzir níveis de carga ou repetições, nunca o número de arquiteturas |
| Compressão do Kafka inflada pela repetição do payload | `compression.type=none` nos dois brokers, declarado na tabela de configuração |
| Acúmulo de disco entre rodadas | Retenção de 15 min no Kafka, tópico recriado e tabelas truncadas entre rodadas |

## 5. Sequência de trabalho

Etapas em ordem de dependência, sem datas. O que importa é a ordem: cada etapa destrava a seguinte.

| Etapa | Entrega | Destrava |
| --- | --- | --- |
| A | Dataset coletado e gerador de carga validado | B, D |
| B | Infraestrutura no ar (brokers e banco) | C |
| C | Sinks de Kafka e RabbitMQ no replayer | E |
| D | `Processing.Core` com a lógica idêntica às variantes | E |
| E | Consumers nos três modos (direct, channels, pipelines) | F |
| F | Persistência em lote e coleta de métricas | G |
| G | Script da matriz e experimento piloto | H |
| H | Rodadas oficiais | I |
| I | Análise estatística e gráficos | — |

**Escrita em paralelo:** Fundamentação Teórica e Trabalhos Relacionados vêm do TCC1. A seção de Implementação é escrita junto com o código, para que no fim sobrem apenas Resultados e Discussão.

**Ponto de redução de escopo:** se a etapa E demorar mais que o esperado, cortar em níveis de carga ou repetições — nunca em número de arquiteturas, porque a matriz completa é a contribuição do trabalho.

## 6. Pendências no documento do TCC1

- Nome do orientador truncado ("Prof. Dr. Evandro K."); campo de coorientador vazio.
- Membros da banca ainda com texto de exemplo.
- Legendas saem como "Figure" e "Table" no template SBC.
- Atualizar as datas de "Acesso em: maio 2025" nas referências.
- Atualizar a Figura 1 e os objetivos específicos para incluir a baseline e o replayer.
