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

**Decidido depois da matriz `7e283c2`: 100 Hz por carro no cenário principal.** Cada carro é reamostrado de 3,7 para 100 Hz por interpolação (`TelemetryInterpolator`, opção `--hz` do replayer), sobre um trecho da corrida (`--window-start`).
- **Canais:** velocidade, rotação e acelerador são interpolados em linha reta; marcha, freio e DRS ficam em degrau. Por construção, e verificado em teste, as frenagens e trocas de marcha detectadas são as mesmas da corrida medida.
- **Por quê:** a cadência por carro fica compatível com a telemetria real de F1, e a vazão passa a vir de uma frota plausível, com 1.000 carros a 100 mil ev/s. Na mesma vazão, o banco grava 27× menos janelas; a 400 mil ev/s, na matriz, era ele o gargalo, e passaria a mascarar a comparação dos brokers (1e).
- **Sensibilidade:** a matriz de 3,7 Hz, com o dado exatamente como medido, vira análise de sensibilidade. Para comparar as frequências sem misturar protocolos, uma bateria a 3,7 Hz no protocolo novo, em duas cargas.
- **No texto:** os pontos interpolados não são medidos, e isso precisa estar escrito (AMEACAS-VALIDADE).

**Decidido depois do experimento 2×2 (IMPLEMENTACAO §7): núcleos exclusivos por container.** Produtor nos núcleos 0-3, broker em 4-7, consumidor em 8-10, e banco e coleta de métricas no 11, sem cota. É a mesma capacidade do protocolo anterior, sem estrangulamento por cota e sem vizinhos. Só o broker medido fica no ar, e o painel fica desligado. As faixas do RabbitMQ usam CRC32, a mesma função do Kafka.

### 1b. Channels e Pipelines não são equivalentes

`System.Threading.Channels` é uma fila produtor-consumidor de **objetos**. `System.IO.Pipelines` opera sobre **fluxos de bytes**.

**Decisão proposta:**
- **Channels:** o consumer desserializa a mensagem para objeto, escreve num `Channel<T>` limitado (bounded) e N workers processam.
- **Pipelines:** o consumer escreve os bytes crus no `PipeWriter`, com enquadramento por prefixo de tamanho; o processamento lê do `PipeReader` e faz o parse sem alocação (`Utf8JsonReader` ou formato binário).

A comparação deve ser apresentada no artigo como "processamento orientado a objetos com backpressure" contra "processamento de bytes com zero-copy", e não como duas formas intercambiáveis da mesma coisa.

### 1c. Baseline

**Decidido:** incluir a variante *Direct* — processamento no próprio callback do consumer, sem mecanismo interno. A matriz passa a ser 2 brokers × 3 modos.

**Por que acrescentar algo que o TCC1 não previa.** A *Direct* não é uma quarta arquitetura: é o **grupo de controle** do experimento. Sem ela, um resultado como "Kafka + Channels entrega 80 mil ev/s contra 60 mil do RabbitMQ + Channels" permite concluir que o broker importa, mas não responde se o Channels ajudou ou atrapalhou — o Kafka sozinho poderia entregar 95 mil, e o Channels estar custando 15 mil.

O título do trabalho promete avaliar *arquiteturas compostas*. Medir a composição exige medir também o não-composto. Sem o controle, o trabalho mede a diferença entre dois brokers com um mecanismo por cima, e não o efeito do mecanismo.

Custo: duas células a mais na matriz, e são as duas mais simples de implementar das seis.

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

- **Fatores:** broker (2) × mecanismo (3) × nível de carga.
- **Os níveis de carga saem do piloto, não são fixados antes.** Primeiro uma varredura crescente até a latência disparar em cada uma das seis variantes — é assim que se descobre o ponto de saturação de cada uma. Só depois se escolhem os níveis oficiais, de modo que pelo menos um fique acima do joelho da curva de cada arquitetura. Fixar 100 mil ev/s de antemão seria arbitrário: se o RabbitMQ saturar em 40 mil, medir a 100 mil só produz gráfico de sistema quebrado. Para referência, o DEBS 2013 operou em torno de 15 mil ev/s.
- **Tamanho da mensagem em projeto fatorial fracionado:** avaliado apenas em duas cargas (uma intermediária e a de saturação), não nas quatro. O fatorial completo dobraria o tempo de máquina sem dobrar a informação (Jain, 1991).
- **Por execução:** warm-up descartado, janela de medição fixa e repetições em ordem aleatória. Dimensionamento em *Orçamento de tempo de máquina*, abaixo.

### Orçamento de tempo de máquina

O tempo de experimento é tempo real e não pode ser acelerado: latência em milissegundos e vazão por segundo **são** medidas de relógio de parede. Acelerar o gerador não encurta o experimento — aumenta a carga, que é outro experimento. Os ajustes legítimos são outros:

| Alavanca | Efeito |
| --- | --- |
| Janela de medição curta | A precisão estatística vem do número de amostras, não da duração. A 50 mil ev/s, 90 s já são 4,5 milhões de eventos — muito além do necessário para P99 |
| Repetições dimensionadas | Rodar 3 repetições piloto, medir o coeficiente de variação e calcular quantas bastam para o intervalo de confiança desejado, em vez de fixar 10 |
| Warm-up por estado estável | Encerrar o aquecimento quando a vazão estabiliza, em vez de esperar um tempo fixo |
| Fatorial fracionado | Tamanho de mensagem só em duas cargas |
| Execução desatendida | Rodar a matriz de madrugada: tempo de máquina não é tempo de pessoa |

Com 5 repetições, 90 s de medição e 30 s de aquecimento e limpeza, a matriz base cai de cerca de 24 h para **cerca de 5 h**.

**Contrapartida a cobrir:** janelas curtas podem esconder efeitos lentos — pausas de GC, rotação de segmento no Kafka, descarga de cache de página. Mitigação: uma execução longa (10 a 15 min) por arquitetura, na carga de saturação, para confirmar que as janelas curtas não escondem deriva.
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
