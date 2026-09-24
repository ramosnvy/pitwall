# Aprofundamento: o mecanismo interno além do uso padrão

**Situação:** proposta. Nada aqui foi medido ainda. As hipóteses estão escritas antes da medição, de propósito: o resultado deve confirmar ou refutar algo declarado, e não ser explicado depois.

A matriz oficial (`7e283c2`, [RESULTADOS.md](RESULTADOS.md)) passa a ser tratada como **uso padrão** dos mecanismos. Este documento propõe medir o **uso ajustado**: as camadas de Channels e Pipelines que a implementação atual não explora.

## 1. Por que aprofundar

A matriz concluiu que, sob processamento barato e uniforme, desacoplar a busca do processamento não muda a latência e custa CPU. No Kafka a 200 mil ev/s, o consumidor usa 58% de um núcleo no Direct, 131% no Channels e 141% no Pipelines.

A diferença por evento é:

```
Channels:  (131% − 58%) × 1 núcleo / 200.000 ev/s ≈ 3,6 µs de CPU por evento
Pipelines: (141% − 58%) × 1 núcleo / 200.000 ev/s ≈ 4,1 µs de CPU por evento
```

O processamento em si custa ~0,12 µs. Os ~3,6 µs extras são da ordem de grandeza de acordar uma thread e trocar de contexto ([Li, Ding e Shen, 2007](https://dl.acm.org/doi/10.1145/1281700.1281702)). Não são o custo de pôr um item numa fila: com `SingleReader` e `SingleWriter`, o Channels lê sem lock e sem operação interlocked ([Toub, 2019](https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/)).

O uso atual provoca exatamente esse custo. A 200 mil ev/s, cada uma das 4 faixas recebe um evento a cada 20 µs. O worker processa em menos de 1 µs, esvazia o canal e dorme, e o evento seguinte o acorda de novo. No Pipelines, o `FlushAsync` a cada mensagem sinaliza o leitor a cada 46 bytes. Fowler descreve o Pipelines como uma estrutura que cria "a batching effect that lets the parsing logic consume larger chunks of buffers" ([Fowler, 2018](https://devblogs.microsoft.com/dotnet/system-io-pipelines-high-performance-io-in-net/)), e o flush por mensagem anula esse efeito.

**Consequência para o texto:** "desacoplar é puro custo" pode ser uma propriedade do uso ingênuo, não das ferramentas. Enquanto isso não for medido, a conclusão da matriz vale só para o uso padrão. Isso está registrado em [AMEACAS-VALIDADE.md](AMEACAS-VALIDADE.md).

## 2. As camadas que o uso atual não explora

| Camada | Channels | Pipelines | Uso atual |
| --- | --- | --- | --- |
| **Lote** | ler com `WaitToReadAsync` e esvaziar com `TryRead` em laço; o escritor pode acumular antes de publicar | `FlushAsync` a cada N registros ou a cada T µs; um `ReadAsync` devolve vários registros | um evento por despertar; flush a cada mensagem |
| **Escalonamento** | `AllowSynchronousContinuations` | `readerScheduler` e `writerScheduler` (`PipeScheduler.ThreadPool` ou `Inline`) | padrão: continuação no ThreadPool |
| **Topologia** | vários leitores por canal; canais por chave; estágios encadeados | um leitor por pipe; estágios encadeados | um canal e um worker por faixa |
| **Contrapressão** | capacidade; `BoundedChannelFullMode` | `pauseWriterThreshold` e `resumeWriterThreshold` | 10 mil eventos por faixa; limite equivalente em bytes |
| **Memória** | struct por item, sem alocação | `MemoryPool`, `minimumSegmentSize`, escrita direta com `GetSpan` | cópia do payload para o pipe |

Os modos `DropNewest`, `DropOldest` e `DropWrite` ficam de fora: descartar evento quebra o at-least-once e invalida o digest.

O Pipelines foi feito para ler de um socket ([Microsoft Learn](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines)). Num consumidor de broker ele não chega ao socket, porque o cliente (librdkafka ou RabbitMQ.Client) entrega a mensagem já montada. Isso limita o que o Pipelines pode render nesta arquitetura. Não é defeito da implementação: é um achado sobre compor Pipelines com um broker.

## 2.1 Fundamentação teórica

As perguntas da §3 se apoiam em cinco corpos de teoria. Cada um prevê algo que pode ser medido.

**Teoria de filas.**
- **Lei de Little** (L = λW; [Little, 1961](https://pubsonline.informs.org/doi/abs/10.1287/opre.9.3.383)): relaciona ocupação, taxa e tempo de permanência, e fundamenta a Q4.
- **Fórmula de Kingman** (aproximação do tempo de espera numa fila G/G/1; [Kingman, 1961](https://en.wikipedia.org/wiki/Kingman%27s_formula)). A espera cresce com o fator de utilização ρ/(1−ρ) e com a variabilidade das chegadas e do atendimento. Isso explica por que um evento lento raro pesa tanto: ele aumenta muito a variância do tempo de atendimento sem quase mudar a média.
- **Filas com atendimento em lote** ([Bailey, 1954](https://rss.onlinelibrary.wiley.com/doi/10.1111/j.2517-6161.1954.tb00149.x)): modelam o que a Q1 propõe, com vários itens atendidos de uma vez até um tamanho máximo.
- **Estágios ligados por filas** (SEDA; [Welsh, Culler e Brewer, 2001](https://www.sosp.org/2001/papers/welsh.pdf)): o Channels e o Pipelines, como usados aqui, são uma rede de dois estágios em série.

**Esperar girando ou dormindo.** Uma thread sem trabalho pode girar em espera ativa (acorda rápido, mas queima CPU) ou bloquear (não gasta CPU, mas paga o custo de acordar).
- Ousterhout (1982) propôs a espera em duas fases: girar um pouco e depois bloquear.
- [Karlin et al. (1991)](https://doi.org/10.1145/121133.286599) compararam sete estratégias, incluindo algumas que ajustam o tempo de giro pela experiência passada.
- O ThreadPool do .NET implementa essa espera em duas fases, com um controlador de concorrência por *hill climbing* ([Hellerstein, Morrison e Eilebrecht, 2008](https://www.researchgate.net/publication/228977836_Optimizing_concurrency_levels_in_the_net_threadpool_A_case_study_of_controller_design_and_implementation)).
- É essa teoria que separa as duas explicações concorrentes do excesso de CPU na Q1.

**Escalabilidade com mais workers.** A Universal Scalability Law de [Gunther](https://www.perfdynamics.com/Manifesto/USLscalability.html) estende a lei de Amdahl com um termo de coerência. A vazão com N workers cresce, satura e pode *cair*, por disputa de recurso compartilhado (α) e pelo custo de manter estado coerente (β). Ela prevê a forma da curva da Q3 quando K passa do número de núcleos.

**Contrapressão por demanda.** Na especificação [Reactive Streams](https://github.com/reactive-streams/reactive-streams-jvm), o consumidor sinaliza quantos elementos aceita (`request(n)`), e isso limita o buffer. O controle de fluxo por créditos do Flink segue o mesmo princípio ([Kruber, 2019](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/)). Nos nossos mecanismos, o análogo é o limite do canal ou do pipe (Q4).

**Cauda e medição.**
- [Dean e Barroso (2013)](https://cacm.acm.org/research/the-tail-at-scale/) mostram que soluços raros dominam a cauda em escala, e que eliminar toda fonte de variabilidade é impraticável. Por isso a Q3 mede P99 e P99,9, e não a média.
- A *coordinated omission* ([Tene, 2013](https://www.youtube.com/watch?v=lJ8ydIuPFeU)) já é evitada no gerador em malha aberta, e o microbenchmark do nível 1 precisa seguir o mesmo princípio.

### Leitura teórica dos resultados atuais

*Interpretação, não medição.* Pela fórmula de Kingman, a cauda do RabbitMQ perto do teto de ~60 mil ev/s é o esperado. Tomando ρ ≈ carga/60 mil, o fator ρ/(1−ρ) vai de ~0,2 a 10 mil ev/s para ~2 a 40 mil, um crescimento de ~10×. O P99 medido cresceu ~6× no mesmo intervalo (de 1,4 para 7,5 a 9,7 ms). Kingman descreve a espera média e não o P99, então a comparação é só qualitativa.

O Kafka, com teto acima de 200 mil ev/s, opera sempre com ρ baixo. Isso é coerente com o P99 plano. A diferença de cauda entre os brokers seria, em boa parte, a distância de cada um até o próprio teto. É uma hipótese testável: repetir o RabbitMQ com 8 filas (RESULTADOS §9) deveria afastar o teto e achatar a cauda na mesma carga.

### O que relatam os praticantes

Discussões públicas não são evidência experimental, mas mostram onde os problemas aparecem na prática.
- **Reddit:** não foi possível consultar, porque o site bloqueia o acesso automatizado desta pesquisa.
- **Hacker News:** as discussões encontradas tratam do uso do Channels, sem dados de desempenho.
- **Issues do dotnet/runtime:** foi a fonte mais útil. Os mantenedores discutem os mecanismos ali, com números.

- **Espera ativa do ThreadPool.** Kouvel, do time de runtime do .NET, registrou que o pool "allows too many threads to spin-wait simultaneously, which in some scenarios causes higher CPU usage when similar performance can be achieved with fewer spin-waiters and less CPU". Em containers, o *hill climbing* pode manter girando mais threads do que há núcleos ([#93234](https://github.com/dotnet/runtime/issues/93234)).
  - O problema aparece quando o trabalho chega em rajadas e a fila esvazia entre elas ([coreclr #5928](https://github.com/dotnet/coreclr/issues/5928)). É o nosso regime: um evento a cada 20 µs por faixa.
  - Num servidor web mínimo, `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0` cortou a CPU pela metade, com vazão parecida ([dotnet-async-nightmare](https://github.com/pwrdrvr/dotnet-async-nightmare)).
  - Em outro relato, a queda foi de ~25% ([#103812](https://github.com/dotnet/runtime/issues/103812)).
  - Segundo o mantenedor, mudanças posteriores encurtaram a espera e acrescentaram realimentação, então o efeito no .NET 10 pode ser menor. Precisa ser medido.
- **Continuações síncronas em saturação.** Na [#111486](https://github.com/dotnet/runtime/issues/111486), `AllowSynchronousContinuations = true` foi *mais lento*: 121 ms contra 44 ms para 1 milhão de itens num canal sem limite. A documentação da opção promete o contrário. A issue não teve resposta dos mantenedores. Nossa leitura: com a continuação na thread do escritor, produtor e consumidor passam a rodar em série, e perde-se o paralelismo de pipeline.
- **Continuação síncrona "quase de graça".** Uma discussão no repositório de diagnósticos de Fowler argumenta que continuar na mesma thread é uma chamada direta, enquanto mandar para o pool tem custo mensurável ([AspNetCoreDiagnosticScenarios #72](https://github.com/davidfowl/AspNetCoreDiagnosticScenarios/issues/72)). As duas fontes sobre continuações não se contradizem: descrevem regimes diferentes, e isso vira hipótese na Q2.
- **O pool domina o desempenho do Channels.** No Akka.NET, o executor construído sobre Channels teve ganhos "massive" só com `ThreadPool.SetMinThreads(0,0)` ([akka.net #4882](https://github.com/akkadotnet/akka.net/pull/4882)). A configuração do pool é uma variável de confusão e precisa ser fixada e registrada.
- **Bloqueio na cabeça da fila em produção.** A Uber construiu um proxy de consumo para Kafka, o uForwarder, em parte para contornar o bloqueio na cabeça da fila, com um rastreador de commits fora de ordem e uma fila de mensagens mortas ([Uber, uForwarder](https://www.uber.com/us/en/blog/introducing-ufowarder/)). É evidência industrial de que o problema da Q3 é real.

## 3. Perguntas e hipóteses

Cada pergunta corresponde a uma decisão que um desenvolvedor toma ao compor um broker com um mecanismo interno.

### Q1. Lote: o custo do desacoplamento vem de despertar uma thread por evento?

**Por que importa:** decide se Channels e Pipelines são caros por natureza ou só quando usados evento a evento.

**Métrica nova:** *eventos por despertar*, o número médio de eventos que o worker processa cada vez que acorda.

**Hipóteses:**

- **H1a.** O excesso de CPU em relação ao Direct é proporcional ao número de despertares. Com um evento por despertar, reproduz os ~3,6 µs por evento.
- **H1b.** Só mudar a leitura para `WaitToReadAsync` + `TryRead` quase não muda nada entre 10 e 200 mil ev/s: o canal raramente tem mais de um item quando o worker acorda. Previsão: menos de 1,5 evento por despertar abaixo da saturação.
- **H1c.** Acumular no escritor com lote fixo de N eventos reduz o excesso de CPU aproximadamente na proporção de 1/N. O custo é latência: até N/λ por faixa, sendo λ a taxa de eventos por faixa. Com N = 64 a 5 mil ev/s por faixa, seriam ~12,8 ms. Por isso, um lote fixo precisa de limite de tempo, como o `buffer-timeout` do Flink, onde "you cannot get both" latência mínima e vazão máxima ([Kruber, 2019](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/)).
- **H1d.** O *smart batching* publica assim que há dado e agrupa o que chegou enquanto o envio anterior acontecia ([Thompson, 2011](https://mechanical-sympathy.blogspot.com/2011/10/smart-batching.html)). Ele mantém a latência do uso atual em carga baixa e reduz a CPU em carga alta, porque o lote cresce sozinho com a carga. Thompson relata latência *menor* com lote bem feito.

- **H1e (concorrente de H1a).** O excesso de CPU não vem de trocar de contexto, e sim da **espera ativa** dos workers do ThreadPool entre um evento e outro (§2.1, *O que relatam os praticantes*). As duas hipóteses fazem previsões diferentes:
  - **Se for espera ativa:** com `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0`, o excesso de CPU cai muito e a latência sobe pouco, pelo custo de acordar uma thread bloqueada.
  - **Se for troca de contexto:** a opção quase não muda a CPU.

  O teste custa uma variável de ambiente, e por isso vem **antes** de qualquer variante de lote. A espera ativa também muda a leitura da própria matriz: CPU consumida em espera ativa não é trabalho útil. O consumidor gasta a cota de 3 núcleos do container, mas cederia esse tempo se houvesse outra tarefa para rodar.

**Onde está o limite natural do lote:** no Kafka, o laço de consumo pode drenar a fila local da librdkafka até ela ficar vazia antes de publicar no canal ou dar o flush no pipe. No RabbitMQ, as entregas chegam em rajadas limitadas pelo prefetch.

**Como refutar:** se o microbenchmark (§4) não reproduzir o excesso de ~3,6 µs com um evento por despertar, a causa não é o mecanismo e Q1 para ali.

### Q2. Escalonamento: onde roda a continuação do worker?

**Por que importa:** as opções que tiram a troca de thread podem desfazer o desacoplamento sem nenhum aviso.

- **H2a.** Com `AllowSynchronousContinuations = true` ou `PipeScheduler.Inline`, a continuação do leitor roda na thread do broker sempre que o leitor estiver esperando. Previsão: CPU e latência a menos de 10% do Direct com processamento barato, e o mesmo bloqueio na cabeça da fila do Direct sob evento lento (Q3). Na prática, o mecanismo vira Direct.
  - Toub avisa que a opção "can significantly cut down on overheads, but also requires great understanding of the environment".
  - A documentação do Pipelines diz que `Inline` "can cause unintended consequences such as deadlocks".
  - O StackExchange.Redis chama de *thread theft* o efeito de a thread de leitura ser sequestrada para rodar lógica da aplicação, e se protege dele ([Thread Theft](https://stackexchange.github.io/StackExchange.Redis/ThreadTheft.html)).
  - O Direct é *thread theft* deliberado. Esta hipótese testa se as opções de desempenho o reintroduzem.
- **H2c.** O efeito da continuação síncrona depende do regime, o que concilia as duas fontes da §2.1:
  - **Abaixo da saturação**, o leitor está quase sempre esperando, e rodar a continuação na mesma thread evita o custo de acordá-lo. Deve reduzir CPU.
  - **Em saturação**, produtor e consumidor passam a rodar em série, e perde-se o paralelismo de pipeline. Deve reduzir a vazão, como na [#111486](https://github.com/dotnet/runtime/issues/111486).
  - Previsão: o sinal do efeito se inverte entre 20 mil e 200 mil ev/s.
- **H2b.** Workers em threads dedicadas (`LongRunning`) em vez do ThreadPool reduzem os picos isolados de cauda: rodadas com P99 de ~196 ms contra ~5,7 ms nas demais repetições (RESULTADOS §1). O esgotamento do pool já apareceu uma vez neste projeto (REVISAO-TECNICA §1.9).

### Q3. Topologia: quando dividir a faixa por carro compensa?

**Por que importa:** é a decisão de ir além do paralelismo que o broker impõe. O processamento precisa de ordem **por carro**, não por partição. O Kafka dá ordem por partição, que é mais forte do que o necessário e custa bloqueio na cabeça da fila: um carro lento segura os carros não relacionados da mesma partição.
- O Confluent Parallel Consumer resolve isso no Java com o modo `KEY`, em que "messageKey becomes the basic unit of concurrency, and you can go beyond the number of partitions while still maintaining FIFO order" ([Confluent](https://www.confluent.io/blog/introducing-confluent-parallel-message-processing-client/)).
- O argumento aparece também em [Derosiaux (2026)](https://www.conduktor.io/blog/kafka-partitions-are-the-wrong-ordering-abstraction-keys-are).

**Variante nova:** Channels com K workers por faixa, cada um com um `TelemetryProcessor` próprio e o carro roteado por `carro % K`. A ordem por carro se mantém, então **o digest tem de continuar igual**, e isso testa a corretude da topologia nova.

**Modelo:** o evento lento acontece com probabilidade p e custa uma espera de S segundos, simulando I/O. Seja λ a taxa por faixa e K o número de workers por faixa. A fração de eventos atrasados por essa espera é aproximadamente:

```
f ≈ p × (λ / K) × S
```

Com p = 1/10.000 e S = 50 ms:

| Carga total | λ por faixa | K = 1 | K = 4 | K = 8 |
| --- | --- | --- | --- | --- |
| 20 mil ev/s | 5 mil | 2,5% | 0,63% | 0,31% |
| 40 mil ev/s | 10 mil | 5,0% | 1,25% | 0,63% |

- **H3a.** O P99 fica na faixa dos ~50 ms quando f > 1% e volta perto do valor sem evento lento quando f < 1%. O P99,9 continua afetado em todos os casos. O modelo ignora o tempo de recuperação da fila depois da espera, que é curto com processamento de ~0,12 µs.
- **H3b.** Direct e Channels com K = 1 degradam **igualmente**. Isso corrige a previsão do REVISAO-TECNICA §4.2, que esperava que o Channels absorvesse o evento lento. Com um worker por faixa, o canal só muda onde os eventos esperam, não quanto.
- **H3c.** Com processamento barato e sem evento lento, K > 1 só acrescenta CPU (mais despertares, pela Q1) e não reduz a latência.
- **H3d.** Com custo de CPU uniforme, e não espera, o ganho de K para no limite de 3 núcleos do consumidor. Pela Universal Scalability Law, a vazão pode até cair acima disso, pela disputa dos workers pelo ThreadPool e pela cota de CPU (α > 0). O ajuste da curva de vazão × K dá α e β, e isso descreve a composição com dois números em vez de um gráfico.

Duas referências de apoio:
- **SEDA:** [Welsh, Culler e Brewer (2001)](https://www.sosp.org/2001/papers/welsh.pdf) organizam serviços em estágios ligados por filas, cada um com seu pool de threads e controle dinâmico, incluindo lote.
- **Paralelismo de pipeline:** [Prasaad, Ramalingam e Rajan (2018)](https://arxiv.org/abs/1803.11328) observam que heurísticas que exploram paralelismo de pipeline superam as que buscam paralelismo de dados em processamento ordenado. O Channels atual já é um pipeline de dois estágios: a desserialização roda na thread do broker e o processamento no worker.

### Q4. Contrapressão: o tamanho do buffer muda a latência ou só onde o acúmulo fica?

**Por que importa:** decide o tamanho do buffer e se o acúmulo deve ficar no broker ou no processo.

- **H4a.** Pela lei de Little (L = λW; [Little, 1961](https://pubsonline.informs.org/doi/abs/10.1287/opre.9.3.383)), em regime estável o limite do buffer não muda a latência média enquanto não é atingido. Previsão: capacidade de 100 e de 10.000 eventos por faixa com diferença de P99 menor que 10% sem evento lento.
- **H4b.** Sob evento lento, um buffer pequeno empurra o acúmulo de volta para o broker. No Kafka, isso aparece como lag. No RabbitMQ, como mensagens prontas na fila, porque o prefetch limita as não confirmadas. A latência de ponta a ponta é a mesma; mudam a memória do consumidor e o estado visível no broker. É o mesmo argumento do controle de fluxo por créditos do Flink: bufferizar menos entre emissor e receptor torna a contrapressão mais imediata ([Kruber, 2019](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/)).

## 4. Método

### Nível 1: o mecanismo isolado, sem broker

Ferramenta nova, `tools/mechanism-bench`. Gera eventos na memória e os entrega a cada configuração pelo mesmo `IProcessingPipeline`, com o mesmo `Processing.Core`. Dois modos, porque cada um esconde o que o outro mostra:

- **Saturado, com BenchmarkDotNet:** custo por evento e bytes alocados por evento. Esconde o custo de despertar, porque na saturação o leitor nunca dorme.
- **Com ritmo fixo, em malha aberta:** o mesmo princípio do replayer, a 1.250, 5.000 e 50.000 eventos por segundo por faixa (5, 20 e 200 mil no total). Mede:
  - CPU por evento (`Process.TotalProcessorTime`);
  - latência de entrega em HdrHistogram;
  - eventos por despertar;
  - alocação.

  É o modo que reproduz o regime da matriz.

**Configuração do ThreadPool como fator explícito.** Pelos relatos da §2.1, a espera ativa do pool pode dominar o custo do Channels. O limite de espera ativa entra como fator nos dois níveis: padrão e `DOTNET_ThreadPool_UnfairSemaphoreSpinLimit=0`. Toda rodada registra as variáveis `DOTNET_*` do container, e a matriz oficial rodou com os valores padrão.

**Portão:** o nível 1 tem de reproduzir, no modo com ritmo, o excesso de ~3,6 µs por evento do Channels sobre o Direct a 50 mil eventos por segundo por faixa. Se não reproduzir, a diferença da matriz não vem do mecanismo, e a investigação muda de direção antes de qualquer outra medição.

O nível 1 é barato, então a triagem pode ser um fatorial completo de 2 níveis nos seis fatores: espera ativa do pool, lote, escalonamento, topologia, capacidade e custo do processamento. São 64 configurações × 3 taxas.

### Nível 2: no sistema

Só as configurações cujo efeito no nível 1 passar do limiar de relevância viram modos novos do consumidor. Elas rodam no mesmo protocolo da matriz: containers, ordem aleatória, 5 repetições, aquecimento descartado, validade por jitter e por digest.

- **Kafka primeiro**, pela cauda estável e pelo teto alto: 20 e 200 mil ev/s para Q1 e Q2; 20 e 40 mil ev/s com evento lento para Q3 e Q4.
- **RabbitMQ para confirmar** os achados principais, a 20 e 40 mil ev/s.
- Se houver mais de três fatores relevantes, fatorial fracionado 2^(k−p) (Jain, 1991, cap. 19) em vez do completo.

**Limiar de relevância**, o mesmo de AMEACAS-VALIDADE.md: 10% de CPU ou 20% de P99. O texto reporta o tamanho do efeito junto com o teste estatístico.

**Métricas novas no nível 2:**
- eventos por despertar;
- lag do consumidor no Kafka (hoje não é coletado);
- profundidade da fila no RabbitMQ (já coletada pelo Prometheus).

## 5. Escopo e limites

- **Dentro do TCC1:** o TCC1 descreve o módulo de processamento como responsável pelo "processamento concorrente dos eventos" com Channels ou Pipelines. Aprofundar como esses mecanismos são usados é estudar o objeto do trabalho, não ampliar o escopo.
- **Sem bibliotecas novas no experimento:** TPL Dataflow, o Disruptor ([Thompson et al., 2011](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)) e o Parallel Consumer entram como trabalhos relacionados, não como variantes.
- **A matriz oficial não é refeita:** ela é o uso padrão.
- **Regra de parada:** fator sem efeito acima do limiar no nível 1 não vai para o nível 2.
- **Decisão com o orientador:** este documento amplia o que a matriz mede e deve ser levado à reunião junto com REUNIAO-ORIENTADOR.md.

## 6. O que muda no texto

A seção de resultados passa a ter duas partes:
- **Uso padrão:** a matriz atual.
- **Uso ajustado:** este aprofundamento.

A conclusão vira uma tabela de decisão: para cada condição de carga e de custo de processamento, qual mecanismo usar, com qual configuração e a que custo. É o trade-off que o TCC1 pede para identificar, com as condições em que cada escolha vale.

## 7. Sequência

| Etapa | Entrega | Destrava |
| --- | --- | --- |
| A | `tools/mechanism-bench` com as configurações atuais; portão de reprodução; H1a contra H1e (espera ativa do pool) | B |
| B | Q1 no nível 1 | C |
| C | Triagem de Q2, Q3 e Q4 no nível 1 | D |
| D | Configurações relevantes como modos do consumidor; coleta do lag do Kafka | E |
| E | Rodadas do nível 2 | F |
| F | Tabela de decisão e texto | — |

## Referências

- Toub, S. [An Introduction to System.Threading.Channels](https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/). .NET Blog, 2019.
- Fowler, D. [System.IO.Pipelines: High performance IO in .NET](https://devblogs.microsoft.com/dotnet/system-io-pipelines-high-performance-io-in-net/). .NET Blog, 2018.
- Microsoft. [System.IO.Pipelines](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines). Microsoft Learn.
- Gravell, M. [Pipe Dreams](https://blog.marcgravell.com/2018/07/pipe-dreams-part-1.html), série sobre a migração do StackExchange.Redis para Pipelines, 2018; e [Thread Theft](https://stackexchange.github.io/StackExchange.Redis/ThreadTheft.html), documentação do StackExchange.Redis.
- Thompson, M. [Smart Batching](https://mechanical-sympathy.blogspot.com/2011/10/smart-batching.html). Mechanical Sympathy, 2011.
- Thompson, M.; Farley, D.; Barker, M.; Gee, P.; Stewart, A. [Disruptor: High performance alternative to bounded queues for exchanging data between concurrent threads](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf). LMAX, 2011.
- Welsh, M.; Culler, D.; Brewer, E. [SEDA: An Architecture for Well-Conditioned, Scalable Internet Services](https://www.sosp.org/2001/papers/welsh.pdf). SOSP, 2001.
- Prasaad, G.; Ramalingam, G.; Rajan, K. [Scaling Ordered Stream Processing on Shared-Memory Multicores](https://arxiv.org/abs/1803.11328). arXiv, 2018.
- Kruber, N. [A Deep-Dive into Flink's Network Stack](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/). Apache Flink Blog, 2019.
- Karimov, J. et al. [Benchmarking Distributed Stream Data Processing Systems](https://dblp.org/rec/conf/icde/KarimovRKSHM18.html). ICDE, 2018.
- Gencer, C. et al. [Hazelcast Jet: Low-latency Stream Processing at the 99.99th Percentile](https://arxiv.org/abs/2103.10169). VLDB, 2021.
- Confluent. [Introducing Confluent's Parallel Consumer Message Processing Client](https://www.confluent.io/blog/introducing-confluent-parallel-message-processing-client/).
- Derosiaux, S. [Kafka Partitions are the wrong ordering abstraction. Keys are.](https://www.conduktor.io/blog/kafka-partitions-are-the-wrong-ordering-abstraction-keys-are) Conduktor, 2026.
- Li, C.; Ding, C.; Shen, K. [Quantifying the cost of context switch](https://dl.acm.org/doi/10.1145/1281700.1281702). ExpCS, 2007.
- Little, J. D. C. [A Proof for the Queuing Formula: L = λW](https://pubsonline.informs.org/doi/abs/10.1287/opre.9.3.383). Operations Research, 9(3), 1961.
- Jain, R. *The Art of Computer Systems Performance Analysis*. Wiley, 1991.

Teoria (§2.1):

- Kingman, J. F. C. The single server queue in heavy traffic. *Mathematical Proceedings of the Cambridge Philosophical Society*, 57(4), 1961. Resumo em [Kingman's formula](https://en.wikipedia.org/wiki/Kingman%27s_formula).
- Bailey, N. T. J. [On Queueing Processes with Bulk Service](https://rss.onlinelibrary.wiley.com/doi/10.1111/j.2517-6161.1954.tb00149.x). *Journal of the Royal Statistical Society B*, 16(1), 1954.
- Ousterhout, J. K. Scheduling techniques for concurrent systems. *ICDCS*, 1982.
- Karlin, A. R.; Li, K.; Manasse, M. S.; Owicki, S. [Empirical studies of competitive spinning for a shared-memory multiprocessor](https://doi.org/10.1145/121133.286599). *SOSP*, 1991.
- Hellerstein, J. L.; Morrison, V.; Eilebrecht, E. [Optimizing concurrency levels in the .NET ThreadPool: a case study of controller design and implementation](https://www.researchgate.net/publication/228977836_Optimizing_concurrency_levels_in_the_net_threadpool_A_case_study_of_controller_design_and_implementation). 2008.
- Gunther, N. J. [How to Quantify Scalability: the Universal Scalability Law](https://www.perfdynamics.com/Manifesto/USLscalability.html). Performance Dynamics.
- [Reactive Streams Specification for the JVM](https://github.com/reactive-streams/reactive-streams-jvm).
- Dean, J.; Barroso, L. A. [The Tail at Scale](https://cacm.acm.org/research/the-tail-at-scale/). *Communications of the ACM*, 56(2), 2013.
- Tene, G. [How NOT to Measure Latency](https://www.youtube.com/watch?v=lJ8ydIuPFeU). Palestra, 2013.

Relatos de praticantes (§2.1):

- dotnet/runtime [#93234](https://github.com/dotnet/runtime/issues/93234) (limite de threads em espera ativa no pool), [#103812](https://github.com/dotnet/runtime/issues/103812) (uso de CPU do pool com carga em rajadas), [#111486](https://github.com/dotnet/runtime/issues/111486) (`AllowSynchronousContinuations` reduzindo a vazão); dotnet/coreclr [#5928](https://github.com/dotnet/coreclr/issues/5928) (CPU alta na espera ativa do pool).
- pwrdrvr. [dotnet-async-nightmare](https://github.com/pwrdrvr/dotnet-async-nightmare): CPU de um servidor mínimo com e sem espera ativa.
- davidfowl/AspNetCoreDiagnosticScenarios [#72](https://github.com/davidfowl/AspNetCoreDiagnosticScenarios/issues/72): custo de continuações assíncronas.
- akkadotnet/akka.net [#4882](https://github.com/akkadotnet/akka.net/pull/4882): executor do Akka.NET sobre Channels.
- Uber. [Introducing uFowarder: The Consumer Proxy for Kafka Async Queuing](https://www.uber.com/us/en/blog/introducing-ufowarder/).
