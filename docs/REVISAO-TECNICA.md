# Revisão técnica antes da matriz oficial

Revisão de código, testes e decisões arquiteturais feita contra a documentação dos fornecedores e contra o escopo do TCC1, antes e durante a execução da matriz oficial.

**Resultado:** oito lacunas corrigidas antes da matriz (commit `25fa684`), seis achados que não comprometem a matriz em curso e ficam para depois, e uma suíte de 24 testes automatizados que o projeto não tinha. A lacuna mais grave encontrada não estava no código do experimento, e sim no sistema operacional: o timer do Windows penalizava o Kafka em duas de cada três rodadas.

## 1. Corrigido antes da matriz

Cada item abaixo teria contaminado as ~6 h de execução.

### 1.1 Latência bimodal do Kafka no Windows — a mais grave, e resolvida só depois da matriz

**Sintoma.** O Kafka apresentava latência **bimodal**: a mesma configuração caía, por rodada inteira, ora num regime de 3,4 ms de média, ora num de 25,4 ms, com P99 saltando de 6 para 49 ms. As rodadas lentas têm números quase idênticos entre si (média 25,4, P50 25,4, P95 45,4, P99 49) — a assinatura de eventos esperando uniformemente por um ciclo periódico de ~50 ms. O RabbitMQ nunca apresentou o efeito.

**Primeira hipótese: timer do Windows. Aplicada, não resolveu.** A granularidade padrão do timer do Windows é de 15,6 ms, e desde o Windows 10 2004 a resolução fina só é garantida a processos que a solicitam ([`timeBeginPeriod`](https://learn.microsoft.com/en-us/windows/win32/api/timeapi/nf-timeapi-timebeginperiod)). Um A/B de 3 rodadas por lado pareceu confirmar a hipótese (3 rodadas rápidas com o timer em 1 ms), e a correção entrou no commit `25fa684`. **A confirmação estava errada**: com a taxa de ~43% de rodadas lentas observada depois, três rodadas rápidas seguidas acontecem por acaso em cerca de 1 vez em 5. Na matriz oficial, com a correção aplicada, 26 de 60 rodadas do Kafka em carga baixa caíram no regime lento. A correção foi mantida por ser inofensiva, mas não é a causa.

**Segunda hipótese: algoritmo de Nagle. Refutada.** A librdkafka deixa o Nagle ligado por padrão (`socket.nagle.disable=false`) e o RabbitMQ.Client o desliga, o que explicaria a assimetria. Um A/B de 10 rodadas por lado, intercaladas, deu 7 lentas com Nagle e 5 sem — diferença estatisticamente irrelevante.

**Isolamento do caminho.** Em vez de uma terceira hipótese, o caminho foi isolado:

| Configuração | Rodadas lentas | Média | P99 |
| --- | --- | --- | --- |
| Clientes .NET no host Windows (matriz) | 26 de 60 | 3,4 ou 25,4 ms | 6 a 49 ms |
| Ferramenta de latência do Kafka dentro do container | 0 de 8 | ~1,0 ms | 2 a 3 ms |
| **Nossos clientes .NET em containers na rede Docker** | **0 de 10** | **3,07 a 3,12 ms** | **5,80 a 5,89 ms** |

O broker é saudável, e o mesmo código .NET, dentro da rede Docker, é estável e rápido em todas as rodadas. Dada a taxa observada no host, dez rodadas limpas por acaso teriam probabilidade de 0,36%.

**Conclusão.** A causa está no caminho entre o Windows e a VM Linux do Docker Desktop, por onde os clientes no host alcançam as portas publicadas dos brokers. O mecanismo exato dentro desse caminho não foi identificado. O RabbitMQ atravessa o mesmo caminho sem sofrer o efeito, o que sugere interação com o padrão de requisição e resposta da librdkafka — mas isso é conjectura, não medição.

**Correção.** Produtor e consumidor passam a rodar em containers na rede Docker dos brokers (commit `e779e7c`). É também o que o TCC1 declarava: "toda a infraestrutura será containerizada com Docker, garantindo reprodutibilidade e padronização dos experimentos". **Rodar os clientes no Windows foi um desvio da metodologia proposta**, e é ele que explica boa parte da variância que o projeto vinha atribuindo ao ambiente.

**Efeito colateral positivo.** Com os clientes em containers, o cAdvisor mede todos os módulos pela mesma régua, inclusive o produtor — resolvendo o item 2.2 abaixo.

**Consequência para os resultados.** A latência do Kafka em carga baixa (5 a 30 mil ev/s) da matriz de `25fa684` está contaminada e não pode ser usada. Vazão, CPU, memória e todas as métricas do RabbitMQ não dependem do efeito. A matriz precisa ser refeita no modo containerizado para que as duas famílias sejam medidas nas mesmas condições.

**Lição de método, registrada para o artigo.** Duas das três hipóteses pareciam confirmadas por amostras pequenas. Só o A/B com dez rodadas por lado e o isolamento do caminho separaram causa de coincidência.

### 1.2 Aquecimento não era descartado

Os primeiros segundos misturam compilação JIT, conexão, busca de metadados e eventos que chegaram antes de o consumidor assinar. A 30 mil ev/s, um segundo de aquecimento é 1,1% dos eventos de uma janela de 90 s — mais que o 1% que define o P99. O consumidor agora descarta os primeiros 10 s e mede a vazão sobre a mesma janela em que mede a latência.

### 1.3 Assimetria na confirmação do consumidor

O RabbitMQ confirmava cada mensagem; o Kafka nunca registrava progresso. A comparação favorecia o Kafka por omissão.

| Broker | Antes | Depois | Referência |
| --- | --- | --- | --- |
| RabbitMQ | `basic.ack` por mensagem | lote de 100 com `multiple=true` | [Consumer Acknowledgements](https://www.rabbitmq.com/docs/confirms): confirmações "can be batched to reduce network traffic" |
| Kafka | nenhum commit | offset armazenado após entregar ao pipeline, commit periódico (5 s, padrão) | [Confluent: consumidor](https://docs.confluent.io/platform/current/clients/consumer.html), padrão da librdkafka com `enable.auto.offset.store=false` |

Nos dois casos a confirmação acontece **após a entrega ao pipeline**, não após o processamento. Isso é simétrico e está declarado em §2.6.

### 1.4 Prefetch fora da faixa do fornecedor

A documentação do RabbitMQ indica 100 a 300 como faixa que "usually offer optimal throughput". O valor era 1.000; passou a 300.

### 1.5 CPU e memória dos brokers e do banco — exigência do TCC1

O TCC1 exige CPU e memória "abrangendo todos os módulos do sistema". O consumidor media o próprio processo; os brokers e o PostgreSQL só existiam no Prometheus, sem ligação com rodada nenhuma. O runner agora consulta o Prometheus na janela medida de cada rodada e grava média e pico de CPU e memória do broker e do banco.

### 1.6 Robustez da execução desatendida

- **Reset do tópico Kafka sujeito a corrida.** A exclusão é assíncrona; recriar imediatamente pode falhar com o tópico marcado para exclusão. Com a criação automática desligada, a rodada publicaria num tópico inexistente. O reset agora espera a exclusão e confirma o número de partições.
- **Assentamento de 5 s entre rodadas**, para o broker concluir a limpeza da rodada anterior.
- **`run_id` comum** a produtor, consumidor e métricas de container, e **commit do código** em cada linha do CSV consolidado.
- **Persistência ligada por padrão.** A Figura 1 do TCC1 inclui o módulo de persistência em PostgreSQL; rodar a matriz sem ele era medir uma arquitetura diferente da proposta.
- **Verificação cruzada agrupada por carga.** Na mesma taxa, a entrada é idêntica em todas as rodadas; as seis variantes e as cinco repetições precisam produzir o mesmo digest.

### 1.7 Defeitos menores corrigidos no caminho

- A vazão dividia o total recebido (com aquecimento) pela janela medida (sem aquecimento). Numerador e denominador agora cobrem o mesmo intervalo.
- O teto do histograma usava `TimeSpan.TicksPerMinute` (unidades de 100 ns) sobre valores em microssegundos: o limite real era 50 minutos, não os 5 declarados. Inofensivo, corrigido.

### 1.8 Server GC — avaliado e rejeitado

A [documentação da Microsoft](https://learn.microsoft.com/en-us/dotnet/standard/garbage-collection/workstation-server-gc) descreve o Server GC como "intended for server applications that need high throughput", e seria a escolha natural para um serviço. Mas a mesma página adverte que vários processos com Server GC na mesma máquina interferem entre si, com threads de coleta em `THREAD_PRIORITY_HIGHEST`. Aqui produtor, consumidor e os brokers dividem a máquina: as threads de coleta em prioridade máxima roubariam CPU dos brokers e distorceriam justamente a medição deles. **Mantido o Workstation GC, padrão para aplicações de console.** Numa implantação com o consumidor em máquina dedicada, a escolha se inverteria.

### 1.9 Esgotamento do pool de threads no consumidor Kafka — exposto pela containerização

**Sintoma.** Na primeira validação do modo containerizado, `kafka-channels` e `kafka-pipelines` tiveram latência média de ~200 ms e máxima de ~1 s em todas as 6 rodadas, enquanto `kafka-direct` ficava em 3 ms.

**Causa.** O `Consume()` do Confluent.Kafka é síncrono. Os quatro laços de consumo rodavam via `Task.Run`, prendendo quatro threads do pool do .NET. Num container limitado a 3 CPUs, o mínimo do pool é 3 threads: com o pool esgotado, as continuações dos workers de Channels e Pipelines ficavam na fila esperando o pool injetar threads novas. A [documentação da Microsoft sobre ThreadPool starvation](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/debug-threadpool-starvation) descreve essa injeção como "1-2 per second" — o teto de ~1 s observado. O Direct processa na própria thread do laço e não depende de continuação, por isso escapou. No host Windows, com 12 núcleos, o pool tinha folga e o defeito ficava escondido.

**Correção.** `TaskCreationOptions.LongRunning`: uma thread dedicada por partição, fora do pool (commit `d0874a6`).

**Validação** — seis variantes, 10 mil ev/s, 2 rodadas cada, 10 s de aquecimento e 30 s de medição, clientes em containers:

| Variante | Média | P99 | CPU do consumidor | CPU do produtor | CPU do broker |
| --- | --- | --- | --- | --- | --- |
| kafka-direct | 3,06 ms | 5,79 ms | 14% | 100% | 42% |
| kafka-channels | 3,08 ms | 5,83 ms | 20% | 103% | 44% |
| kafka-pipelines | 3,08 ms | 5,83 ms | 20% | 100% | 41% |
| rabbitmq-direct | 0,57 ms | 2,27 ms | 64% | 172% | 211% |
| rabbitmq-channels | 0,57 ms | 2,35 ms | 69% | 162% | 200% |
| rabbitmq-pipelines | 0,56 ms | 2,22 ms | 71% | 172% | 212% |

Zero falhas e digest idêntico nas 12 rodadas. O P99 do Kafka variou 0,7% entre rodadas, contra 6 a 49 ms no host.

Leitura preliminar, a confirmar na matriz: o RabbitMQ tem latência ~5× menor em carga baixa, mas consome ~2,7× mais CPU somando os três módulos. Parte da latência do Kafka vem de `linger.ms=5`, que é o padrão da librdkafka: o produtor espera até 5 ms para agrupar, o que dá ~2,5 ms médios de espera deliberada. A CPU do produtor inclui a espera ativa do gerador em malha aberta, que custa perto de um núcleo nos dois brokers; a diferença entre eles é o custo do cliente de publicação.

**Lição de método.** Os dois defeitos mais graves desta revisão — o caminho Windows→VM e o esgotamento do pool — mascaravam um ao outro. No host, a folga do pool escondia o segundo; no container, o primeiro desaparecia e o segundo aparecia. Nenhum dos dois seria visível sem mudar o ambiente de execução e medir de novo.

## 2. Achados que não comprometem a matriz em curso

Ficam registrados para correção depois da execução. Nenhum invalida os resultados desta matriz; alguns limitam o que ela pode concluir.

### 2.1 Pipelines sinaliza o leitor a cada mensagem — limita a conclusão sobre Pipelines

A [documentação de System.IO.Pipelines](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines) mostra o escritor chamando `FlushAsync` depois de cada **recepção** — um bloco de bytes vindo do socket —, não depois de cada mensagem. `FlushAsync` é o que torna os dados visíveis ao leitor e o acorda.

A implementação atual chama `FlushAsync` a cada registro de 46 bytes. Com isso, o Pipelines acorda o leitor por mensagem, exatamente como o Channels — e perde a vantagem para a qual foi projetado, que é transportar blocos de bytes com um único sinal.

**Consequência.** A matriz mede o Pipelines **usado por mensagem**. É um uso válido, mas não o melhor aproveitamento da ferramenta, e um empate entre Channels e Pipelines não pode ser generalizado para "Pipelines não traz vantagem". A conclusão sobre Pipelines precisa dizer isso.

**Correção proposta.** Drenar as mensagens já disponíveis antes de sinalizar: no Kafka, consumir com tempo de espera zero até não haver mais nada e só então chamar `FlushAsync`; no RabbitMQ, sinalizar a cada lote de entregas ou quando o fluxo para. Rodar como bateria adicional, `pipelines-lote`, sem substituir a variante atual — a comparação entre as duas formas de usar o Pipelines é, em si, um resultado.

### 2.2 CPU e memória do produtor não eram medidos — resolvido

O módulo de ingestão é um dos módulos da Figura 1 do TCC1, e o custo do cliente de publicação é parte do custo da arquitetura. **Resolvido pela containerização** (§1.1): o produtor é um container e o cAdvisor o mede como aos demais. Na primeira rodada de teste, o produtor Kafka usou 75 a 81% de CPU e o RabbitMQ 94 a 129% a 10 mil ev/s — diferença que antes era invisível.

### 2.2b Encerramento do consumidor Kafka por partição — resolvido

Na matriz, a rodada `kafka-pipelines#2` a 400 mil ev/s perdeu 2,12 milhões de eventos: um travamento de 5,6 s levou uma das quatro threads de partição a encerrar sozinha pelo tempo ocioso de 6 s. A verificação cruzada de digest pegou a perda. O consumidor agora recebe o total esperado e só encerra ao completá-lo; o tempo ocioso passou a valer para o sistema inteiro, como rede de segurança.

### 2.3 Espera síncrona no caminho de contrapressão

Quando a fila interna enche, `ChannelsPipeline` e `PipelinesPipeline` bloqueiam a thread chamadora com `GetAwaiter().GetResult()`. No consumidor RabbitMQ essa thread é o despachante assíncrono do cliente, e bloqueá-lo pode esgotar o pool de threads sob saturação. Na matriz atual o caminho praticamente não é exercitado: a capacidade de 10 mil eventos por faixa é muito maior que o prefetch de 300. Correção: `Submit` devolver `ValueTask` e as fontes aguardarem.

### 2.4 Registro incompleto descartado em silêncio

Ao completar o `PipeReader`, bytes remanescentes que não formam um registro inteiro são descartados sem aviso. A documentação recomenda lançar `InvalidDataException` nesse caso. Com registros de tamanho fixo e escritor confiável não deve ocorrer, mas se ocorrer precisa aparecer.

### 2.5 Assimetria de durabilidade — declarar

A [documentação do RabbitMQ](https://www.rabbitmq.com/docs/confirms) diz que a confirmação de uma mensagem persistente numa fila durável "will be sent after persisting the message to disk". A [configuração do Kafka](https://kafka.apache.org/43/configuration/topic-configs/) tem `flush.messages` com padrão praticamente infinito e recomenda não alterá-lo, usando replicação para durabilidade e deixando a descarga para o sistema operacional.

Com um único nó, sem replicação, o Kafka confirma com a mensagem apenas no cache de página: uma queda da máquina a perderia. O RabbitMQ confirma com ela em disco. **Nesta configuração o RabbitMQ oferece garantia mais forte que o Kafka**, e isso favorece o Kafka na comparação. O custo medido foi de até 7,3% (persistente contra transiente, docs/PILOTO.md) — pequeno diante de uma diferença de 7× entre os tetos, mas precisa estar declarado.

Pelo mesmo motivo, `acks=all` com fator de replicação 1 equivale a `acks=1`.

### 2.6 Janela de perda no desacoplamento

Nas variantes Channels e Pipelines, a confirmação ao broker acontece quando o evento entra na fila interna, antes do processamento. Uma queda do consumidor nesse intervalo perde os eventos em trânsito, para os dois brokers igualmente. É o preço do desacoplamento que mitiga o head-of-line blocking (§4.1), e deve ser apresentado como parte do trade-off.

### 2.7 Uma única corrida no workload

A matriz usa apenas o Bahrein 2024, embora nove corridas estejam coletadas. Para o transporte o conteúdo é quase indiferente; para o processamento, circuitos diferentes mudam a frequência de frenagens e trocas de marcha. Uma bateria de robustez com uma corrida de perfil oposto — Mônaco, lenta e com muitas frenagens — fortaleceria a validade externa.

## 3. Testes automatizados

O projeto não tinha testes automatizados: a verificação era feita por execução. Os três defeitos mais graves encontrados até aqui — o estouro do número do carro, a vírgula decimal no CSV e o 404 da OpenF1 — foram descobertos rodando, não testando.

Agora há 24 testes em `tests/Pitwall.Tests`:

| Área | O que garante |
| --- | --- |
| Codec | Ida e volta de todos os campos; números de carro acima do limite de `short` (regressão do estouro); remontagem de registro partido entre segmentos, como o `PipeReader` entrega |
| Multiplicação de frota | Nenhum número de carro repetido ou negativo com fator 5.000; deslocamento de fase dentro do intervalo de amostragem |
| Processamento | Janelas por tempo de evento; frenagem contada na transição; neutro não é troca de marcha; estado isolado por carro; **reordenar amostras muda o resultado** — o teste que justifica todo o arranjo de particionamento |
| Digest | Independe do número de workers e da ordem de fechamento das janelas; detecta uma janela perdida |

## 4. Aprofundamento dentro do escopo do TCC1

### 4.1 Head-of-line blocking como enquadramento teórico

O TCC1 compara Channels e Pipelines sem dizer que problema eles resolvem. A literatura dá o nome: [head-of-line blocking](https://en.wikipedia.org/wiki/Head-of-line_blocking), quando o primeiro item de uma fila impede o avanço dos seguintes. Na variante Direct, o processamento acontece dentro do laço que busca do broker; enquanto um evento é processado, os seguintes da mesma partição esperam. Channels e Pipelines desacoplam a busca do processamento — são mecanismos de mitigação de HOL blocking dentro do consumidor.

O primeiro teste concorrente já mostrava isso, antes da correção do timer: `kafka-direct` com 25,6 ms de média contra 3,9 ms das variantes desacopladas.

**Ordenação e HOL blocking são a mesma moeda.** O Kafka garante ordem por chave dentro da partição e, por isso, [um evento lento bloqueia tudo que vem atrás dele na partição](https://milanjovanovic.tech/blog/rabbitmq-vs-kafka-dotnet). O RabbitMQ, na configuração natural de fila única com consumidores concorrentes, não sofre HOL blocking — mas também não garante ordem. Este trabalho equalizou pela ordem (P filas com roteamento por carro), e com isso **deu ao RabbitMQ a mesma exposição a HOL blocking que o Kafka tem**. É uma escolha que favorece o modelo do Kafka e precisa ser declarada.

### 4.2 Experimento proposto: sensibilidade a evento lento

Com carga uniforme, os três mecanismos tendem a empatar: nada os diferencia. Tornar o custo sintético probabilístico — por exemplo, 1 evento em cada 10 mil custando 50 ms — isola exatamente a propriedade que o §4.1 descreve. Direct deve degradar; Channels e Pipelines devem absorver; e o efeito deve diferir entre os brokers. É uma propriedade arquitetural qualitativa, não velocidade bruta, e responde "quando o mecanismo interno importa?" melhor que um empate. Custo: cerca de dez linhas no `TelemetryProcessor`, já existe o `SyntheticCostMicros`.

### 4.3 RabbitMQ Streams — decisão pendente

Registrado em docs/PILOTO.md: o tópico do Kafka é um log e a fila clássica é uma fila; o fornecedor declara que "a super stream corresponds to a Kafka topic". Incluir streams separaria o efeito do broker do efeito da abstração. Aguarda o orientador.

### 4.4 O que a conclusão pode afirmar

A [comparação da Aiven](https://aiven.io/tools/streaming-comparison) e a [análise para .NET de Milan Jovanović](https://milanjovanovic.tech/blog/rabbitmq-vs-kafka-dotnet) convergem: em volumes moderados os dois brokers dão conta, e a escolha real é operacional — replay, roteamento, retentativa, fila de mensagens mortas, escala de consumidores. A contribuição deste trabalho é sobre o **limite superior** de cada arquitetura e sobre o **efeito do mecanismo interno**, não sobre qual broker escolher em geral. A conclusão deve dizer isso explicitamente.

Uma vantagem operacional do RabbitMQ que a matriz apaga: o Kafka limita consumidores ao número de partições, o RabbitMQ não. Fixar P = 4 nos dois é correto para medir, mas remove essa diferença.

## 5. Protocolo de uma rodada

Para a seção de metodologia.

1. Zerar o broker: recriar o tópico Kafka (aguardando a exclusão e confirmando as partições) ou esvaziar as filas RabbitMQ.
2. Assentamento de 5 s.
3. Subir o consumidor, com resolução de timer de 1 ms, persistência ligada e tabelas truncadas.
4. Após 3 s, subir o produtor, também com timer de 1 ms, publicando `taxa × (aquecimento + medição)` eventos em malha aberta.
5. O consumidor descarta os primeiros 10 s como aquecimento e mede latência e vazão nos 90 s seguintes.
6. O consumidor encerra após 6 s sem mensagem.
7. O runner consulta o Prometheus na janela medida e grava CPU e memória do broker e do banco.
8. Uma linha consolidada por rodada, com `run_id`, commit e todas as métricas.
9. Ao fim, verificação cruzada: todas as rodadas de uma mesma carga devem ter o mesmo digest.

Rodada válida: jitter de emissão do produtor até 50 ms, digest igual ao das demais rodadas da mesma carga, e nenhuma janela descartada na persistência — esta última apenas para as análises de completude.

## Referências consultadas

- Microsoft. [timeBeginPeriod function](https://learn.microsoft.com/en-us/windows/win32/api/timeapi/nf-timeapi-timebeginperiod).
- Microsoft. [Workstation vs. server garbage collection](https://learn.microsoft.com/en-us/dotnet/standard/garbage-collection/workstation-server-gc).
- Microsoft. [System.IO.Pipelines](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines).
- RabbitMQ. [Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms).
- RabbitMQ. [RabbitMQ vs. Apache Kafka](https://www.rabbitmq.com/docs/compare/kafka).
- Apache Kafka. [Topic Configs](https://kafka.apache.org/43/configuration/topic-configs/).
- Confluent. [Kafka Consumer](https://docs.confluent.io/platform/current/clients/consumer.html).
- Wikipedia. [Head-of-line blocking](https://en.wikipedia.org/wiki/Head-of-line_blocking).
- Aiven. [Streaming comparison](https://aiven.io/tools/streaming-comparison).
- M. Jovanović. [RabbitMQ vs Kafka: Which One for .NET Applications](https://milanjovanovic.tech/blog/rabbitmq-vs-kafka-dotnet).
