# Guia de estudo e preparação para a banca — TCC2

**Avaliação Experimental de Arquiteturas Compostas de Processamento de Eventos em Alta Frequência Utilizando .NET**
Pedro Augusto Ramos de Sousa · UTFPR Dois Vizinhos · Engenharia de Software

> Como usar: a Parte 0 explica, em linguagem simples, cada ponto que você anotou. Depois leia a Parte 1 para ter a história inteira na cabeça; a Parte 2 para os conceitos; as Partes 3 a 8 acompanham o texto do TCC seção por seção, sempre com **o que foi feito, por que, e em que se baseia**; a Parte 9 é o banco de perguntas da banca; a Parte 10 é a cola de números.
>
> Tudo o que está aqui vem do texto (`tcc2.tex`) ou do código do repositório. Onde eu sugiro uma resposta que **não está escrita no texto**, marquei com **[sugestão]** — use, mas saiba que é argumento seu, não dado medido.

---

## Sumário

0. [Suas anotações, explicadas sem complicação](#0-suas-anotações-explicadas-sem-complicação)
1. [A história do trabalho em 2 minutos](#1-a-história-do-trabalho-em-2-minutos)
2. [Conceitos que você precisa dominar](#2-conceitos-que-você-precisa-dominar)
3. [A bancada, etapa por etapa](#3-a-bancada-etapa-por-etapa)
4. [Configuração dos brokers e os dois cenários](#4-configuração-dos-brokers-e-os-dois-cenários)
5. [Metodologia: protocolo, regras e calibração](#5-metodologia-protocolo-regras-e-calibração)
6. [Resultados, um por um, com o porquê](#6-resultados-um-por-um-com-o-porquê)
7. [Comparação com a literatura e ameaças à validade](#7-comparação-com-a-literatura-e-ameaças-à-validade)
8. [Conclusão, estudo preliminar e trabalhos futuros](#8-conclusão-estudo-preliminar-e-trabalhos-futuros)
9. [Banco de perguntas da banca, com respostas](#9-banco-de-perguntas-da-banca-com-respostas)
10. [Cola: números para saber de cor](#10-cola-números-para-saber-de-cor)
11. [A apresentação, slide a slide](#11-a-apresentação-slide-a-slide)

---

## 0. Suas anotações, explicadas sem complicação

> Cada item tem três partes. **Em uma frase** é o que você fala. **Explicando** é para entender. **Se perguntarem** é a resposta curta para a banca. A ordem é a das suas anotações. Os números da seção do TCC aparecem entre parênteses.

---

### 0.1 "Processamento de eventos em alta frequência é…" (2.1)

**Em uma frase:** é receber, processar e responder a **muitos eventos por segundo**, em tempo **próximo do real**.

**Explicando:** um evento é um fato pequeno que acontece num instante, como "o carro 44 estava a 312 km/h no instante t". Em alta frequência, chegam milhares ou centenas de milhares desses fatos por segundo, e eles precisam ser tratados conforme chegam, não guardados para processar depois. Exemplos são telemetria, IoT, monitoramento e o mercado financeiro.

**Como esses sistemas são avaliados:** por três coisas.
- **Latência:** quanto tempo cada evento demora.
- **Throughput:** quantos eventos passam por segundo.
- **Uso de recursos:** quanta CPU e memória isso custa.

**Se perguntarem:** "É ingerir, processar e responder a grandes volumes de eventos em tempo próximo ao real; avalia-se por latência, *throughput* e uso de recursos (Luckham; Cugola; Jain; Fu)."

---

### 0.2 Entendendo a seção 2.1 inteira, parágrafo por parágrafo

A 2.1 apresenta, em sequência, **as ferramentas de medição que o trabalho todo usa**. São seis ideias, nesta ordem:

1. **O que é** processamento de eventos em alta frequência (item 0.1).
2. **Como se mede:** latência, *throughput* e recursos.
3. **Por que olhar a cauda (P99) e não a média.** O usuário não sente a média, sente o evento que atrasou. Imagine 100 eventos: 99 levam 1 ms e 1 leva 500 ms. A média dá cerca de 6 ms e parece ótima, mas esconde o problema. O **P99** é o valor abaixo do qual ficam 99% dos eventos, e mostra essa cauda.
4. **Para medir a cauda, o gerador precisa ser de malha aberta** (item 0.6). Se não for, ocorre a *coordinated omission*: os piores atrasos somem da medição.
5. **Ponto de saturação:** a maior carga que o sistema aguenta antes de a fila e a latência crescerem sem parar (item 0.5).
6. **Operações de referência:** as operações que os *benchmarks* clássicos usam. O TCC usa as mesmas, para que o processamento não seja "inventado" (item 0.15).

**Resumo para guardar:** a 2.1 diz **o que medir** (latência pela cauda, *throughput*, recursos), **como medir sem enganar** (malha aberta), **qual o limite** (saturação) e **que trabalho simular** (operações de referência).

---

### 0.3 Por que é importante estudar isso: os 3 motivos (Introdução)

**Em uma frase:** porque ninguém tinha medido a **cadeia inteira** em .NET; porque Channels e Pipelines **têm um custo que ninguém mediu**; e porque comparar *brokers* **só é justo com ambiente controlado**.

1. **As comparações olham o *broker* sozinho, e quase sempre em Java.** Numa aplicação .NET, o que o usuário sente é o desempenho da cadeia inteira: *broker* + biblioteca cliente + consumidor. Medir só o *broker* não responde à pergunta de quem escreve o sistema em .NET.
2. **Channels e Pipelines são recomendados, mas não são de graça.** A Microsoft os indica para produtor-consumidor, mas eles **acrescentam uma fila e uma troca de *thread* a cada evento**. Sem medir, não se sabe se isso **reduz a latência ou só gasta CPU**.
3. **Comparação entre *brokers* sem controle mede a máquina, não a arquitetura.** Fu et al. (2021) mostram isso. A diferença medida pode vir da configuração ou do ambiente, e por isso o TCC controla os dois.

**Dica:** esses três motivos viram as três contribuições do trabalho.
- (i) Seis combinações medidas com grupo de controle.
- (ii) Um protocolo com regras fixadas antes da coleta.
- (iii) Uma bancada aberta.

---

### 0.4 O que é cada coisa: Kafka, RabbitMQ, Channels, Pipelines, Direct

Pense em **duas camadas**. A camada de fora é o ***broker***, que leva o evento do gerador até o consumidor. A camada de dentro é o **mecanismo interno**, que decide como o evento anda **dentro** do consumidor.

| Nome | Camada | Em uma frase | Analogia |
|---|---|---|---|
| **Kafka** | *broker* | Um **log** (um caderno onde só se escreve no fim) dividido em partições. Trabalha em **lotes** e foi feito para volume alto. | Correio que junta cartas num malote e manda o malote inteiro. |
| **RabbitMQ** | *broker* | *Broker* de uso geral com **filas**. Trata **cada mensagem individualmente**: roteia, confirma e grava. | Correio que entrega carta por carta, com recibo de cada uma. |
| **Direct** | mecanismo | **Nenhum mecanismo:** a mesma *thread* que recebe a mensagem já a processa. | Quem atende o telefone resolve o pedido na hora. |
| **Channels** | mecanismo | **Fila de objetos** dentro do processo: quem recebe põe o evento na fila, e outra *thread* tira e processa. | Quem atende anota o pedido num papel e põe numa bandeja; outra pessoa pega da bandeja e resolve. |
| **Pipelines** | mecanismo | **Fila de bytes:** quem recebe copia os bytes crus, e o leitor interpreta direto no *buffer*, sem criar um objeto por evento. | Igual à bandeja, mas em vez de papéis preenchidos vai uma fita contínua que o outro lê. |

**Por que o Direct existe:** é o **grupo de controle**. Sem ele, se "Kafka com Channels" fosse melhor que "RabbitMQ com Channels", daria para concluir que o *broker* importa, mas **não** se o Channels ajudou ou atrapalhou. Comparando Channels com Direct no mesmo *broker*, isola-se o efeito do mecanismo.

---

### 0.5 Saturou? Era limitado pela CPU?

**Em uma frase:** o **RabbitMQ saturou em 60 mil eventos/s porque a CPU do *broker* acabou**. O **Kafka não saturou até 400 mil**, a maior carga testada.

**Explicando "saturar":** um sistema satura quando chega mais trabalho do que ele consegue fazer. A fila cresce, a latência cresce junto e não para mais. No TCC, uma carga é **sustentada** se três condições valem:
- a maioria das rodadas é válida;
- o *throughput* chega a 99% da carga;
- o **P99 fica abaixo de 50 ms**.

O **ponto de saturação** é a maior carga sustentada, desde que todas as menores também sejam.

**RabbitMQ: sim, limitado pela CPU do *broker*.**
- O *broker* ocupou de **2,8 a 3,5 dos 4 núcleos** que tinha.
- Ele trata cada mensagem sozinha (roteia, confirma, grava em disco), e isso custa CPU por mensagem.
- Com a CPU no limite, a fila acumula e a cauda sobe: o P99 foi a **25 ms a 60 mil** e a **75 ms a 80 mil**, passando do limite de 50 ms.

**Kafka: não saturou.**
- Ocupou só **0,4 a 0,5 núcleo** até 400 mil, porque trabalha em lotes.
- O ponto de saturação dele **fica acima do alcance da bancada**.
- A 200 mil ou mais, algumas rodadas foram invalidadas porque o **gerador** atrasou o envio (episódios de disco da VM). O limite ali é da máquina, não do Kafka.

**E o mecanismo interno?** Não mudou o ponto de saturação em nenhum *broker*. O consumidor nunca foi o gargalo.

**Se perguntarem "como sabe que foi a CPU?":** "A CPU do *broker* RabbitMQ estava em 2,8 a 3,5 de 4 núcleos, e no cenário ajustado, ao tirar a gravação em disco, sobrou CPU e o ponto de saturação subiu de 60 para 80 mil."

---

### 0.6 Malha aberta

**Em uma frase:** o gerador **envia cada evento no horário marcado**, sem esperar o sistema responder.

**Explicando com analogia:** imagine medir a fila de um caixa de supermercado.
- **Malha fechada:** você manda um cliente só quando o anterior saiu. Se o caixa ficar lento, você manda menos gente, a fila nunca cresce e o caixa parece ótimo. Você **escondeu o problema**.
- **Malha aberta:** os clientes chegam no ritmo do mundo real, um a cada X segundos, aconteça o que acontecer. Se o caixa ficar lento, a fila cresce e você **vê** o atraso.

Esse erro da malha fechada tem nome: ***coordinated omission*** (Tene, 2015). O gerador "combina" com o sistema e deixa de medir justamente os eventos mais atrasados.

**Consequência no TCC:** como o gerador é de malha aberta, **o *throughput* sempre bate com a carga** enquanto o sistema aguenta, porque todos processam exatamente o que chega. Por isso o *throughput* não distingue as combinações. O que distingue é **a latência e o ponto de saturação**.

---

### 0.7 100 Hz

**Em uma frase:** cada carro passa a gerar **100 eventos por segundo**, em vez dos **3,7 por segundo** que a OpenF1 fornece.

**Explicando:**
- "Hz" é "vezes por segundo". A telemetria real da OpenF1 vem a cerca de 3,7 Hz, ou seja, uma leitura a cada 270 ms por carro. É pouco para uma carga de alta frequência.
- O gerador **reamostra** cada carro para 100 Hz, uma leitura a cada 10 ms, preenchendo as lacunas entre duas leituras reais:
  - **Velocidade, rotação e acelerador** são **interpolados em linha reta**. Se a velocidade foi de 300 para 310 km/h, os pontos do meio ficam em 301, 302 e assim por diante.
  - **Marcha, freio e DRS** ficam **em degrau**: mantêm o último valor real até a próxima leitura, porque não existe "marcha 4,5".
- Resultado: **as frenagens e trocas de marcha são exatamente as da corrida original**, e não aparecem eventos inventados.

**Se perguntarem "isso não distorce os dados?":** "Rodei uma bateria de sensibilidade **a 3,7 Hz, sem interpolação**. O P99 mudou menos de 10%, e as conclusões são as mesmas."

---

### 0.8 Frota multiplicada

**Em uma frase:** para chegar a centenas de milhares de eventos por segundo, **cada carro real vira vários carros**, cada um com número próprio e começando num ponto diferente da volta.

**Explicando:**
- A 100 Hz, 20 carros dão só 2.000 eventos por segundo. Para 100 mil, são necessários **1.000 carros** (100 mil ÷ 100 Hz).
- O gerador cria **réplicas** de cada carro real. Cada réplica recebe:
  - **um número de carro próprio**, para ser um carro distinto, ir para sua faixa e ter suas próprias janelas;
  - **um deslocamento de fase**: começa em outro ponto do trecho da corrida, para que nem todas as réplicas freiem no mesmo instante.
- **Por que não acelerar o tempo?** Tocar a corrida 10× mais rápido também daria mais eventos por segundo, mas mudaria a física da carga: frenagens a cada poucos milissegundos e janelas de 1 s com dados de 10 s. Multiplicar a frota mantém **o tempo real** e cada carro se comporta como um carro de verdade.

**Por que não liga compressão:** as réplicas têm conteúdo quase igual, e o Kafka comprime lotes. Comprimir daria uma vantagem artificial ao Kafka.

---

### 0.9 Como funciona o gerador

Ele faz, em ordem:

1. **Lê a cópia local** da telemetria do Bahrein 2024. A API **não** é chamada durante os experimentos.
2. **Recorta 130 s da corrida, a partir do décimo minuto**, um trecho de corrida "normal", já sem a largada.
3. **Reamostra de 3,7 para 100 Hz** (item 0.7).
4. **Multiplica a frota** até a carga pedida (item 0.8).
5. **Monta cada evento como um registro binário de 46 bytes, de tamanho fixo**, e grava nele o **instante da publicação**, lido de um relógio monotônico, para medir a latência depois.
6. **Publica cada evento no instante previsto**, em malha aberta (item 0.6), na faixa do carro (CRC32 do número do carro).
7. **Registra o maior atraso de envio**: quanto ficou atrás do horário previsto. Se passar de **50 ms**, a rodada é inválida, porque a carga pedida não foi de fato oferecida.

Ele roda num *container* com **4 núcleos exclusivos** (0 a 3) e aceita até **100 mil mensagens sem confirmação** do *broker* (limite igual nos dois).

---

### 0.10 Diferença entre Channels e Pipelines

**Em uma frase:** o **Channels transporta objetos**; o **Pipelines transporta bytes**.

| | Channels | Pipelines |
|---|---|---|
| O que vai na fila | um **objeto** `TelemetryEvent` já pronto | os **46 bytes crus** da mensagem |
| Quem desserializa | a *thread* do cliente, **antes** de pôr na fila | o leitor, **depois**, direto no *buffer* |
| Cria objeto por evento? | sim | não |
| Limite da fila | **10.000 eventos** por faixa | **460.000 bytes** (= 10.000 × 46, o mesmo volume) |
| Quando a fila enche | o escritor espera (`FullMode.Wait`) | o escritor pausa (`pauseWriterThreshold`) e volta à metade |
| Pensado para | passar **itens** entre *threads* | **fluxos de bytes** (rede, arquivos), com *buffers* reaproveitados |

**O que é igual:** os dois tiram o processamento da *thread* do cliente e passam para uma *thread* de trabalho, com **um leitor por faixa** e contrapressão quando enchem.

**Resultado:** latência igual à do Direct nos dois. No Kafka, o **Pipelines gastou sempre um pouco mais de CPU que o Channels**. A alocação também foi igual nos três modos (~950 B/evento no Kafka), porque **quem aloca é a biblioteca cliente**, antes de o evento chegar ao mecanismo. Por isso a vantagem teórica do Pipelines ("não cria objeto") não apareceu.

---

### 0.11 Por que Channels e Pipelines "distribuem o trabalho com pouco *overhead*"

**Em uma frase:** porque são filas feitas para isso: **sem travar *threads*** enquanto esperam, com **pouca disputa** entre quem escreve e quem lê e, no Pipelines, **reaproveitando a memória**.

**Explicando (conhecimento geral de .NET; a frase vem de Cleary, 2019):**
- O jeito "na mão" seria uma `Queue` com `lock` e uma *thread* parada esperando item. Isso gasta uma *thread* bloqueada e gera disputa pelo *lock*.
- **Channels:** quem lê **espera de forma assíncrona** (`await`). A *thread* fica livre enquanto não chega item. A implementação é otimizada para um escritor e um leitor (`SingleReader`/`SingleWriter`, que o TCC usa).
- **Pipelines:** além disso, usa ***buffers* reaproveitados** de um *pool*, então não aloca memória nova a cada mensagem nem copia os dados para interpretar.

**O ponto importante para a banca:** "pouco *overhead*" **não é zero**.
- O TCC mediu o custo: **2,4 a 3,7 µs por evento** a mais de CPU no Kafka, mais ou menos o custo de acordar uma *thread* a cada evento.
- O processamento em si custa **0,12 µs**. Diante disso, até "pouco" é **20 a 30 vezes o trabalho útil**. Por isso o mecanismo custou CPU sem trazer ganho.

**Se perguntarem:** "Pouco *overhead* comparado a fazer a fila na mão, mas não é de graça: medi 2,4 a 3,7 µs por evento, e quando o trabalho útil é 0,12 µs, esse custo domina."

---

### 0.12 Critério de validade da rodada

**Em uma frase:** antes de olhar os dados, defini **quatro motivos para jogar uma rodada fora**. Se qualquer um acontece, a rodada é inválida e **não é repetida**.

| Regra | O que garante |
|---|---|
| **Atraso máximo de envio > 50 ms** | O gerador realmente ofereceu a carga pedida. Se ele atrasou, a rodada não mediu "60 mil/s". |
| ***Digest* diferente do de referência** | O sistema processou **tudo certo**: sem perder, duplicar ou embaralhar evento (item 0.16). |
| **Registro incompleto no `Pipe`** | O Pipelines não quebrou nenhuma mensagem no meio. |
| **Persistência descartou janelas** | O banco não perdeu resultados. |

**Por que "não repetir"?** Repetir até dar certo seria escolher o resultado. A rodada inválida fica registrada e conta contra a carga (a carga só é sustentada se **a maioria** das rodadas for válida).

**O que aconteceu:** 12 inválidas em 390, **todas do Kafka a 200 mil ou mais, todas pelo atraso de envio**, que coincide com os episódios de disco da VM. **Nenhuma falhou no *digest*.**

---

### 0.13 *Brokers* com um único nó?

**Em uma frase:** cada *broker* rodou **como um servidor só**, sem *cluster*, porque **a bancada é uma única máquina**.

**Explicando:**
- Em produção, Kafka e RabbitMQ costumam rodar em vários servidores (**nós**) que copiam as mensagens entre si (**replicação**), para não perder dados se um cair.
- Aqui há **um nó**: então **não há replicação**. No Kafka, o `acks=all` significa "todas as réplicas confirmaram", e com um nó só a réplica é ele mesmo.
- **Por que não simular vários nós na mesma máquina?** [sugestão] Os nós dividiriam a mesma CPU e o mesmo disco. Você mediria a **disputa entre eles**, não o custo real da replicação, que em produção passa pela rede entre máquinas.

**Se perguntarem:** "É uma limitação declarada. O resultado vale para um nó por *broker*. Com vários nós, a replicação acrescenta rede e confirmação entre servidores, o que pode mudar as latências. Por isso está nos trabalhos futuros: vários nós com replicação."

---

### 0.14 *At-least-once*

**Em uma frase:** **nenhuma mensagem se perde, mas uma pode chegar mais de uma vez**.

**Explicando com analogia (carta registrada):** o correio só dá a carta como entregue quando recebe o seu recibo. Se o recibo se perder, ele manda de novo. Você nunca fica sem a carta, mas pode receber duas.

**Como funciona nos dois lados:**
1. **Gerador → *broker*:** o *broker* confirma que recebeu.
   - Kafka: `acks=all`.
   - RabbitMQ: *publisher confirms*, em lotes de 12.500, dois lotes "em voo" por faixa.
   - Sem confirmação, o gerador reenviaria.
2. ***Broker* → consumidor:** o consumidor só confirma **depois de entregar ao processamento**.
   - Kafka: guarda o *offset* só depois disso.
   - RabbitMQ: confirma em lotes de 100.
   - Se o consumidor cair antes, o *broker* entrega de novo.

**Por que essa garantia?** É a garantia comum para telemetria (perder dado é pior que receber repetido) e é a que **os dois *brokers* oferecem do mesmo jeito**. Comparar um *broker* que pode perder mensagem com outro que não pode seria injusto.

---

### 0.15 Operações de referência dos *benchmarks*

**Em uma frase:** o processamento do consumidor faz **o mesmo tipo de trabalho que os *benchmarks* clássicos de fluxo de eventos usam**, em vez de algo inventado.

**Os *benchmarks*:**
- **Linear Road** (Arasu et al., 2004) simula pedágio em rodovias.
- **DEBS 2013 Grand Challenge** (Mutschler et al., 2013) usa dados de sensores de um jogo de futebol.

Os dois usam duas operações típicas:

| Operação | O que é | No TCC |
|---|---|---|
| **Agregação em janela por chave** | Juntar os eventos de cada "coisa" num intervalo de tempo e resumir. | **Janelas de 1 s por carro**, em **tempo do evento** (pelo instante em que aconteceu, não em que chegou): velocidade máxima, contagem etc. |
| **Detecção de padrões em eventos consecutivos** | Olhar um evento e o anterior da mesma chave para achar uma mudança. | **Frenagens** (freio passando o limiar) e **trocas de marcha** entre dois eventos seguidos do mesmo carro. |

**Por que isso importa:** a detecção **depende da ordem** dos eventos de cada carro. Por isso há **um leitor por faixa**: dois leitores poderiam processar fora de ordem e criar ou perder frenagens.

---

### 0.16 *Digest*

**Em uma frase:** é uma **"impressão digital" do resultado** de cada rodada. Se todas as combinações processaram a mesma entrada direito, **todas têm de dar o mesmo *digest***.

**Explicando:** ao fim da rodada, o consumidor resume tudo o que calculou num conjunto de números:
- quantas janelas;
- quantos eventos;
- quantas frenagens;
- quantas trocas de marcha;
- a soma das velocidades máximas;
- um *hash* de cada janela, combinado com os outros.

A combinação é feita com **soma e XOR**, operações em que **a ordem não muda o resultado** (5 + 3 = 3 + 5). Assim o *digest* não depende de qual janela fechou primeiro nem de quantas *threads* existiam.

**O que ele pega:**
- **Evento perdido:** a contagem cai.
- **Evento duplicado:** a contagem sobe.
- **Evento fora de ordem:** uma frenagem aparece ou some, e o *hash* muda.

**Resultado:** **zero divergências** em todas as rodadas. Isso prova que **as seis combinações fizeram exatamente o mesmo trabalho**, condição para comparar desempenho.

**Detalhe se perguntarem:** a velocidade **média** ficou de fora, porque é número com vírgula (ponto flutuante). Somar em outra ordem pode mudar o último dígito sem erro nenhum, e isso daria falso alarme.

---

### 0.17 Persistência e métricas (etapa 5)

**Em uma frase:** os resultados vão para o **PostgreSQL de um jeito que não atrapalha a medição**, e as métricas vêm de **ferramentas separadas**, uma para cada coisa.

**Persistência (guardar os resultados):**
- As janelas são gravadas no **PostgreSQL 17**:
  - **em lote**;
  - com **`COPY` binário**, o jeito mais rápido de inserir muitas linhas;
  - em tabelas **`UNLOGGED`**, que não gravam o diário de recuperação e por isso são mais rápidas.
- Objetivo: **o banco não pode ser o gargalo**, senão você mediria o PostgreSQL em vez do *broker*.
- **A latência termina antes da gravação.** O banco não entra no número.
- Se a gravação descartar janelas, a rodada é inválida.

**Métricas (como cada coisa foi medida):**

| O quê | Com o quê |
|---|---|
| CPU e memória de cada *container* | **Prometheus + cAdvisor** |
| Latência | O gerador grava na mensagem o instante de publicação (**relógio monotônico**, que só anda para frente). O consumidor compara com **o mesmo relógio** no fim do processamento. Tudo na mesma máquina, então **não precisa sincronizar relógios**. |
| Distribuição da latência | **HdrHistogram**, feito para medir percentis altos com precisão |
| Custo do .NET | **Contadores do *runtime***: bytes alocados por evento e coletas de lixo |

---

### 0.18 O que foi feito na calibração

**Em uma frase:** antes da coleta oficial, fiz medições de teste e **descobri que o ambiente estava pesando mais que as arquiteturas**. Corrigi tudo e só então congelei o cenário.

| O que apareceu | O que fiz |
|---|---|
| Gerador e consumidor **fora dos *containers***: o Kafka tinha latência "com dois picos" (bimodal) em **43%** das rodadas de carga baixa | Coloquei **tudo em *containers*** |
| Com **cota de CPU**, o RabbitMQ era "estrangulado" | Dei **núcleos exclusivos** a cada componente: o P99 do RabbitMQ caiu **40% a 64%** |
| Auditoria da configuração contra os valores de fábrica | Achei e corrigi **3 defeitos de configuração** e **3 assimetrias** entre os *brokers* |
| A partir de 100 mil/s, o Kafka **travava de 200 ms a 2 s**, junto com a gravação de páginas no disco virtual. Logo depois de a VM parar esperando disco, a chance de latência > 50 ms era **126 vezes maior** | Criei o critério do **atraso de envio** e usei **10 repetições, mediana e Tukey**, para um episódio isolado não decidir o resultado |
| `linger.ms` = 0 deu P50 de **400 ms** já a 10 mil | Descartei esse valor do cenário ajustado |
| A gravação em disco do RabbitMQ custava **48%** no P99 a 40 mil | Declarei como assimetria e criei o cenário ajustado para igualar |

**O ponto principal:** esses dados **não entram na análise**. A calibração serviu para **consertar a bancada**, não para tirar conclusão. É a origem da frase "o ambiente pesou mais que a arquitetura", que concorda com Fu et al.

---

### 0.19 Cenário ajustado

**Em uma frase:** repeti os experimentos com **a configuração que cada fornecedor recomenda**, mantendo o *at-least-once*, para ver **quanto da diferença vinha da configuração**.

**O que mudou:**
- **RabbitMQ:**
  - **mensagem transitória**: não grava em disco antes de confirmar, o que iguala a garantia à do Kafka (item 0.21);
  - ***prefetch* 300** (item 0.22).
- **Kafka:**
  - **`linger.ms` de 5 para 1 ms**, a recomendação para latência (item 0.23);
  - um **perfil para vazão** com **20 ms**.

**O que aconteceu:**
- **RabbitMQ melhorou muito:**
  - **P99 caiu 43% a 76%** (2,3 → 0,65 ms a 10 mil; 25 → 10 ms a 60 mil);
  - a saturação subiu de **60 para 80 mil**;
  - sem a gravação em disco, sobrou CPU.
- **Kafka foi uma troca:**
  - **`linger.ms` 1:** o P50 caiu de 3 para 1 ms, provando que o "piso" de 3 ms era a espera para formar lotes. Mas com lotes menores o *broker* recebe mais pedidos: **a CPU dele dobrou** e o **P99 subiu** em carga alta (até 22 ms a 300 mil).
  - **`linger.ms` 20:** o contrário. P99 perto de 21 ms, mas **30% menos CPU** no *broker*.
- **Channels e Pipelines continuaram sem efeito** na latência e custando 1,9 a 2,3 vezes a CPU do consumidor no Kafka.

**Conclusão do cenário:** parte da diferença entre os *brokers* no cenário padrão **vinha da configuração**. Um parâmetro mudou o P99 em até 76%, e **cada parâmetro troca latência por CPU**.

---

### 0.20 Uso de recursos (6.3), versão simples

**A ideia em três frases:**
1. **Channels e Pipelines fazem o consumidor trabalhar mais sem entregar nada a mais.**
2. **O *broker* RabbitMQ gasta muita CPU e o Kafka pouca**, e é isso que explica quem satura.
3. **Memória não depende do modo.**

**Agora um por um:**

**a) CPU do consumidor: é onde os modos se separam.**
- **Kafka:**
  - o Direct usa de **0,09 a 0,63** núcleo;
  - Channels e Pipelines usam de **0,17 a 1,69**, ou **1,9 a 3,3 vezes mais**.
  - O extra é de **2,4 a 3,7 µs por evento**, mais ou menos o custo de **acordar uma *thread*** a cada evento.
- **RabbitMQ:**
  - o consumidor já usa **perto de 1 núcleo** em qualquer carga;
  - os mecanismos acrescentam só **4% a 9%**.
  - [sugestão] A biblioteca do RabbitMQ entrega e confirma mensagem a mensagem, então a base já é alta e o extra do mecanismo fica pequeno em proporção.

**b) CPU do *broker*: é o que separa os *brokers*.**
- **RabbitMQ:** **2,8 a 3,5 de 4 núcleos**, perto do limite. Trata cada mensagem individualmente: roteia, confirma e grava em disco.
- **Kafka:** **0,4 a 0,5 núcleo** até 400 mil. Trata lotes: junta, grava em sequência e entrega em lote.
- O mesmo aparece no **gerador**: 3,1 núcleos para publicar no RabbitMQ a 60 mil, contra 1,1 no Kafka.

**c) Memória.**
- **Consumidor:** entre **87 e 117 MB** em todas as combinações. O modo não importa.
- ***Broker*: aqui o Kafka perde.** Usa de **1,4 a 1,7 GB**, contra **0,2 a 0,3 GB** do RabbitMQ.
- [sugestão] O Kafka roda na JVM e mantém o *log* e os índices em memória.

**d) Alocação do .NET.**
- Os três modos alocam **o mesmo**: ~950 B por evento no Kafka e 720 a 834 no RabbitMQ, cerca de **20 vezes o tamanho da mensagem**.
- Quem aloca é **a biblioteca cliente**, antes de o evento chegar ao mecanismo.
- Coletas de lixo pesadas (geração 2): **no máximo uma por rodada**.

**O "porquê" que amarra tudo:** o processamento custa **0,12 µs por evento**, e o consumidor termina um evento muito antes de o *broker* entregar o próximo. **Não há fila de trabalho acumulada** para o mecanismo paralelizar. Então ele só acrescenta a fila interna e a troca de *thread*, isto é, CPU a mais.

---

### 0.21 Mensagem transitória (6.4)

**Em uma frase:** no RabbitMQ, é a mensagem que **não é gravada em disco antes de o *broker* confirmar** ao produtor. Fica em memória.

**Explicando:**
- **Persistente**, o padrão usado: o RabbitMQ **grava em disco e só depois confirma**. É mais seguro contra queda do servidor, mas cada mensagem espera o disco. Na calibração, isso custou **48% no P99** a 40 mil.
- **Transitória**, a do cenário ajustado: confirma **sem esperar o disco**.

**Por que isso é justo e não "afrouxar" o RabbitMQ:** o **Kafka já fazia isso desde o começo**. Ele confirma com a mensagem no cache do sistema operacional, sem esperar o disco, e deixa a durabilidade para a **replicação entre nós**, como recomenda a própria documentação. Tornar a mensagem transitória **iguala a garantia dos dois**. A entrega continua *at-least-once*: o que muda é só a proteção contra a queda do servidor inteiro.

**Se perguntarem "então o cenário padrão era injusto?":** "A assimetria foi deliberada e declarada: no padrão, cada *broker* está com seu valor de fábrica. O cenário ajustado existe exatamente para mostrar o efeito de igualar, e o RabbitMQ melhorou de 43% a 76% no P99."

---

### 0.22 *Prefetch*

**Em uma frase:** no RabbitMQ, é **quantas mensagens o *broker* pode mandar ao consumidor sem receber confirmação**.

**Explicando com analogia:** um garçom pode levar até N pratos para a mesa antes de você dizer "recebi". Com N pequeno, ele fica indo e voltando esperando. Com N ilimitado, ele pode encher a mesa.

**No TCC:**
- **Cenário padrão:** *prefetch* **0 = sem limite**, o valor de fábrica do RabbitMQ (está no código: `--prefetch 0`, "sem limite, padrão do RabbitMQ").
- **Cenário ajustado:** ***prefetch* 300**, seguindo a recomendação do fornecedor de **limitar** o *prefetch*. Assim o *broker* não empurra mensagens sem controle para o consumidor e mantém um fluxo estável.
- **Detalhe do código:** o consumidor confirma em lotes de 100, e o lote precisa caber com folga no *prefetch* (no máximo a metade dele). Senão o *broker* para de entregar esperando uma confirmação que só viria com mais mensagens: um impasse. Com 300 e lotes de 100, cabe.

---

### 0.23 `linger.ms`

**Em uma frase:** no Kafka, é **quanto tempo o produtor espera para juntar mensagens num lote** antes de enviar.

**Explicando com analogia:** o ônibus espera alguns minutos no ponto para encher. Esperar mais leva mais gente por viagem (menos viagens, menos CPU), mas cada passageiro demora mais a sair.

| `linger.ms` | O que aconteceu |
|---|---|
| **5 ms** (fábrica, cenário padrão) | Explica o **piso de ~3 ms no P50** do Kafka em qualquer carga |
| **1 ms** (ajustado, latência) | P50 cai de **3 para 1 ms**, mas lotes menores mandam mais requisições: **CPU do *broker* dobra** e o **P99 sobe** em carga alta |
| **20 ms** (ajustado, vazão) | Lotes grandes: **P99 ≈ 21 ms**, mas **30% menos CPU** |
| **0 ms** | **Descartado** na calibração: o P50 foi a **400 ms** já a 10 mil (sem espera nenhuma, o *broker* afoga em pedidos minúsculos) |

**A lição:** é o exemplo mais claro de que **cada parâmetro troca latência por CPU**, e de que a configuração muda o resultado da comparação.

---

### 0.24 Regra de Tukey

**Em uma frase:** é uma regra **objetiva, fixada antes**, para **tirar as rodadas muito fora da curva**.

**Explicando passo a passo:** em cada combinação (por exemplo, "Kafka + Direct a 100 mil"), pegue o P99 das 10 rodadas.

1. Ordene os 10 valores.
2. **Q1** é o valor a 25% da lista e **Q3**, o valor a 75%. A "caixa do meio" tem metade das rodadas.
3. **IQR = Q3 − Q1**, a largura dessa caixa.
4. Tudo **abaixo de Q1 − 1,5 × IQR** ou **acima de Q3 + 1,5 × IQR** sai.
5. A combinação é resumida pela **mediana** do que sobrou.

**Exemplo:** P99 de 9 rodadas entre 5 e 7 ms e uma com 80 ms (um episódio de disco da VM). Q1 ≈ 5,5 e Q3 ≈ 6,5, então IQR = 1. O limite de cima é 6,5 + 1,5 = 8 ms. A rodada de 80 ms sai.

**Por que usar:** a calibração mostrou que **um episódio da VM podia multiplicar a latência**. Sem a regra, uma rodada azarada decidiria o resultado. Como a regra foi **fixada antes de ver os dados**, não dá para dizer que escolhi o que tirar.

**Números:** descartou **32 de 390 rodadas (~8%)**.

---

### 0.25 Kruskal-Wallis

**Em uma frase:** é um **teste estatístico** que responde "**esses 3 grupos são diferentes de verdade ou é só variação aleatória?**", sem precisar que os dados sigam a curva normal.

**Explicando:**
- Os grupos são os **3 modos** (Direct, Channels, Pipelines) numa mesma carga e *broker*.
- Em vez de usar os valores, ele **ordena todas as rodadas juntas** e vê se um grupo fica sistematicamente no topo ou no fundo da lista (usa **postos**, ou *ranks*).
- **Por que esse e não o ANOVA?** O ANOVA supõe dados normais, e latência não é normal (tem cauda longa). Além disso as amostras são pequenas (10 rodadas). O Kruskal-Wallis não faz essa suposição.
- **Nível de 5%:** se p < 0,05, a diferença dificilmente é sorte.

**O detalhe mais importante:** no TCC ele **só acompanha** as diferenças grandes. O critério principal é o **tamanho do efeito**: **20% no P99, 10% no *throughput***. Com rodadas muito parecidas entre si, **até uma diferença de 2%, que não importa na prática, pode dar "estatisticamente significativa"**. Então primeiro pergunto "a diferença é grande o bastante para importar?" e só depois "ela é real?".

**Números:**
- RabbitMQ a 40 mil: Channels e Pipelines 6% abaixo do Direct, **p = 0,06**, ou seja, nem significativo nem relevante.
- Cenário ajustado: maior diferença de 18%, **p > 0,7**.

---

## 1. A história do trabalho em 2 minutos

**O problema.** Em .NET, um sistema que recebe muitos eventos por segundo (telemetria, IoT, monitoramento) costuma ter duas camadas: um **broker de mensageria** (RabbitMQ ou Kafka) e, dentro do consumidor, um **mecanismo de concorrência** (Channels ou Pipelines) que passa o evento da thread que recebe para uma thread de trabalho. A Microsoft recomenda esses mecanismos para produtor-consumidor, mas eles **acrescentam uma fila e uma troca de thread a cada evento**. Ninguém tinha medido se isso ajuda ou só custa.

**A lacuna.** Os trabalhos relacionados comparam os brokers isoladamente, muitas vezes em Java; nenhum combina RabbitMQ e Kafka com Channels e Pipelines numa avaliação unificada, e **nenhum mede o mecanismo interno contra um grupo de controle sem ele**.

**O que eu fiz.** Uma bancada com **6 combinações** (2 brokers × 3 modos: **Direct**, Channels, Pipelines), onde o Direct é o **grupo de controle** (sem mecanismo interno). Carga real de telemetria de F1 (OpenF1), reamostrada a 100 Hz e com a frota multiplicada, publicada em **malha aberta** de 10 mil a 400 mil eventos/s. Dois cenários de configuração: **padrão** (valores de fábrica) e **ajustado** (recomendação de cada fornecedor). 10 repetições por combinação e carga, regras de análise fixadas **antes** da coleta.

**O que eu descobri.**
1. **Channels e Pipelines não fizeram diferença no desempenho e pioraram o custo.** Latência, throughput, saturação e memória iguais nos 3 modos; CPU do consumidor de **1,9 a 3,3×** maior no Kafka (+4 a 9% no RabbitMQ). Motivo: o processamento custa **0,12 µs por evento**; quem limita é o broker; não há trabalho acumulado para paralelizar.
2. **Entre os brokers, a diferença é grande e depende da carga.** RabbitMQ tem a menor latência em carga baixa (P99 **2,3 ms** vs **5,8 ms** a 10 mil/s) mas **satura em 60 mil** (CPU do broker esgota). Kafka fica estável (P99 ~5,5–6,7 ms) até **≥ 400 mil**, com ~1/7 da CPU de broker, mas tem um piso de ~3 ms no P50 (formação de lotes) e usa mais memória.
3. **A configuração pesa muito.** Mensagem transitória no RabbitMQ: P99 **43–76% menor** e saturação em **80 mil**. `linger.ms` 1 no Kafka: P50 de 3 → 1 ms, mas P99 maior em carga alta e CPU do broker dobrada.
4. **Metodologicamente**, efeitos do ambiente (cota de CPU, posição dos clientes, disco da VM) foram **maiores que as diferenças entre arquiteturas** — por isso núcleos exclusivos, configurações auditadas e regras prévias.
5. **Estudo preliminar:** quando o consumidor **espera algo** (republicar no Kafka e esperar a confirmação), o mecanismo passa a importar: Direct parou em ~**64 mil**, Channels/Pipelines sustentaram **200 mil**.

**Frase-síntese:** *o broker decide o desempenho; o mecanismo interno, nesta carga, decide só o custo.*

**Contribuições** (como estão na Introdução): (i) comparação das 6 combinações sob o mesmo protocolo, com grupo de controle, em dois cenários; (ii) protocolo de medição com critérios de validade e análise registrados antes da coleta; (iii) bancada aberta (código, scripts e dados no GitHub `ramosnvy/pitwall`; tags `cenario-padrao-v1.2` e `cenario-ajustado-v1.1`).

---

## 2. Conceitos que você precisa dominar

### 2.1 Medição de desempenho

| Conceito | Explicação curta | Por que importa no TCC |
|---|---|---|
| **Latência** | Tempo do envio do evento pelo gerador até o fim do processamento no consumidor. | É a métrica principal de comparação. |
| **P50 / P99** | Percentis: P50 é a mediana (metade dos eventos abaixo); P99 é o valor abaixo do qual ficam 99% dos eventos. | Sistemas de eventos são limitados pela **cauda** (P99), não pela média (Fu et al., 2021). |
| **Throughput** | Eventos processados por segundo. | Em malha aberta, ele **é igual à carga** enquanto o sistema aguenta — por isso não distingue as combinações. |
| **Ponto de saturação** | Maior carga sustentada antes de fila e latência crescerem sem limite (Jain, 1991). No TCC: maior carga em que a maioria das rodadas é válida, throughput ≥ 99% da carga e P99 < 50 ms, com todas as menores também sustentadas. | É o que de fato separa as combinações em "capacidade". |
| **Malha aberta** (open loop) | O gerador envia cada evento **no instante previsto**, independentemente de o sistema ter respondido. | Um gerador de **malha fechada** desaceleraria quando o sistema atrasa e **deixaria de medir justamente os eventos mais atrasados**. |
| **Coordinated omission** | Esse erro de medição da malha fechada (Tene, 2015). | Justifica a malha aberta. |
| **Relógio monotônico** | Contador que só avança (no código, `Stopwatch` do .NET); não sofre ajuste de hora. | O gerador grava na mensagem o instante da publicação; o consumidor compara com o **mesmo** relógio. Como tudo roda na mesma máquina, **não precisa sincronizar relógios**. |
| **HdrHistogram** | Biblioteca de histograma de alta faixa dinâmica, feita para medir latência com precisão nos percentis altos. | Onde as latências são registradas. |

### 2.2 Mensageria

| Conceito | Explicação |
|---|---|
| **Broker** | Servidor intermediário que recebe mensagens de produtores e entrega a consumidores, desacoplando-os. |
| **Kafka** | Plataforma de streaming baseada em **log particionado** (Kreps et al., 2011). Grava em sequência, trabalha em **lotes**; ordem garantida **dentro de cada partição**; grava no **cache de páginas do SO** e deixa a durabilidade para a **replicação** entre nós. Versão usada: **4.1.0** (sem ZooKeeper, modo KRaft). |
| **RabbitMQ** | Broker de uso geral baseado no protocolo **AMQP**, com roteamento por **exchanges** e **filas**; trata **cada mensagem individualmente**. Mensagem **persistente** só é confirmada depois de gravada em disco. Versão: **4.3.6**, **filas clássicas**. |
| **Partição / fila / faixa** | No TCC, "faixa" é o nome comum para **partição do Kafka** e **fila do RabbitMQ**. São 4 faixas. |
| **At-least-once** | Nenhuma mensagem se perde, mas uma pode chegar mais de uma vez. O broker confirma ao produtor (`acks=all` no Kafka; **publisher confirms** no RabbitMQ) e o consumidor confirma ao broker **só depois de tratar**. |
| **`acks=all`** | O produtor Kafka só considera a mensagem enviada quando todas as réplicas em sincronia a têm (aqui, 1 nó). |
| **Publisher confirms** | Equivalente no RabbitMQ: o broker confirma ao produtor que recebeu. No TCC, em lotes de 12.500, dois lotes "em voo" por faixa. |
| **`linger.ms`** | Quanto o produtor Kafka **espera para juntar mensagens num lote**. Padrão de fábrica no Kafka 4.x: **5 ms**. Mais espera → lotes maiores, menos CPU no broker, mais latência mínima. |
| **Prefetch** | No RabbitMQ, quantas mensagens o broker pode entregar ao consumidor sem confirmação. Ajustado: 300. |
| **Mensagem transitória** | No RabbitMQ, não é gravada em disco antes de confirmar — fica com a mesma garantia que o Kafka já tinha. |

### 2.3 Concorrência no .NET

| Conceito | Explicação |
|---|---|
| **Callback** | Função do consumidor que a **biblioteca cliente** do broker chama para entregar cada mensagem, numa thread própria (a **thread do cliente**). |
| **Direct** | O callback chama o processamento **ali mesmo**, na thread do cliente. Sem fila, sem troca de thread. **Grupo de controle.** |
| **Channels** (`System.Threading.Channels`) | Fila **thread-safe de objetos** dentro do processo. No TCC: o callback **desserializa** o evento (vira objeto `TelemetryEvent`) e escreve num `Channel` **limitado a 10.000 eventos** por faixa; um leitor numa thread de trabalho lê e processa. |
| **Pipelines** (`System.IO.Pipelines`) | **Fluxo de bytes** com buffers reaproveitados. No TCC: o callback **só copia os 46 bytes** para um `Pipe`; o leitor interpreta os registros **no próprio buffer**, sem criar um objeto por evento. O `Pipe` pausa o escritor com **460.000 bytes** pendentes (= 10.000 × 46, o mesmo volume do Channel). |
| **Contrapressão** (backpressure) | Quando a fila interna enche, o escritor (callback) espera; isso faz o consumidor parar de pedir ao broker, e a pressão "volta" até a origem. No código: `BoundedChannelFullMode.Wait` e `pauseWriterThreshold` do Pipe. Descartar mensagem invalidaria a rodada, então **esperar é a única opção correta**. |
| **SingleReader** | Há **um único leitor por faixa** (4 no total). |
| **Coleta de lixo (GC)** | O .NET libera memória automaticamente em "gerações"; coletas de **geração 2** são as caras. No TCC foram **no máximo uma por rodada**. |

### 2.4 Estatística

| Conceito | Explicação | Por que foi usado |
|---|---|---|
| **Mediana** | Valor do meio. | Robusta a valores extremos; cada combinação é resumida pela mediana das rodadas. |
| **Regra de Tukey** (IQR) | Descarta valores fora de [Q1 − 1,5·IQR; Q3 + 1,5·IQR], onde IQR = Q3 − Q1. | Remove rodadas discrepantes (ex.: um episódio de disco da VM) de forma objetiva e fixada antes. Aplicada ao P99, **por combinação**. Removeu 32 rodadas (~8%). |
| **Kruskal-Wallis** | Teste **não paramétrico** que compara 3+ grupos pelos postos (ranks), sem supor distribuição normal. | Compara os 3 modos; amostras pequenas (10) e latência não é normal. Nível de 5%. |
| **Tamanho de efeito / limiar de relevância** | Diferença mínima que importa na prática: **20% no P99** e **10% no throughput**. | Com dispersão pequena, **diferenças irrelevantes ficam estatisticamente significativas**; por isso o critério principal é o tamanho do efeito, e o teste só "acompanha" as diferenças maiores. |

---

## 3. A bancada, etapa por etapa

(É a **Figura 1** do texto / slide 6.)

### Etapa 1 — Dados (OpenF1)
- **O quê:** um programa próprio baixa da OpenF1, **uma única vez**, a telemetria de nove corridas de 2024 e grava cópia local. A API **não é chamada durante os experimentos**. Todas as rodadas usam a **corrida do Bahrein de 2024**.
- **Dados de cada carro:** velocidade, rotação, marcha, acelerador, freio, DRS, a ~**3,7 Hz** (≈ 75 eventos/s com 20 carros).
- **Por quê OpenF1:** medições **reais**, de **muitas fontes simultâneas com sequências ordenadas** (como uma frota de IoT), e **pública** → reproduzível. O trabalho **não analisa as corridas**; F1 é só a fonte de carga.

### Etapa 2 — Ingestão (o Gerador)
- Usa **130 s** da corrida a partir do **10º minuto**.
- **Reamostra de 3,7 para 100 Hz:** velocidade, rotação e acelerador interpolados **em linha reta**; marcha, freio e DRS **em degrau** → as frenagens e trocas de marcha são **as da corrida original**.
- **Multiplica a frota:** cada réplica ganha número de carro próprio e um **deslocamento de fase**; **100 mil eventos/s = 1.000 carros**. Assim chega a centenas de milhares de eventos/s **sem comprimir o tempo** (comprimir mudaria a semântica das janelas de 1 s).
- Publica **em malha aberta**, no instante previsto, e registra o **maior atraso de envio** (critério de validade).
- Cada evento: **registro binário de 46 bytes**, tamanho fixo (por isso o Pipelines não precisa de prefixo de tamanho — e não paga um cabeçalho que o Channels não pagaria).

### Etapa 3 — Mensageria
- **Um broker por rodada, com um único nó.**
- Carga dividida em **4 faixas** (4 partições no Kafka, 4 filas clássicas no RabbitMQ).
- O carro define a faixa **pela mesma função nos dois brokers**: **CRC32 da chave**, que é a função de fábrica do cliente Kafka. Resultado: **mesma distribuição nos dois** e **eventos de cada carro chegam em ordem**.
- Entrega **at-least-once** nos dois.

### Etapa 4 — Processamento (o coração do trabalho)
- **Um consumidor por faixa.** O que muda entre os modos é **só o caminho entre o callback e o código de processamento** (quadro de baixo da Figura 1; verde = thread do cliente, azul = thread de trabalho, cinza = fila interna).
- **Direct:** callback → processa. Grupo de controle. *Sem ele, uma diferença entre "Kafka com Channels" e "RabbitMQ com Channels" mostraria que o broker importa, mas não se o Channels ajudou ou atrapalhou.*
- **Channels:** callback → desserializa → `Channel` (10.000) → leitor → processa.
- **Pipelines:** callback → copia bytes → `Pipe` (460.000 B) → leitor interpreta no buffer → processa.
- Em Channels e Pipelines o **callback não espera o processamento**; só espera se a fila interna encher (contrapressão).
- **Um único leitor por faixa**, porque **dois leitores na mesma faixa poderiam processar fora de ordem os eventos de um mesmo carro**, e a detecção depende da ordem. O paralelismo do consumidor vem das **4 faixas**.
- **O processamento** (o mesmo código nos três modos), seguindo operações de referência de benchmarks de fluxos (**Linear Road**, Arasu 2004; **DEBS 2013 Grand Challenge**, Mutschler 2013):
  - **agregação em janelas de 1 s por carro, em tempo do evento** (tempo do evento, não de processamento — é o que torna o resultado idêntico nas 6 combinações);
  - **detecção de frenagens e trocas de marcha** sobre eventos consecutivos do mesmo carro (frenagem: freio cruza o limiar de 50; troca: marcha muda e é > 0).
- **Digest:** cada rodada resume o resultado num digest — contagem de janelas, eventos, frenagens, trocas, soma das velocidades máximas e um **hash FNV-1a por janela combinado por XOR**. Soma e XOR são **comutativas**: o digest independe da ordem de fechamento das janelas e de quantas threads existiam. A velocidade **média** fica de fora de propósito (ponto flutuante; a ordem das somas poderia mudar o último bit sem erro nenhum). **Mesma entrada → mesmo digest nas 6 combinações; divergência = perda, duplicação ou quebra de ordem.**

### Etapa 5 — Persistência e métricas
- Resultados das janelas → **PostgreSQL 17**, **em lote**, por **`COPY` binário**, em tabelas **`UNLOGGED`** (sem log de transação) → o banco não é gargalo. **A latência termina antes da gravação**, e uma rodada em que a gravação descarta janelas é **inválida**.
- **Prometheus + cAdvisor** leem CPU e memória **por container**.
- Latências em **HdrHistogram**; alocações e coletas de lixo dos **contadores do runtime do .NET**.

### Ambiente
- **Intel Core i5-12400F** (6 núcleos, 12 threads), **16 GB**, **Windows 11**, **Docker Desktop 4.91 sobre WSL2**; a VM tem 12 núcleos lógicos e 8 GB.
- **Núcleos exclusivos (cpuset):** gerador **0–3**, broker **4–7**, consumidor **8–10**, banco e métricas **11**. Brokers com 3 GB de memória.
- Durante as medições, **só o broker medido fica ligado** e o computador não é usado.
- Software: **.NET 10**, Confluent.Kafka 2.15.1, RabbitMQ.Client 7.2.2, Npgsql 10.0.3, HdrHistogram 2.5.0.

---

## 4. Configuração dos brokers e os dois cenários

### 4.1 Cenário padrão
**Regra:** valores **de fábrica**, **conferidos na fonte de cada versão**, alterados **só onde impediam a equivalência** entre os brokers. Por quê: ninguém pode dizer que um broker foi "ajustado a favor".

| Componente | Parâmetro e valor | Motivo |
|---|---|---|
| Ambos | 4 faixas, carro por CRC32; **compressão desligada** | Ordem por carro; o conteúdo repetido das réplicas da frota **favoreceria o Kafka** (que comprime lotes) |
| Ambos | Até 100 mil mensagens sem confirmação no gerador | Limite equivalente; é o valor de fábrica do Kafka |
| Kafka | `acks=all`; offset guardado **só depois da entrega ao processamento**; 1 nó | At-least-once; máquina única |
| RabbitMQ | Publisher confirms em lotes de 12.500, dois em voo por faixa; **mensagem persistente** | Equivalente ao `acks=all` |
| RabbitMQ | Uma conexão por faixa; confirmação do consumidor em lotes de 100 | Recomendação do fornecedor |
| RabbitMQ | Memória considerada de 3 GB | O valor de fábrica usaria a memória da VM inteira |

**A assimetria que fica (e você precisa saber explicar):** com a mensagem persistente, o **RabbitMQ grava em disco antes de confirmar**; o **Kafka confirma com a mensagem no cache do SO**, como a própria documentação do Kafka recomenda (durabilidade pela replicação). Na calibração, essa gravação custou **48% no P99 do RabbitMQ a 40 mil**. Ela **favorece o Kafka no cenário padrão** — e é exatamente por isso que existe o **cenário ajustado**, que iguala as garantias. Está declarado no texto (Configuração e Ameaças à Validade).

### 4.2 Cenário ajustado
**Regra:** a **recomendação de cada fornecedor**, mantendo o at-least-once.
- **Kafka:** `linger.ms` de **5 → 1 ms** (recomendação para latência) e um **perfil para vazão** com **20 ms**.
- **RabbitMQ:** mensagem **transitória** (iguala a garantia à do Kafka) e **prefetch 300**.
- **`linger.ms` 0 foi descartado** num diagnóstico antes da coleta: com ele, o **P50 do Kafka passou de 400 ms já a 10 mil eventos/s**.

---

## 5. Metodologia: protocolo, regras e calibração

### 5.1 Tipo de pesquisa
**Experimental e quantitativa** (Jain, 1991). Calibração exploratória → correções → **cenário congelado** → coleta oficial com análise **registrada antes de ver os dados**.

### 5.2 Fatores e níveis
- **Broker** (2) × **modo** (3) × **carga**:
  - RabbitMQ: 10, 20, 40, 60, 80 mil eventos/s.
  - Kafka: 10, 20, 40, 60, 100, 200, 300, 400 mil.
  - Faixas tiradas de **varreduras de saturação**, com **ao menos uma carga acima do ponto de saturação** de cada broker.
- Cada rodada: **10 s de aquecimento (descartados) + 90 s de medição**.
- **10 repetições** por combinação e carga, em **ordem sorteada**.
- Cenário padrão: 5 cargas × 3 modos × 10 = 150 rodadas RabbitMQ; 8 × 3 × 10 = 240 Kafka → **390**.
- **Bateria de sensibilidade:** modo Direct a **3,7 Hz** (sem interpolação), em duas cargas por broker.
- **Cenário ajustado:** Direct em todas as cargas; Channels e Pipelines em **duas cargas por broker** (o efeito do mecanismo já é medido no padrão); perfil para vazão só no Direct, de 200 a 400 mil. **240 rodadas, 232 válidas.**

### 5.3 Métricas (três grupos, sempre para as 6 combinações)
1. **Latência:** P50 e P99, do envio ao fim do processamento.
2. **Throughput** e **ponto de saturação**.
3. **Recursos:** CPU do consumidor, do broker e do gerador (em núcleos; 1,0 = um núcleo inteiro), memória de pico do consumidor e do broker, e custo do .NET (bytes alocados por evento, coletas de lixo).

### 5.4 Regras fixadas antes da coleta
- **Rodada inválida** se: atraso máximo de envio > **50 ms**; **digest** diverge do de referência; registro incompleto no `Pipe`; persistência descarta janelas. Rodadas inválidas **não são repetidas**.
- **Discrepantes:** regra de **Tukey** sobre o P99, por combinação; resumo pela **mediana** das demais.
- **Carga sustentada:** maioria das rodadas válida, throughput ≥ **99%** da carga e P99 < **50 ms**. **Ponto de saturação:** maior carga sustentada com todas as menores também sustentadas.
- **Relevância:** diferença entre modos < **10% no throughput** ou < **20% no P99** = sem relevância. **Kruskal-Wallis a 5%** só acompanha as diferenças maiores.

### 5.5 Calibração (o que ela achou e que decisão gerou)

| Achado | Decisão que gerou |
|---|---|
| Gerador e consumidor **fora dos containers** → Kafka com **latência bimodal em 43%** das rodadas de carga baixa | Tudo em containers |
| **Cota de CPU** estrangulava o broker RabbitMQ; **núcleos exclusivos reduziram o P99 dele de 40% a 64%** | Núcleos exclusivos (cpuset) por componente |
| Auditoria contra os valores de fábrica: **3 defeitos de configuração e 3 assimetrias** entre os brokers | Corrigidos antes da coleta; tabela de configuração |
| A partir de 100 mil/s, Kafka com episódios de **200 ms a 2 s** sem confirmar nem entregar, coincidindo com a gravação periódica de páginas no disco virtual; logo após uma parada da VM por espera de disco, chance de latência > 50 ms **126× maior** | Atraso de envio como critério de validade; 10 repetições, mediana e Tukey para que um episódio isolado não decida a saturação |
| `linger.ms` 0 → P50 de **400 ms** a 10 mil | Descartado do cenário ajustado |
| Gravação em disco do RabbitMQ custou **48%** no P99 a 40 mil | Declarada como assimetria; cenário ajustado iguala |

**Tudo foi corrigido ou registrado antes da coleta oficial, e esses dados não entram na análise.** A mensagem metodológica: **o ambiente pesou mais que a arquitetura**.

---

## 6. Resultados, um por um, com o porquê

### 6.1 Validade das rodadas (cenário padrão, 100 Hz)

| Broker | Modo | Rodadas | Inválidas | Descartadas (Tukey) | Usadas |
|---|---|---|---|---|---|
| RabbitMQ | Direct | 50 | 0 | 2 | 48 |
| RabbitMQ | Channels | 50 | 0 | 3 | 47 |
| RabbitMQ | Pipelines | 50 | 0 | 6 | 44 |
| Kafka | Direct | 80 | 4 | 7 | 69 |
| Kafka | Channels | 80 | 4 | 6 | 70 |
| Kafka | Pipelines | 80 | 4 | 8 | 68 |
| **Total** | | **390** | **12** | **32** | **346** |

- **Todas as 12 inválidas são do Kafka a ≥ 200 mil**, por **atraso de envio** (> 50 ms), coincidindo com os episódios de disco da VM. Repare: **4 por modo** — afeta os três igualmente, logo não é efeito do mecanismo.
- **Zero divergências de digest**: nenhum evento perdido, duplicado ou fora de ordem em nenhuma combinação.
- Tukey descartou **32 (~8%)**.

### 6.2 Latência e throughput (Figura 2)

**Entre os brokers — a latência muda de sinal com a carga:**
- 10 mil/s: **RabbitMQ P50 0,36 ms e P99 2,3 ms** vs **Kafka P50 3,0 ms e P99 5,8 ms**.
- Cruzamento: no **P50**, RabbitMQ mais rápido **até 60 mil**; no **P99**, só **até 20 mil**.
- RabbitMQ P99 cresce: **25 ms a 60 mil**, **75 ms a 80 mil** (acima do limite → não sustenta).
- Kafka P99 entre **5,5 e 6,7 ms até 400 mil**.
- **Por quê o piso do Kafka (~3 ms no P50):** é o `linger.ms` = 5 ms, a espera para formar lotes. O Kafka troca alguns ms de latência por lotes maiores e menos CPU no broker.
- **Por quê a cauda do RabbitMQ cresce:** sem espera de lote, ele é rápido em carga baixa; mas, com a CPU do broker perto do limite, acumula fila e a cauda cresce.

**Entre os modos — as três linhas de cada broker ficam sobrepostas:**
- **Maior diferença no P99: 11%** (Kafka Pipelines a 400 mil — e para **pior**), abaixo do limite de 20%.
- RabbitMQ: Channels/Pipelines só abaixo do Direct a **40 mil**, **P99 6% menor**, **sem significância (Kruskal-Wallis p = 0,06)**.
- Kafka: **nunca** ficaram abaixo do Direct.
- **Bateria a 3,7 Hz:** P99 mudou **menos de 10%** em relação a 100 Hz → **a interpolação não muda as conclusões**.

**Throughput — as seis linhas coincidem:**
- Em malha aberta, toda combinação processa **exatamente a carga oferecida** enquanto a sustenta (diferença < 0,1%). O throughput medido **não distingue**; o que distingue é o **ponto de saturação**, decidido pela latência.
- **RabbitMQ sustenta 60 mil** nos 3 modos (5,4 milhões de eventos por rodada de 90 s) e **falha a 80 mil** (P99 > 50 ms).
- **Kafka sustenta 400 mil** nos 3 modos (36 milhões por rodada) — **o ponto de saturação dele fica acima do alcance da bancada** (não foi localizado; só limitado por baixo).
- **Em nenhum broker o modo mudou o ponto de saturação.**

### 6.3 Uso de recursos (Figura 3)

**CPU do consumidor — a única medida em que os modos se separam (e para pior):**
- **Kafka:** Direct **0,09 a 0,63 núcleo**; Channels e Pipelines **0,17 a 1,69** → **1,9 a 3,3×**, Pipelines sempre um pouco acima do Channels (3–10%).
- A partir de 100 mil/s, o acréscimo é de **2,4 a 3,7 µs por evento** — a ordem de grandeza de **acordar uma thread de trabalho a cada evento**.
- **RabbitMQ:** consumidor perto de **1 núcleo** em qualquer carga; mecanismos acrescentam só **4% a 9%**. **[sugestão]** Por que tão pouco em termos relativos: a base já é alta (a biblioteca cliente do RabbitMQ gasta muito por mensagem), então o custo extra do mecanismo pesa pouco em proporção.

**CPU do broker — o que separa os brokers e explica a saturação:**
- **RabbitMQ: 2,8 a 3,5 dos 4 núcleos** → esgota primeiro. Trata cada mensagem individualmente: roteia, confirma e, persistente, grava em disco antes de confirmar.
- **Kafka: 0,4 a 0,5 núcleo** até 400 mil. Trata lotes: agrupa por partição, grava em sequência no log, entrega em lotes.
- Mesmo padrão no **gerador**: **3,1 núcleos** para publicar no RabbitMQ a 60 mil, contra **1,1** no Kafka.

**Memória — não depende do modo:**
- Consumidor: **87 a 117 MB** em todas as combinações.
- Broker: **Kafka 1,4 a 1,7 GB** vs **RabbitMQ 0,2 a 0,3 GB** (o Kafka troca CPU por memória: cache de páginas, lotes).
- **Alocação igual nos três modos:** **952–975 bytes/evento no Kafka**, **720–834 no RabbitMQ** (~20× o tamanho da mensagem), porque **quem aloca é a biblioteca cliente, antes de o evento chegar ao mecanismo**. Por isso o Pipelines, que evita criar objeto por evento, **não economizou memória** aqui.
- Coletas de GC de geração 2: **no máximo uma por rodada**.

### 6.4 Por que os modos não se separam (o argumento central — domine isto)

1. **Quem limita o fluxo é o broker**, não o processamento local.
2. Processar um evento custa **~0,12 µs**; o consumidor termina um evento **muito antes** de o broker entregar o seguinte.
3. **Sem trabalho acumulado no consumidor**, o mecanismo interno **não tem o que paralelizar nem espera a esconder**.
4. Desacoplar recepção e processamento **só acrescenta** a fila interna e a troca de thread (2,4–3,7 µs/evento — **20 a 30× o próprio processamento**).
5. **Mais leitores por faixa também não mudariam nada:** no Kafka, o Direct inteiro (recepção + processamento) usa de **0,09 a 0,63 dos 3 núcleos** do consumidor — sobra CPU.
6. **Logo, o resultado vale para processamento leve, um evento por vez.** Se o processamento fosse mais lento que a entrega do broker (ou esperasse algo), o paralelismo local poderia fazer diferença → trabalho futuro e estudo preliminar.

> **Analogia [sugestão]:** é como contratar um ajudante para passar pratos a quem lava, quando a pessoa lava cada prato num piscar de olhos e os pratos chegam devagar da mesa. O ajudante não acelera nada — só dá mais uma mão de obra para pagar. Ele só ajuda quando lavar demora mais que trazer, ou quando quem lava precisa parar para esperar algo.

### 6.5 Cenário ajustado (Figura 4)
- **240 rodadas, 232 válidas**; as 8 inválidas de novo por atraso de envio no Kafka a ≥ 200 mil.
- **RabbitMQ** (transitória + prefetch 300): P99 caiu **43% a 76%** em todas as cargas — de **2,3 → 0,65 ms a 10 mil** e de **25 → 10 ms a 60 mil**; saturação **60 → 80 mil** (P99 de 43 ms a 80 mil). Sem gravação em disco, o broker gasta menos CPU em carga baixa e, em carga alta, usa a CPU liberada para entregar mais rápido.
- **Kafka `linger.ms` 1:** P50 **3 → 1 ms** em todas as cargas (**confirma que o piso vinha da formação de lotes**). Mas o P99 só caiu a 10 mil; a partir de 40 mil **subiu 28% a 296%** (até 22 ms a 300 mil), porque lotes menores = mais requisições ao broker, e a **CPU do broker dobrou**. Saturação continua > 400 mil.
- **Kafka perfil para vazão (`linger.ms` 20):** o inverso — P99 ≈ **21 ms** e CPU do broker **~30% menor**.
- **Mecanismos:** continuaram **sem efeito relevante de latência** (maior diferença 18%, Kruskal-Wallis sem diferença, p > 0,7) e custando **1,9 a 2,3×** a CPU do Direct no Kafka.
- **Os brokers se aproximam:** RabbitMQ ajustado tem o menor P99 **até 40 mil**; a 60 mil ficam próximos (10,4 vs 8,3 ms).
- **Conclusão:** parte da diferença do cenário padrão vinha da configuração; **cada parâmetro troca latência por CPU do broker**.

### 6.6 Onde cada broker e cada mecanismo ganha (Tabela de síntese)

| Broker | Modo | Onde se sai melhor | O que ganha | O que custa |
|---|---|---|---|---|
| RabbitMQ | Direct | Carga baixa: até 20 mil (P99) e 60 mil (P50) | Menor latência da bancada: P99 2,3 ms a 10 mil | Broker no limite de CPU; satura em 60 mil |
| RabbitMQ | Channels | 40 mil | P99 6% menor, sem relevância | CPU 1,04–1,07× |
| RabbitMQ | Pipelines | 40 mil | P99 6% menor, sem relevância | CPU 1,06–1,09× |
| Kafka | Direct | Carga alta: 40 a 400 mil | P99 5,5–6,1 ms até 400 mil, 0,4–0,5 núcleo de broker | P50 de 3 ms; mais memória no broker |
| Kafka | Channels | Nenhuma carga | Nada (P99 0–9% acima do Direct) | CPU 1,9–3,0× |
| Kafka | Pipelines | Nenhuma carga | Nada (P99 0–11% acima do Direct) | CPU 2,0–3,3× |

**Onde cada mecanismo "se encaixa"** (atenção: **medido** vs **deduzido**):
- **RabbitMQ Direct:** processamento leve por evento — **medido**.
- **RabbitMQ Channels:** cargas intermediárias, quando o broker começa a acumular fila; a 40 mil foi o único caso com P99 abaixo do Direct, a custo de só 4–7% de CPU. Candidato natural quando o processamento tem **espera própria** (gravar, chamar serviço) — **deduzido**.
- **RabbitMQ Pipelines:** como o Channels, com um pouco mais de CPU; encaixa-se quando a entrada é **fluxo de bytes a interpretar em lote** — **deduzido** do propósito.
- **Kafka Direct:** toda a faixa medida — **medido**. O cliente entrega de uma fila local, o processamento termina antes do próximo pedido.
- **Kafka Channels:** sem cenário favorável na bancada; encaixa-se quando o processamento é **bem mais longo** que o custo da troca de thread ou **espera algo** — **deduzido** (e apoiado pelo estudo preliminar).
- **Kafka Pipelines:** o mais caro (3–10% acima do Channels); só quando o processamento trabalha **sobre bytes em lote** — **deduzido**.

> O texto diz explicitamente: *os cenários de Channels e Pipelines fora da bancada são deduzidos do custo medido e do propósito de cada ferramenta, e não foram medidos.* Se a banca perguntar, **diga isso com tranquilidade** — é honestidade científica.

---

## 7. Comparação com a literatura e ameaças à validade

### 7.1 Comparação com os trabalhos relacionados

| Trabalho | O que diz | O que o TCC encontrou |
|---|---|---|
| **Umam et al. (2026)** e **Dobbelaere e Esmaili (2017)** | RabbitMQ para mensagens individuais com baixa latência; Kafka para alto volume | **Concorda**, e acrescenta **as condições**: RabbitMQ com menor P99 até 20 mil (padrão) e até 40 mil (ajustado); Kafka sustentou **≥ 5×** a carga, com ~**1/7** da CPU de broker |
| **Kamiński, Kłonica e Pańczyk (2025)** | Kafka superior em .NET | **Confirma, mas só em carga alta**; em carga baixa o RabbitMQ foi mais rápido nos dois cenários |
| **Ortiz et al. (2023)** | A camada de processamento acoplada ao broker afeta latência e escalabilidade | Aqui, com processamento **leve na memória**, o mecanismo mudou **só o custo em CPU** |
| **Fu, Zhang e Yu (2021)** | Comparação justa exige controlar ambiente, equivalência e repetição | **Confirma:** posição dos clientes, cota de CPU e disco da VM tiveram efeitos **maiores** que as diferenças entre arquiteturas |
| **Todos** | — | **Nenhum** mede o mecanismo interno contra um **grupo de controle**; é essa comparação que separa o efeito do broker do efeito do mecanismo |

### 7.2 Ameaças à validade (saiba as quatro e a resposta para cada)

**Validade interna** (o efeito medido é mesmo da arquitetura?)
- Tudo numa máquina virtualizada → dispensa sincronizar relógios, mas **não exercita replicação nem rede**, que são parte do que justifica o Kafka. **Conclusões valem para nó único.**
- Mitigações: núcleos exclusivos, limites de memória, ordem sorteada.
- Disco da VM causa episódios no Kafka ≥ 100 mil → 10 repetições, mediana e Tukey impedem que um episódio isolado decida a saturação.
- **Assimetria de durabilidade favorece o Kafka no padrão** → o ajustado iguala.
- Persistência não é gargalo (lote, latência medida antes, rodada com janela descartada é inválida).
- Malha aberta + critério de atraso de envio evitam coordinated omission.

**Validade de construto** (estou medindo o que digo medir?)
- Pontos a 100 Hz são **interpolados**, não medidos → bateria a 3,7 Hz mostra que não muda as conclusões.
- **Compressão desligada** (réplicas quase idênticas); em produção o Kafka costuma usá-la.
- Mensagens de 46 bytes e processamento leve → experimento **dominado pelo transporte**; processamento com espera, mensagens maiores ou leitura em lote **podem favorecer** Channels e Pipelines.

**Validade de conclusão** (a estatística sustenta?)
- Rodada isolada não sustenta conclusão → 10 repetições, mediana, regras prévias.
- Tamanho de efeito fixado antes (com dispersão pequena, o irrelevante fica significativo).
- **Saturação do Kafka não localizada**, só limitada por baixo (400 mil).
- No ajustado, Channels e Pipelines medidos em **duas cargas por broker**.

**Validade externa** (generaliza?)
- Vale para **Kafka 4.1.0, RabbitMQ 4.3.6 com filas clássicas, .NET 10**, **um nó de cada broker** e o hardware descrito.

---

## 8. Conclusão, estudo preliminar e trabalhos futuros

**Pergunta central:** acrescentar Channels ou Pipelines ao consumidor melhora, piora ou não muda o desempenho?
**Resposta:** **não fez diferença no desempenho e piorou o custo** — latência, throughput, ponto de saturação e memória equivalentes nos dois brokers, em todas as cargas e nos dois cenários; nenhuma diferença de P99 passou de 20%; a única diferença foi negativa, na CPU do consumidor.

**Entre os brokers:** RabbitMQ para carga baixa com latência mínima; Kafka para carga alta, com menos CPU de broker, em troca de um piso de latência e mais memória. Parte da diferença vem da configuração.

**Recomendação prática:**
1. Escolher o broker **pela faixa de carga**.
2. Ajustar a espera de lote do Kafka e a persistência do RabbitMQ **conforme a latência que se quer pagar em CPU**.
3. Usar o **Direct** quando o processamento é **leve e não espera nada**.

**Metodológica:** virtualização, limitação de CPU e configuração **pesaram mais** que a arquitetura; comparação justa exigiu **núcleos exclusivos, configurações auditadas e análise registrada antes**.

**Estudo preliminar** (não faz parte do texto; está no repositório): o consumidor passou a **republicar o resultado no broker e esperar a confirmação** antes de seguir.
- **Kafka:** Direct não passou de ~**64 mil/s**; **Channels e Pipelines sustentaram 200 mil** (maior carga testada).
- **RabbitMQ:** o limite continuou sendo o broker → os três modos **equivalentes**.
- **Leitura:** no Direct, a espera bloqueia a thread do cliente, que para de receber; com Channels/Pipelines, a recepção continua enquanto a thread de trabalho espera. **Quando o processamento local espera algo, o mecanismo interno pode fazer diferença.**

**Trabalhos futuros:** aprofundar esse estudo (processamento mais pesado por evento, mais de um leitor por faixa); vários nós com replicação; filas de quórum e streams do RabbitMQ; mensagens maiores; Channels e Pipelines em lote.

---

## 9. Banco de perguntas da banca, com respostas

> Formato de resposta que funciona bem: **resposta direta em uma frase → o porquê → o dado ou a referência**. Se não souber, diga o que mediu e o que não mediu.

### 9.1 Sobre o problema e a motivação

**1. Por que esse tema é relevante para Engenharia de Software?**
Porque é uma **decisão de arquitetura** que times tomam o tempo todo (qual broker, se usa ou não um mecanismo de concorrência) e que costuma ser tomada por recomendação ou costume, não por evidência. O trabalho dá evidência medida, com protocolo reproduzível, e mostra que um componente recomendado pode **só custar** em certos cenários. **[sugestão]**

**2. Por que .NET?**
Porque Channels e Pipelines são as ferramentas **da plataforma .NET** para produtor-consumidor, e as comparações de brokers costumam ser em Java; numa aplicação .NET o desempenho percebido é o da **cadeia inteira** (Introdução).

**3. Por que comparar só RabbitMQ e Kafka?**
São os dois brokers mais usados e os mais comparados na literatura (Dobbelaere, Kamiński, Umam), com modelos opostos — mensagem individual vs log em lotes —, o que torna a comparação informativa. **[sugestão — o texto justifica pela literatura]**

**4. Por que Fórmula 1? O trabalho é sobre F1?**
Não; o trabalho **não analisa as corridas**. A OpenF1 dá dados **reais, públicos e reproduzíveis**, com **muitas fontes simultâneas e ordenadas** — o mesmo formato de uma frota de IoT ou de monitoramento.

### 9.2 Sobre a bancada e as decisões

**5. Por que ter o modo Direct?**
É o **grupo de controle**. Sem ele, eu só poderia comparar "Kafka com Channels" contra "RabbitMQ com Channels" e saberia que o broker importa, mas **não se o Channels ajudou ou atrapalhou**. É a contribuição que nenhum trabalho relacionado tem.

**6. Por que malha aberta?**
Porque um gerador de malha fechada **desacelera quando o sistema atrasa** e deixa de medir justamente os eventos mais atrasados — a *coordinated omission* (Tene, 2015). Em malha aberta, cada evento sai no instante previsto e a cauda aparece.

**7. Como você garantiu que o gerador não era o gargalo?**
Pelo **critério de validade de atraso de envio**: se o gerador atrasa mais de 50 ms em relação à taxa pedida, a rodada é **inválida**. No RabbitMQ, **nenhuma** rodada foi inválida, então o gerador acompanhou até a saturação; além disso, ele tem 4 núcleos exclusivos (usou 3,1 a 60 mil no RabbitMQ). No Kafka, as 12 inválidas a ≥ 200 mil coincidiram com os episódios de disco da VM.

**8. Por que interpolar para 100 Hz? Isso não "inventa" dados?**
Para chegar a centenas de milhares de eventos/s **sem comprimir o tempo** (o que mudaria as janelas de 1 s). Os canais discretos (marcha, freio, DRS) ficam **em degrau**, então frenagens e trocas são as reais; os contínuos são interpolados em linha reta. E a **bateria a 3,7 Hz**, sem interpolação, mudou o P99 em **menos de 10%**: a interpolação **não muda as conclusões**.

**9. Por que multiplicar a frota?**
Para gerar carga alta mantendo a semântica por carro: cada réplica tem número próprio e um deslocamento de fase. 100 mil eventos/s = 1.000 carros.

**10. Por que 4 faixas?**
O paralelismo do consumidor vem das faixas, e o número é o mesmo nos dois brokers (4 partições = 4 filas). **[sugestão]** Quatro casa com os 4 núcleos do broker e mantém o consumidor (3 núcleos) com uma thread de recepção por faixa; o essencial é que seja **igual nos dois** e com a **mesma função de distribuição**.

**11. Por que CRC32?**
É a **função de fábrica do cliente Kafka** para escolher a partição pela chave. Usei a mesma no RabbitMQ para que **os dois tenham exatamente a mesma distribuição** de carros por faixa.

**12. Por que um único leitor por faixa? Mais threads não seriam mais rápidas?**
Porque **dois leitores na mesma faixa poderiam processar fora de ordem** os eventos de um mesmo carro, e a detecção de frenagens e trocas depende da ordem. E mais leitores **não mudariam o resultado**: no Kafka, o Direct inteiro usa no máximo 0,63 dos 3 núcleos do consumidor — não falta CPU.

**13. Por que o Channel tem 10.000 e o Pipe 460.000 bytes?**
Para os dois mecanismos terem **o mesmo volume** em fila: 10.000 eventos × 46 bytes = 460.000 bytes. **[sugestão]** O valor exato é um limite generoso para não estrangular o fluxo; o que importa para a justiça é ser igual.

**14. Por que a fila é limitada e espera quando enche, em vez de descartar?**
Para ter **contrapressão** (a fila não cresce até estourar a memória) e porque **descartar mensagem invalidaria a rodada** (o digest divergiria).

**15. Qual a diferença real entre Channels e Pipelines na sua implementação?**
No **Channels**, a thread do cliente **desserializa** o evento num objeto e o coloca na fila; a thread de trabalho processa o objeto. No **Pipelines**, a thread do cliente **só copia os 46 bytes** para o Pipe; a thread de trabalho interpreta os bytes no próprio buffer, sem criar objeto por evento. Um transporta **objetos**, o outro **bytes**.

**16. Por que o Pipelines gastou mais CPU que o Channels, se ele foi feito para ser eficiente?**
O ganho do Pipelines é **evitar cópias e alocações** quando se lê um **fluxo de bytes em lote**. Aqui cada mensagem já chega como um bloco separado pela biblioteca cliente (que já alocou ~950 bytes/evento), e é escrita no Pipe uma a uma; o leitor acorda por evento. Então se paga a gerência do buffer sem o benefício do lote. **[sugestão — o texto registra o fato (3–10% acima) e diz que ele se encaixa em bytes em lote]**

**17. O que é o digest e por que confiar nele?**
É um resumo determinístico do resultado: contagem de janelas, eventos, frenagens, trocas, soma das velocidades máximas e um hash por janela combinado por XOR. Como soma e XOR são comutativas, **não depende da ordem** em que as janelas fecham nem do número de threads. Se as 6 combinações processam a mesma entrada, **têm de produzir o mesmo digest**; qualquer perda, duplicação ou quebra de ordem muda o valor. Inspirado no DEBS Grand Challenge, que avalia também a **correção** do resultado. Resultado: **0 divergências**.

**18. O processamento é muito simples. Isso não torna o experimento irrelevante?**
É uma **limitação declarada**, e ao mesmo tempo é o **caso comum** (filtrar, agregar, detectar padrões em memória). As operações são as de referência de benchmarks de fluxos (Linear Road, DEBS 2013). E o resultado tem uma condição clara: vale **para processamento leve, um evento por vez**. O estudo preliminar mostra o outro lado: quando o processamento espera, o mecanismo passa a importar.

**19. Como mediu a latência sem sincronizar relógios?**
O gerador grava na mensagem o instante da publicação lido de um **relógio monotônico** (`Stopwatch`); o consumidor lê o **mesmo relógio** ao fim do processamento. Como os dois rodam **na mesma máquina**, o contador é comparável entre processos.

**20. O PostgreSQL não interfere?**
Não: grava **em lote** com `COPY` binário em tabelas `UNLOGGED`, roda num núcleo próprio (11), a **latência termina antes da gravação**, e uma rodada em que a persistência descarta janelas é **inválida**.

**21. Por que tudo numa máquina? Isso não invalida a comparação com o Kafka?**
Foi uma restrição de recursos e tem uma vantagem (não precisa sincronizar relógios). A limitação está declarada: **não exercita replicação nem rede**, que fazem parte do que justifica o Kafka; as conclusões valem para **nó único**. Vários nós com replicação é trabalho futuro.

**22. Por que Docker com núcleos exclusivos e não cota de CPU?**
Porque a calibração mostrou que, **com cota, o broker RabbitMQ era estrangulado**; com núcleos exclusivos, o P99 dele caiu **40% a 64%**. E com gerador e consumidor fora dos containers o Kafka teve latência **bimodal em 43%** das rodadas de carga baixa.

### 9.3 Sobre a configuração e a justiça

**23. Como você garantiu que a comparação foi justa?** *(a pergunta mais provável — tenha os 4 blocos na ponta da língua)*
1. **Mesma carga e mesmo trabalho:** mesmos dados, mesmo código de processamento nos 3 modos, mesma função de distribuição nas faixas, fila interna com o mesmo volume; o **digest** prova que todos fizeram o mesmo trabalho.
2. **Mesma garantia de entrega:** at-least-once nos dois (`acks=all` ↔ publisher confirms; confirmação do consumidor só depois de tratar); valores de fábrica **conferidos na fonte de cada versão**, mudados só pela equivalência; **compressão desligada** porque favoreceria o Kafka.
3. **Mesmas condições de execução:** núcleos exclusivos, só o broker medido ligado, ordem sorteada, 10 s de aquecimento, malha aberta.
4. **Análise sem viés:** regras e limiar de relevância **fixados antes da coleta** (Jain; Fu), grupo de controle, 10 repetições, Tukey e mediana.
E cada decisão tem uma base: **documentação do fornecedor, literatura ou uma medição da calibração** (slide de apoio A1).

**24. Mas o RabbitMQ gravava em disco e o Kafka não. Isso é justo?**
É uma **assimetria deliberada e declarada**: no cenário padrão cada broker está **no seu padrão de fábrica** — e o próprio Kafka recomenda confirmar com a mensagem no cache e deixar a durabilidade para a replicação. Ela **favorece o Kafka** e custou 48% no P99 do RabbitMQ a 40 mil na calibração. Por isso existe o **cenário ajustado**, com mensagem transitória, que **iguala as garantias**. Mesmo assim, o RabbitMQ ajustado saturou em **80 mil** e o Kafka seguiu acima de **400 mil**: a assimetria explica parte da latência, **não** a diferença de capacidade.

**25. Por que usar valores de fábrica? Ninguém usa em produção.**
Justamente para **ninguém poder dizer que eu ajustei um broker a favor**. E, como ninguém usa só o de fábrica, existe o **cenário ajustado** com a recomendação de cada fornecedor. Os dois juntos mostram **quanto da diferença é configuração** (até 76%).

**26. Por que não testou `linger.ms` 0?**
Testei num diagnóstico antes da coleta: o **P50 do Kafka passou de 400 ms já a 10 mil**. Foi descartado e registrado.

**27. Por que filas clássicas e não filas de quórum ou streams?**
Com um nó só, as filas de quórum (que existem para replicação) não fazem sentido completo, e as clássicas são o caso de comparação mais direto. **[sugestão]** Está nos trabalhos futuros e na validade externa ("RabbitMQ 4.3.6 com filas clássicas").

**28. Por que compressão desligada, se o Kafka usa em produção?**
Porque as réplicas da frota têm conteúdo **quase idêntico**, e o Kafka, que comprime lotes, ganharia uma vantagem artificial que não existiria com dados reais variados. Está declarado em Validade de construto.

### 9.4 Sobre estatística e validade

**29. Por que 10 repetições?**
Porque, neste ambiente, **uma rodada isolada não sustenta conclusão** (a calibração mostrou episódios de disco da VM). Dez repetições + mediana + Tukey impedem que um episódio decida o resultado. **[sugestão]** É também o que cabe no tempo: 390 rodadas × ~100 s ≈ 11 h só no cenário padrão.

**30. Por que mediana e não média?**
A mediana é **robusta a valores extremos**; a média seria puxada por uma rodada com episódio de disco.

**31. Descartar 8% das rodadas com Tukey não é manipular os dados?**
Não: a regra foi **fixada antes de ver os dados**, é **objetiva** e foi aplicada **igualmente a todas as combinações**; a distribuição dos descartes é parecida entre os modos (2 a 8 por combinação). E as **inválidas não foram repetidas** para "melhorar" o resultado.

**32. Por que Kruskal-Wallis?**
Compara **três grupos** (os modos) **sem supor distribuição normal** — latência não é normal e as amostras são pequenas (~10).

**33. Por que 20% como limiar de relevância? Não é arbitrário?**
Todo limiar tem algo de convenção; o que importa é que foi **fixado antes**, porque, com dispersão pequena, diferenças **irrelevantes na prática** viram estatisticamente significativas. **[sugestão]** E o resultado não é sensível a ele: a maior diferença entre modos foi **11%** (e para pior); no RabbitMQ a 40 mil, os 6% de ganho tiveram **p = 0,06** — nem relevante nem significativo.

**34. p = 0,06 não é "quase significativo"?**
Não se interpreta assim: não passou de 5%, e **6% de diferença** está muito abaixo do limiar de relevância. Mesmo que fosse significativo, seria um ganho **sem importância prática** — e o texto o apresenta como "o único caso próximo de um ganho".

**35. As 12 rodadas inválidas do Kafka não enviesam o resultado do Kafka?**
Foram **4 por modo**, iguais nos três, então não afetam a comparação entre modos; e a carga continua sustentada porque a **maioria** das rodadas foi válida. O ponto de saturação do Kafka, por outro lado, **não foi localizado** — isso está declarado.

**36. Qual é o ponto de saturação do Kafka?**
**Não foi localizado**: o Kafka sustentou **400 mil**, a maior carga testada, nos três modos; sei que fica **acima** disso. **[sugestão]** Localizá-lo exigiria um gerador mais forte ou outra máquina, e disco não virtualizado.

### 9.5 Sobre os resultados

**37. Por que o RabbitMQ é mais rápido em carga baixa?**
Porque **não espera para formar lotes**: entrega cada mensagem assim que chega. O Kafka tem um **piso de ~3 ms** no P50, que vem do `linger.ms` = 5 ms — o ajuste para 1 ms derrubou o P50 para 1 ms, confirmando a causa.

**38. Por que o RabbitMQ satura em 60 mil?**
Pela **CPU do broker**: 2,8 a 3,5 dos 4 núcleos. Ele trata cada mensagem individualmente (roteia, confirma e, persistente, grava em disco). Perto do limite, acumula fila e a cauda cresce (P99 75 ms a 80 mil).

**39. Por que o throughput é igual em todas as combinações?**
Porque o gerador é de **malha aberta**: enquanto o sistema sustenta, processa **exatamente** a carga oferecida (diferença < 0,1%). O que distingue as combinações é **até onde** cada uma sustenta — o ponto de saturação.

**40. Então Channels e Pipelines são inúteis?**
Não. **Nesta carga** — processamento leve, que não espera nada — eles só custaram. Eles servem quando o processamento é **mais lento que a entrega do broker** ou **espera algo** (gravar, chamar um serviço). O estudo preliminar mostra isso: com o consumidor esperando a confirmação de uma republicação no Kafka, o Direct parou em ~64 mil e Channels/Pipelines sustentaram 200 mil.

**41. Por que, no Kafka, o custo do mecanismo foi tão maior que no RabbitMQ?**
No Kafka a base é muito baixa (Direct a 0,09–0,63 núcleo), então o custo fixo de **acordar uma thread de trabalho por evento** (2,4–3,7 µs) **multiplica** a CPU. No RabbitMQ, o consumidor já ocupa ~1 núcleo (o cliente custa mais por mensagem), e o mesmo acréscimo pesa só 4–9%.

**42. Por que a alocação de memória foi igual, se o Pipelines evita criar objetos?**
Porque **quem aloca é a biblioteca cliente** do broker, **antes** de o evento chegar ao mecanismo: ~950 bytes/evento no Kafka, ~20× o tamanho da mensagem. A economia do Pipelines acontece depois disso e fica invisível.

**43. Por que o Kafka usa tanta memória no broker?**
Usa de **1,4 a 1,7 GB** contra 0,2–0,3 GB do RabbitMQ: o Kafka troca CPU por memória — trabalha com lotes e com o cache de páginas. **[sugestão — o "cache de páginas" está na fundamentação]**

**44. O que o cenário ajustado mostrou?**
Que **parte da diferença entre os brokers é configuração** e que **cada parâmetro troca latência por CPU do broker**: RabbitMQ transitório com P99 43–76% menor e saturação em 80 mil; Kafka `linger.ms` 1 com P50 de 1 ms mas P99 maior em carga alta e CPU dobrada; `linger.ms` 20 o inverso. E os mecanismos **continuaram sem efeito**.

**45. Se tivesse que escolher uma arquitetura para um sistema real, qual seria?**
Depende da carga: **RabbitMQ + Direct** para carga baixa com latência mínima; **Kafka + Direct** para carga alta; ajustar `linger.ms` e persistência conforme a latência que se quer pagar em CPU. Channels ou Pipelines **só se o processamento esperar algo** ou for pesado — e medir antes.

### 9.6 Sobre limitações e o futuro

**46. Qual a maior limitação do trabalho?**
Ser **uma máquina com um nó por broker e processamento leve**. As conclusões valem para esse cenário; o estudo preliminar já mostra que com espera no processamento o resultado muda.

**47. O que você faria diferente?**
**[sugestão]** Incluir desde o início a **intensidade do processamento** como fator (leve, pesado, com espera), rodar em **hardware sem virtualização** para eliminar os episódios de disco e **localizar a saturação do Kafka**, e testar **vários nós**.

**48. O estudo preliminar não deveria estar no texto?**
Ele foi um estudo exploratório, com protocolo menos completo; por isso aparece só como indicação na conclusão e está disponível no repositório. Aprofundá-lo é o primeiro trabalho futuro.

**49. Seus resultados são reproduzíveis?**
Sim: código, scripts, configurações e resultados brutos estão no GitHub (`ramosnvy/pitwall`), com **tags** para cada cenário (`cenario-padrao-v1.2`, `cenario-ajustado-v1.1`), e os dados vêm de uma cópia local fixa da OpenF1.

### 9.7 Perguntas "armadilha" e como lidar

- **"Você provou que Channels é pior?"** → Não "pior" em geral: mostrei que, **para processamento leve e sem espera**, não melhora o desempenho e custa CPU. Fora disso, é dedução — e o estudo preliminar aponta o contrário quando há espera.
- **"Seu Kafka 'ganhou' por causa do disco do RabbitMQ."** → Em latência, em parte sim (por isso o cenário ajustado); em **capacidade**, não: RabbitMQ transitório saturou em 80 mil, Kafka > 400 mil.
- **"Por que não usou um benchmark padrão (OpenMessaging)?"** → **[sugestão]** Porque o objeto não é só o broker: é a **composição** broker + mecanismo interno em .NET, com grupo de controle; precisei de um consumidor próprio nos três modos e de conferência de correção (digest).
- **"Uma VM no Windows é um ambiente ruim para medir."** → Concordo que não é o ideal, e é um achado do trabalho: **o ambiente pesou mais que a arquitetura**. Por isso a calibração, os núcleos exclusivos, as repetições e os critérios de validade, e a limitação está declarada.
- **Se não souber:** "Isso eu não medi; o que eu posso afirmar é ... ; seria um bom trabalho futuro."

---

## 10. Cola: números para saber de cor

| O quê | Valor |
|---|---|
| Combinações | 6 = 2 brokers × 3 modos |
| Cargas RabbitMQ / Kafka | 10–80 mil / 10–400 mil eventos/s |
| Rodada | 10 s aquecimento + 90 s medição |
| Repetições | 10 por combinação e carga, ordem sorteada |
| Rodadas padrão | 390; 12 inválidas (todas Kafka ≥ 200 mil); 32 descartadas (~8%); 346 usadas; 0 divergências de digest |
| Rodadas ajustado | 240; 232 válidas |
| Evento | 46 bytes; 100 Hz; 100 mil/s = 1.000 carros |
| Faixas | 4 (CRC32); 1 leitor por faixa |
| Fila interna | Channel 10.000 eventos; Pipe 460.000 bytes |
| Núcleos | gerador 0–3, broker 4–7, consumidor 8–10, banco/métricas 11 |
| Limites | atraso de envio ≤ 50 ms; P99 < 50 ms; relevância 20% P99 / 10% throughput; Kruskal-Wallis 5% |
| P99 a 10 mil | RabbitMQ 2,3 ms; Kafka 5,8 ms |
| P50 a 10 mil | RabbitMQ 0,36 ms; Kafka 3,0 ms |
| Cruzamento | P99 a 20 mil; P50 a 60 mil |
| P99 RabbitMQ | 25 ms a 60 mil; 75 ms a 80 mil |
| P99 Kafka | 5,5–6,7 ms até 400 mil |
| Saturação | RabbitMQ 60 mil (ajustado 80 mil); Kafka > 400 mil (não localizada) |
| Maior diferença entre modos (P99) | 11% (Kafka Pipelines, 400 mil, para pior); ajustado 18% |
| Único "quase ganho" | RabbitMQ a 40 mil, P99 −6%, p = 0,06 |
| CPU consumidor Kafka | Direct 0,09–0,63; Ch/Pi 0,17–1,69 → 1,9–3,3× (Ch 1,9–3,0; Pi 2,0–3,3) |
| CPU extra por evento | 2,4–3,7 µs (≥ 100 mil) |
| CPU consumidor RabbitMQ | ~1 núcleo; mecanismos +4 a 9% |
| CPU broker | RabbitMQ 2,8–3,5 de 4; Kafka 0,4–0,5 |
| CPU gerador | 3,1 (RabbitMQ, 60 mil) vs 1,1 (Kafka) |
| Memória | consumidor 87–117 MB; broker Kafka 1,4–1,7 GB vs RabbitMQ 0,2–0,3 GB |
| Alocação | Kafka 952–975 B/evento; RabbitMQ 720–834 B/evento (~20× a mensagem) |
| Custo do processamento | ~0,12 µs por evento |
| Ajustado RabbitMQ | P99 −43 a −76% (2,3 → 0,65 ms a 10 mil; 25 → 10 ms a 60 mil) |
| Ajustado Kafka linger 1 | P50 3 → 1 ms; P99 +28 a +296% a partir de 40 mil; CPU broker 2× |
| Ajustado Kafka linger 20 | P99 ≈ 21 ms; CPU broker −30% |
| Calibração | núcleos exclusivos −40 a −64% P99 RabbitMQ; 43% bimodal; 126×; 3 defeitos + 3 assimetrias; disco RabbitMQ +48% P99 a 40 mil; linger 0 → P50 400 ms |
| Estudo preliminar | Kafka: Direct ~64 mil vs Ch/Pi 200 mil; RabbitMQ: equivalentes |
| Versões | .NET 10; Kafka 4.1.0; RabbitMQ 4.3.6; Confluent.Kafka 2.15.1; RabbitMQ.Client 7.2.2; PostgreSQL 17; HdrHistogram 2.5.0 |

---

## 11. A apresentação, slide a slide

| # | Slide | Tempo | O que precisa ficar claro |
|---|---|---|---|
| 1 | Capa | 0:30 | Título e o tema em uma frase |
| 2 | Contexto e problema | 1:15 | As duas camadas e as duas perguntas |
| 3 | Objetivos | 0:45 | Objetivo geral + 4 específicos; Direct como controle |
| 4 | Conceitos principais | 1:15 | Kafka lotes × RabbitMQ individual; Channels objetos × Pipelines bytes; P99 e malha aberta |
| 5 | Trabalhos relacionados | 0:50 | A lacuna: nenhum com grupo de controle |
| 6 | Bancada experimental | 1:30 | As 5 etapas; um leitor por faixa (ordem); digest |
| 7 | Ambiente e medição | 0:45 | Núcleos exclusivos; como a latência é medida; banco fora da medida |
| 8 | Metodologia | 1:15 | Dois cenários; **assimetria declarada**; regras prévias |
| 9 | Como garanti uma comparação justa | 1:30 | Os 4 blocos e a base de cada decisão |
| 10 | Calibração antes da coleta | 1:00 | O ambiente pesou mais que a arquitetura → decisões |
| 11 | Validade das rodadas | 0:40 | 390 / 0 / 12 / 8%; 3,7 Hz |
| 12 | Latência e throughput | 1:45 | Brokers se cruzam; modos sobrepostos; saturação |
| 13 | Uso de recursos | 1:15 | CPU do consumidor é o único lugar onde os modos se separam — para pior |
| 14 | Cenário ajustado | 1:15 | Configuração muda os brokers, não os modos |
| 15 | Onde cada combinação ganha | 1:00 | "O broker decide o desempenho; o mecanismo, o custo" |
| 16 | Conclusão | 1:45 | Veredito; literatura; estudo preliminar; limitações → futuro |
| 17 | Referências | 0:10 | — |
| 18 | Obrigado | — | — |
| A1 | Apoio: decisões | (perguntas) | Use para "por que você fez X?" |
| A2 | Apoio: configuração | (perguntas) | Use para "e a configuração dos brokers?" |

**Total ≈ 18 min 30 s**, deixando folga para os 20.

**Dicas finais:**
- Abra os resultados dizendo **"antes dos resultados, os dados são confiáveis"** (slide 11) — isso responde metade das perguntas de validade antes que elas apareçam.
- Repita a frase-síntese duas vezes: no slide 15 e na conclusão.
- Quando falar do cenário padrão, **antecipe a assimetria** ("favorece o Kafka, e por isso existe o ajustado") — mostra domínio e tira a pergunta da banca.
- Separe sempre **medido** de **deduzido** ao falar de onde Channels e Pipelines se encaixam.
