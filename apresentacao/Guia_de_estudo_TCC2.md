# Guia de estudo e preparação para a banca — TCC2

**Avaliação Experimental de Arquiteturas Compostas de Processamento de Eventos em Alta Frequência Utilizando .NET**
Pedro Augusto Ramos de Sousa · UTFPR Dois Vizinhos · Engenharia de Software

> Como usar: leia a Parte 1 para ter a história inteira na cabeça; a Parte 2 para os conceitos; as Partes 3 a 8 acompanham o texto do TCC seção por seção, sempre com **o que foi feito, por que, e em que se baseia**; a Parte 9 é o banco de perguntas da banca; a Parte 10 é a cola de números.
>
> Tudo o que está aqui vem do texto (`tcc2.tex`) ou do código do repositório. Onde eu sugiro uma resposta que **não está escrita no texto**, marquei com **[sugestão]** — use, mas saiba que é argumento seu, não dado medido.

---

## Sumário

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
