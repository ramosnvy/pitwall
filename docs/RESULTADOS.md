# Resultados

Retrato de 28/10/2026, depois de todas as medições. São três matrizes, todas com as regras de [ANALISE.md](ANALISE.md), registradas antes da coleta:

| Matriz | Data | Tag | Rodadas | Válidas | Descartadas (Tukey) |
| --- | --- | --- | --- | --- | --- |
| Cenário padrão (v3), 100 Hz e 3,7 Hz | 22/10 | `cenario-padrao-v1.2` | 430 | 417 | 42 |
| Cenário ajustado, reduzida | 27/10 | `cenario-ajustado-v1.1` | 240 | 232 | 14 |
| Segundo cenário (republicação) | 27 e 28/10 | `cenario-republicacao-v1.1` | 300 | 295 | 18 |

Nenhuma falha técnica nas três. Todas as rodadas inválidas foram por atraso de envio do gerador acima de 50 ms, no Kafka, a 200 mil ev/s ou mais (no segundo cenário, também no Kafka). Nenhuma rodada divergiu no digest, teve registro incompleto no `Pipe` ou descartou janelas: em cada carga, todas as combinações produziram o mesmo resultado, e nenhum evento se perdeu, duplicou ou saiu de ordem por carro.

Ambiente: i5-12400F, Docker Desktop sobre WSL2, núcleos exclusivos (gerador 0–3, broker 4–7, consumidor 8–10, banco, métricas e painel 11). CPU em **núcleos**: 1,0 é um núcleo inteiro. Valores são medianas das rodadas válidas e não descartadas de cada combinação. O texto com a discussão completa é a seção 6 de `texto/tcc2.tex`; o apêndice traz todas as métricas do cenário padrão a 100 Hz.

A versão anterior deste documento, da matriz v2 (setembro, commit `7e283c2`), está no histórico do git. Aquela matriz passou a ser calibração e não entra na análise.

## 1. Cenário padrão

Valores de fábrica dos brokers e das bibliotecas, exceto onde impedem a equivalência (tabela final em [AUDITORIA-CONFIG.md](AUDITORIA-CONFIG.md)). 10 s de aquecimento, 90 s de medição, 10 repetições por combinação.

### Saturação

| Broker | Cargas (mil ev/s) | Ponto de saturação |
| --- | --- | --- |
| RabbitMQ | 10, 20, 40, 60, 80 | **60 mil** nos três modos |
| Kafka | 10, 20, 40, 60, 100, 200, 300, 400 | **≥ 400 mil** nos três modos, acima do alcance da bancada |

O throughput acompanhou a carga em todas as combinações, com diferença abaixo de 0,1%; quem decide a saturação é a latência. A 80 mil, o P99 do RabbitMQ passa de 75 ms nos três modos. A 400 mil a margem do Kafka é pequena: só 6 rodadas do Direct e 5 do Pipelines, em 10, cumpriram sozinhas as três condições.

O RabbitMQ esgota a CPU do broker antes: 3,3 dos 4 núcleos a 60 mil, contra 0,4 a 0,5 núcleo do Kafka em qualquer carga até 400 mil. O mesmo vale para o gerador: 3,1 núcleos para publicar no RabbitMQ a 60 mil, contra 1,1 no Kafka. Em troca, o broker Kafka usa mais memória: 1,3 a 1,5 GB em média (pico de 1,7), contra 0,16 a 0,20 GB do RabbitMQ (pico de 0,3).

### Latência (Direct; os outros modos ficam a menos de 12% no P99)

| Carga (mil ev/s) | RabbitMQ P50 | RabbitMQ P99 | Kafka P50 | Kafka P99 |
| --- | --- | --- | --- | --- |
| 10 | **0,36** | **2,35** | 3,00 | 5,80 |
| 20 | **0,57** | **5,36** | 2,98 | 5,63 |
| 40 | **1,03** | 8,94 | 3,01 | **5,63** |
| 60 | **1,98** | 24,73 | 3,00 | **5,57** |
| 80 | 3,14 | 75,39 | — | — |
| 100 | — | — | 3,00 | 5,51 |
| 200 | — | — | 3,10 | 5,49 |
| 300 | — | — | 3,22 | 5,65 |
| 400 | — | — | 3,41 | 6,05 |

Em ms. Em carga baixa o RabbitMQ é mais rápido. A 20 mil o P99 dos dois se iguala, e acima disso o do RabbitMQ cresce com a carga, enquanto o do Kafka fica entre 5,5 e 6,7 ms até 400 mil. O piso do Kafka (P50 ~3 ms, P99 ~5,5 ms) é a espera de formação de lotes do produtor, `linger.ms` = 5, testada no cenário ajustado (§2). A latência máxima do Kafka fica entre 16 e 26 ms até 300 mil e sobe para 93 ms a 400 mil, compatível com os episódios de gravação de disco da VM ([IMPLEMENTACAO.md](IMPLEMENTACAO.md)).

**Frequência por carro:** nas quatro comparações 100 Hz × 3,7 Hz (Direct, duas cargas por broker), o P99 mudou menos de 10%. A interpolação não muda as conclusões.

### Mecanismo interno

**Latência e throughput: sem diferença relevante em nenhuma combinação.** O maior efeito no P99 foi de 11% (Kafka, Pipelines, 400 mil), abaixo do limite de 20%; no RabbitMQ, 9%. O Kruskal-Wallis foi significativo em 6 das 8 cargas do Kafka, com efeito de 0,5 a 6%: com dispersão de centésimos de ms, até diferenças irrelevantes ficam detectáveis.

**CPU do consumidor (núcleos):**

| Carga | Kafka Direct | Kafka Channels | Kafka Pipelines | RabbitMQ Direct | RabbitMQ Channels | RabbitMQ Pipelines |
| --- | --- | --- | --- | --- | --- | --- |
| 10 mil | 0,09 | 0,17 | 0,18 | 1,01 | 1,08 | 1,08 |
| 60 mil | 0,17 | 0,39 | 0,41 | 0,99 | 1,04 | 1,06 |
| 200 mil | 0,36 | 1,01 | 1,10 | — | — | — |
| 400 mil | 0,63 | 1,57 | 1,69 | — | — | — |

No Kafka, Channels e Pipelines custam de 2,4 a 3,3 vezes a CPU do Direct. A partir de 100 mil, o acréscimo é de 2,4 a 3,7 µs por evento, a ordem de acordar uma thread de trabalho a cada evento. No RabbitMQ o acréscimo é de 4 a 9%, porque o consumidor já ocupa perto de um núcleo em qualquer carga (causa não investigada, provavelmente o despacho da biblioteca cliente).

Com processamento de ~0,12 µs por evento e chegada regular, não há espera a esconder, e o desacoplamento só acrescenta a fila interna e a troca de thread. **Channels e Pipelines são indistinguíveis entre si** em todas as medidas.

### Custo do .NET

As alocações não dependem do modo, com diferença abaixo de 0,1%: de 952 a 975 bytes por evento no Kafka e de 720 a 834 no RabbitMQ, cerca de 20 vezes a mensagem. Quem aloca é a biblioteca cliente. Como o evento é `struct`, nem o Channels nem o Pipelines acrescentam alocação. GC geração 0: ~96 por milhão de eventos no Kafka e 72 a 83 no RabbitMQ; geração 2, no máximo uma por rodada. A memória de pico do consumidor ficou entre 87 e 117 MB.

## 2. Cenário ajustado

Matriz reduzida: Direct em todas as cargas; Channels e Pipelines só em RabbitMQ 20 e 60 mil e Kafka 20 e 200 mil; perfil de vazão só com Direct, a 200, 300 e 400 mil; sem bateria a 3,7 Hz ([ANALISE.md](ANALISE.md) §11).

| Perfil | Mudança |
| --- | --- |
| Kafka ajustado | `linger.ms` 5 → 1 (0 degenera: P50 de ~400 ms já a 10 mil) |
| Kafka ajustado para vazão | `linger.ms` 5 → 20 |
| RabbitMQ ajustado | mensagem transitória; `prefetch` 300 |

**Direct, padrão × ajustado** (latência em ms, CPU do broker em núcleos):

| Broker | Carga (mil) | P50 padrão | P50 ajust. | P99 padrão | P99 ajust. | CPU broker padrão | CPU broker ajust. |
| --- | --- | --- | --- | --- | --- | --- | --- |
| RabbitMQ | 10 | 0,36 | 0,24 | 2,35 | 0,65 | 2,80 | 1,91 |
| | 20 | 0,57 | 0,21 | 5,36 | 1,30 | 3,06 | 2,75 |
| | 40 | 1,03 | 0,29 | 8,94 | 3,70 | 3,22 | 3,41 |
| | 60 | 1,98 | 0,41 | 24,73 | 10,40 | 3,27 | 3,67 |
| | 80 | 3,14 | 0,61 | 75,39 | 42,78 | 3,52 | 3,68 |
| Kafka | 10 | 3,00 | 1,04 | 5,80 | 3,36 | 0,41 | 0,81 |
| | 20 | 2,98 | 0,96 | 5,63 | 5,22 | 0,42 | 0,91 |
| | 40 | 3,01 | 0,94 | 5,63 | 7,61 | 0,42 | 1,00 |
| | 60 | 3,00 | 0,94 | 5,57 | 8,29 | 0,43 | 1,02 |
| | 100 | 3,00 | 0,93 | 5,51 | 7,03 | 0,44 | 1,08 |
| | 200 | 3,10 | 0,91 | 5,49 | 9,64 | 0,47 | 1,12 |
| | 300 | 3,22 | 0,94 | 5,65 | 22,37 | 0,46 | 1,14 |
| | 400 | 3,41 | 0,99 | 6,05 | 15,00 | 0,49 | 1,19 |

| Hipótese | Resultado |
| --- | --- |
| H7a: P50 do Kafka ajustado cai em todas as cargas | **Confirmada.** Cai de ~3 para ~1 ms (65 a 71%): o piso do padrão é o `linger.ms`. O P99 só cai a 10 mil; a partir de 40 mil sobe de 28 a 296%, até 22 ms a 300 mil |
| H7b: CPU do broker Kafka sobe e a saturação pode cair | **Em parte.** A CPU do broker dobra (0,4–0,5 → 0,8–1,2) e a do gerador sobe de 11 a 19%, mas a saturação continua ≥ 400 mil |
| H7c: P99 do RabbitMQ cai a partir de 40 mil e a saturação sobe para 80 mil | **Confirmada, e mais forte.** O P99 cai de 43 a 76% em todas as cargas, não só a partir de 40 mil; a saturação sobe para 80 mil (43 ms) |
| H7d: Channels e Pipelines continuam sem efeito de latência | **Confirmada.** Maior efeito no P99 de 18% (para menos), Kruskal-Wallis com p > 0,7 em todas as cargas; no Kafka, 1,9 a 2,3 vezes a CPU do consumidor |
| H7e: Kafka para vazão tem P99 maior, menos CPU e menos rodadas inválidas | **Confirmada.** P99 ~21 ms (247 a 275% acima), CPU do broker ~30% menor (0,32–0,35), gerador 2 a 3% menor, 9 de 10 rodadas válidas a 400 mil contra 6 |

Entre os brokers, o ajuste aproxima os dois nas cargas médias: o RabbitMQ ajustado tem o menor P99 até 40 mil (3,7 contra 7,6 ms), e a 60 mil os dois ficam próximos (10,4 e 8,3 ms). Só o Kafka sustenta de 100 a 400 mil. Boa parte da diferença do cenário padrão é configuração: o piso do Kafka é o `linger.ms`, e parte da latência do RabbitMQ é a gravação em disco. Cada parâmetro troca latência por CPU do broker.

## 3. Segundo cenário: republicação

O consumidor republica o resultado no mesmo broker, e um painel lê a saída e mede a latência de ponta a ponta. A entrada só é confirmada depois de todas as saídas até ela. O Direct para a cada 100 saídas para esperar as confirmações. Channels e Pipelines usam três estágios e até 10 mil saídas sem confirmação, com liberação pelo prefixo contínuo confirmado. Saída **enriquecida**: 64 bytes por evento, o dobro do volume no broker. Saída **resumida**: 38 bytes por janela, ~1% da entrada. Cenário padrão de configuração, 60 s de medição, 10 repetições.

Nenhuma saída foi recusada, e o digest do painel foi o mesmo em todas as rodadas de cada carga e saída; na resumida, também igual ao do consumidor. O painel ocupou no máximo 0,42 núcleo, abaixo do limite de meio núcleo.

**Throughput (mil ev/s) e P99 de ponta a ponta (ms); em negrito, cargas sustentadas:**

| Broker | Saída | Carga | Direct vazão | Direct P99 | Channels vazão | Channels P99 | Pipelines vazão | Pipelines P99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RabbitMQ | enriquecida | 10 | **10,0** | **8,4** | **10,0** | **8,4** | **10,0** | **8,2** |
| | | 20 | **20,0** | **15,5** | **20,0** | **13,3** | **20,0** | **14,3** |
| | | 30 | **30,0** | **37,3** | **30,0** | **32,4** | **30,0** | **36,6** |
| | | 40 | 40,0 | 236 | 40,0 | 128 | 40,0 | 119 |
| | resumida | 40 | **40,0** | **19,7** | **40,0** | **19,1** | **40,0** | **19,5** |
| Kafka | enriquecida | 20 | **20,0** | **15,5** | **20,0** | **11,7** | **20,0** | **11,7** |
| | | 50 | **50,0** | **16,8** | **50,0** | **12,1** | **50,0** | **12,0** |
| | | 100 | 64,0 | 38.027 | **100,0** | **12,2** | **100,0** | **12,2** |
| | | 200 | 63,7 | 144.966 | **200,0** | **13,0** | **200,0** | **13,4** |
| | resumida | 200 | **200,1** | **33,5** | **200,0** | **15,5** | **200,0** | **14,8** |

- **Kafka, enriquecida: o mecanismo decide o resultado.** O Direct tem teto de ~64 mil ev/s, e a 100 e 200 mil a fila cresce até P99 de 38 e 145 s. Channels e Pipelines sustentam 200 mil com P99 de 13 ms. O teto é o custo da espera, porque cada espera custa perto de um lote do `linger.ms`: na saturação, 5,8 ms por espera dão ~17 mil ev/s por faixa, ~69 mil nas quatro. Mesmo abaixo do teto, Channels e Pipelines têm P99 24 a 28% menor (Kruskal-Wallis p < 0,001).
- **RabbitMQ, enriquecida: efeito pequeno.** Os três modos sustentam até 30 mil e falham a 40 mil por latência. Até 30 mil a diferença no P99 é de no máximo 14%. A confirmação chega rápido (~1,3 ms por espera a 10 mil), e o limite é a CPU do broker, que agora carrega o dobro de mensagens (3,2 a 3,6 núcleos): a saturação cai de 60 para 30 mil.
- **Resumida.** No RabbitMQ a 40 mil, os três modos empatam (diferença abaixo de 4%). No Kafka a 200 mil, o Direct tem o dobro do P99: ainda para ~1.400 vezes por rodada, e cada parada custa um lote.
- **Custo.** No Kafka, Channels e Pipelines custam de 1,1 a 1,3 vezes a CPU do consumidor do Direct nas cargas que ele sustenta (a 20 mil, 0,29–0,30 contra 0,23 núcleo), bem menos que no primeiro cenário, porque o Direct também paga a publicação. A 200 mil, 0,96 e 1,12 núcleo. No RabbitMQ, 1,0 a 1,3 núcleo nos três modos.

| Hipótese (no texto, H1 e H2) | Resultado |
| --- | --- |
| H5a: na enriquecida, o Direct sustenta menos carga ou tem P99 maior | **Confirmada no Kafka** (um quarto da carga dos outros modos); **no RabbitMQ, só acima da saturação**, onde o gargalo é o broker |
| H5b: na resumida, os três modos empatam | **Confirmada no RabbitMQ, refutada no Kafka** (33,5 contra 15 ms): a espera do Direct custa um lote do Kafka |

## 4. Síntese

- **RabbitMQ:** menor latência em carga baixa, cauda que cresce com a carga, saturação em 60 mil ev/s (80 mil ajustado), broker no limite de CPU. Pouca memória.
- **Kafka:** piso de latência de ~3 ms no P50, que é configuração (`linger.ms`), cauda estável até 400 mil, pelo menos 5 vezes a carga do RabbitMQ com cerca de um sétimo da CPU de broker na mesma carga. Mais memória no broker.
- **Mecanismo interno:** sem espera a esconder, Channels e Pipelines não mudam latência, throughput nem alocação, e custam de 2,4 a 3,3 vezes a CPU do consumidor no Kafka. Com a espera da republicação, sustentam quatro vezes a carga do Direct no Kafka, com 1,1 a 1,3 vez a CPU dele. Compensam quando o processamento espera algo e a espera é longa em relação ao custo do evento.
- **Configuração:** um parâmetro por broker muda o resultado mais que o mecanismo no primeiro cenário. Cada parâmetro troca latência por CPU do broker, e o melhor valor depende da faixa de carga.

## 5. Limitações

Detalhadas em [AMEACAS-VALIDADE.md](AMEACAS-VALIDADE.md) e na seção de ameaças do texto. As que afetam a leitura destes números:

- **Máquina única:** não há rede entre os componentes, um nó por broker, sem replicação.
- **Durabilidade assimétrica no cenário padrão:** o RabbitMQ grava em disco antes de confirmar e o Kafka não. O cenário ajustado iguala as duas.
- **Episódios de gravação de disco da VM:** invalidam rodadas do Kafka a partir de 200 mil.
- **Matriz ajustada reduzida:** Channels e Pipelines medidos em duas cargas por broker; a conclusão sobre eles nesse cenário vale para essas cargas.
- **Painel do segundo cenário:** divide o núcleo 11 com o banco e as métricas; a regra de meio núcleo tirou da matriz as cargas em que ele seria o gargalo (60 mil no RabbitMQ, 300 mil no Kafka).
- **Janela de confirmação do Direct:** fixa em 100 saídas e não variada. O teto do Direct no Kafka depende dela; não depende dela que o Direct para de consumir a cada espera.
- **Workload:** uma única corrida, mensagens de 46 bytes; Channels e Pipelines usados um evento por vez.

## 6. Onde estão os números

| O quê | Arquivo |
| --- | --- |
| Rodadas brutas | `results/matriz-v3-*`, `results/matriz-ajustado-*`, `results/republicacao-matriz-*` (`*-runs`, `*-consumer`, `*-producer`, `*-painel`) |
| Análise pré-registrada (rodadas, resumo, saturação, modos) | `results/analise-v3-*`, `results/analise-fase7-*`, `results/analise-republicacao-*` |
| Comparação padrão × ajustado | `results/fase7-latencia-*`, `results/fase7-vazao-*` (`analysis/comparar-perfis.ps1`) |
| Script de análise | `analysis/analise.ps1` (`-Republicacao` no segundo cenário) |
| Gráficos e tabelas do texto | `texto/figuras/graficos.py`, `texto/figuras/tabelas.py` → `texto/fig_*.pdf`, `texto/tab_metricas.tex` |
