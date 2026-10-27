# Análise pré-registrada

25/09/2026. Como os resultados da matriz v3 serão calculados, escrito antes de ver os dados dela (DESENVOLVIMENTO, fase 5). Mudar uma regra depois de ver os dados exige registrar aqui a mudança, a data e o motivo.

**Implementação:** `analysis/analise.ps1`, que escreve quatro CSVs (rodadas, resumo, saturação e comparação entre modos). As contas do teste foram conferidas contra valores de tabela: p = 0,05 no qui-quadrado com 1, 2 e 4 graus de liberdade, e H = 12,5 no Kruskal-Wallis de três grupos separados. Os quartis são iguais aos do R.

## 1. Unidades

- **Rodada:** uma execução de 10 s de aquecimento e 90 s de medição, com uma linha no CSV consolidado.
- **Combinação:** broker × modo × carga, dentro de um mesmo perfil de configuração e de uma mesma frequência por carro (100 Hz no cenário principal, 3,7 Hz na sensibilidade).
- **Repetições:** 10 por combinação, em ordem sorteada dentro de cada broker.

## 2. Validade da rodada

Aplicada nesta ordem. A primeira regra que falhar dá o motivo registrado.

| # | Regra | Motivo registrado |
| --- | --- | --- |
| 1 | Existe o relatório do produtor e o do consumidor, casados pelo `run_id` | `sem_relatorio` |
| 2 | Atraso máximo de envio de até 50 ms | `atraso_envio` |
| 3 | Resumo do resultado igual ao de referência da carga: o mais frequente entre as rodadas da mesma carga e frequência, em todos os brokers e modos | `conferencia` |
| 4 | Nenhum registro incompleto no Pipe (`incomplete_records = 0`) | `registro_incompleto` |
| 5 | Nenhuma janela descartada na fila de persistência (`windows_dropped = 0`) | `persistencia` |

**Repetição de rodadas.** Falha técnica (regra 1: container que não subiu, relatório ausente) é repetida no fim da mesma noite, e a repetição é registrada. Rodada inválida pelas regras 2 a 5 **não** é repetida: é resultado, e conta para o ponto de saturação.

## 3. Descarte de discrepantes

- **Sobre o quê:** o P99 das rodadas válidas de cada combinação.
- **Regra de Tukey:** sai a rodada fora de [Q1 − 1,5·IQR; Q3 + 1,5·IQR], com os quartis por interpolação linear (o método padrão do R e do Excel).
- **Uma vez só:** a regra não é reaplicada sobre o que sobrou.
- **Com menos de 5 rodadas válidas** na combinação, nada é descartado; a combinação é marcada como de poucas rodadas.
- **Publicação:** o número de descartes por combinação sai numa tabela, ao lado dos resultados.

## 4. Resumo por combinação

Sobre as rodadas válidas e não descartadas:

| Medida | Estatística |
| --- | --- |
| Vazão (ev/s) | mediana e desvio padrão |
| Latência P50, P95, P99 e máxima (ms) | mediana e desvio padrão |
| CPU do consumidor, do broker e do produtor (%) | mediana |
| Memória de pico do consumidor e do broker (MB) | mediana |
| Bytes alocados por evento no consumidor | mediana de `allocated_mb` × 10⁶ ÷ eventos medidos |
| Coletas de lixo por milhão de eventos (gerações 0, 1 e 2) | mediana |
| Rodadas: total, válidas, inválidas por motivo, descartadas por Tukey | contagem |

## 5. Ponto de saturação

**Uma carga é sustentada** pela combinação quando valem as três condições:
- a maioria das rodadas é válida pelas regras 2 a 5;
- a mediana da vazão é de pelo menos 99% da carga oferecida;
- a mediana do P99 fica abaixo de 50 ms.

**O ponto de saturação** é a maior carga testada que é sustentada e tem todas as cargas menores também sustentadas. Uma carga que falha abaixo de outra que passa encerra a contagem ali; o caso é apontado no texto como irregular.

Para cada combinação e carga também sai a fração de rodadas que cumprem as condições sozinhas, para mostrar quão perto do limite a combinação estava.

## 6. Comparações

- **Efeito do mecanismo interno:** diferença entre a mediana de Channels ou Pipelines e a mediana do Direct, no mesmo broker e carga. É o efeito principal do trabalho.
- **Efeito do broker:** diferença entre Kafka e RabbitMQ no mesmo modo, nas cargas comuns aos dois (10, 20, 40 e 60 mil ev/s; 80 mil só existe no RabbitMQ), e pelos pontos de saturação.
- **Tamanho de efeito relevante, fixado antes:** 10% na vazão ou 20% no P99. Uma diferença menor é reportada como "sem diferença relevante", mesmo que o teste a aponte.
- **Teste de apoio:** Kruskal-Wallis entre os três modos de cada broker e carga, com nível de 5%. Sozinho, não sustenta conclusão; só acompanha uma diferença acima do tamanho relevante. Sem ANOVA, conforme o orientador.

## 7. Figuras previstas

1. P99 por carga, uma linha por modo, um painel por broker, com a faixa entre o menor e o maior valor das rodadas.
2. Ponto de saturação por combinação.
3. CPU do consumidor por mil eventos por segundo, por modo.
4. Bytes alocados e coletas de lixo por evento, por modo.
5. Sensibilidade: 3,7 Hz contra 100 Hz, nas duas cargas medidas.

## 8. O que não será feito

- Nenhuma rodada sai da análise por outro critério que não os das seções 2 e 3.
- Nenhuma carga ou combinação entra ou sai depois de ver os dados.
- Médias não substituem medianas nos resumos; a média da latência aparece só na tabela completa.

## 9. Mudanças registradas

| Data | Mudança | Motivo |
| --- | --- | --- |
| 28/10/2026 | Matriz ajustada reduzida (tag `cenario-ajustado-v1.1`): Direct em todas as cargas; Channels e Pipelines só em duas por broker (RabbitMQ 20 e 60 mil, Kafka 20 e 200 mil); sem a bateria a 3,7 Hz; parte ajustado-vazao só com Direct. 240 rodadas em vez de 520, com as mesmas 10 repetições | Caber a medição restante numa sessão de cerca de 16 h. A pergunta da matriz ajustada é o efeito da configuração em cada broker; o dos modos já foi medido no padrão (nenhum ganho) e aqui só é conferido (H7d). A frequência mudou menos de 10% no padrão. Decidido antes de qualquer rodada da matriz ajustada |
| 22/10/2026 | Perfil ajustado do Kafka com `linger.ms` 1, e não 0; H7a reescrita | O ensaio curto e o diagnóstico `experiments/diag-linger.ps1` (IMPLEMENTACAO §10) mostraram que `linger.ms` 0 degenera: P50 de cerca de 400 ms a 10 mil ev/s, isolado e repetido. Com 1, o P50 cai de 3 para 1 ms, e o P99 fica entre 3,6 e 12,5 ms. Decidido antes de qualquer rodada da matriz ajustada |
| 13/10/2026 | Uma carga acima da saturação em cada broker: 80 mil no RabbitMQ e 400 mil no Kafka, além das fixadas em 24/09 | No ensaio curto, o RabbitMQ ficou abaixo de 50 ms a 60 mil; na varredura a 100 Hz, o Kafka com Pipelines aguentou 300 mil. Sem a carga a mais, a saturação ficaria só como "acima do maior ponto". Decidido antes de qualquer rodada da matriz v3 |

## 10. Cenário ajustado (registrado em 22/10/2026, antes de medir)

Fase 7. Os perfis estão em AUDITORIA-CONFIG ("Cenário ajustado") e em `Get-ProfileArgs`.

**Mesmas regras.** As seções 2 a 5 valem para cada perfil separadamente: validade, Tukey dentro da combinação, resumo e ponto de saturação.

**Comparação com o padrão.**
- **Efeito do ajuste:** diferença entre a mediana do ajustado e a do padrão, no mesmo broker, modo, carga e frequência.
- **Tamanho relevante:** o mesmo da seção 6, 10% na vazão ou 20% no P99.
- **Saturação:** os pontos de saturação dos dois perfis lado a lado, por broker e modo.
- **No Kafka, nas cargas de 200, 300 e 400 mil,** três perfis lado a lado: padrão (`linger.ms` 5), ajustado (0) e ajustado-vazao (20).

**Hipóteses**, a confirmar ou refutar no texto:

| # | Hipótese |
| --- | --- |
| H7a | Kafka ajustado: o P50 cai de forma relevante em todas as cargas até a saturação. O P99 pode não cair: no diagnóstico, com `linger.ms` 1, ficou entre 3,6 e 12,5 ms (alterada em 22/10, ver a tabela de mudanças) |
| H7b | Kafka ajustado: a CPU do broker sobe, e a saturação pode cair para menos de 400 mil |
| H7c | RabbitMQ ajustado: o P99 cai de forma relevante a partir de 40 mil, e a saturação sobe para 80 mil |
| H7d | Channels e Pipelines continuam sem efeito relevante de latência contra o Direct, nos dois perfis |
| H7e | Kafka ajustado-vazao: o P99 sobe em relação ao padrão, a CPU do broker e do gerador cai, e há menos rodadas inválidas a 400 mil |

## 11. Segundo cenário, passo A (registrado em 28/10/2026, antes de medir)

Republicação no mesmo broker, com at-least-once de ponta a ponta e o painel como consumidor final (APROFUNDAMENTO §8.6; DESENVOLVIMENTO, fase 9). Perfil padrão, 100 Hz, trecho a partir de 600 s, 10 s de aquecimento.

**Unidade.** Combinação = broker × modo × saída (enriquecida ou resumida) × carga. **Medição de 60 s** por rodada (90 s no primeiro cenário): a 20 mil ev/s, 60 s já são 1,2 milhão de eventos.

**Validade.** As regras da seção 2 valem para o consumidor, mais três:
- existe a linha do painel, casada pelo `run_id`;
- nenhuma saída recusada pelo broker (`out_failed = 0`);
- o digest do painel é o de referência da carga e da saída (o mais frequente entre todas as combinações). Na saída resumida, ele também precisa ser igual ao digest do consumidor.

**Latência principal:** o P99 de ponta a ponta, medido no painel. O P99 até o fim do processamento, no consumidor, fica como medida secundária.

**Ponto de saturação:** o da seção 5, com o P99 de ponta a ponta no lugar do P99 do consumidor.

**Comparações e tamanho relevante:** os da seção 6, contra o Direct, no mesmo broker, saída e carga. Medida extra: o tempo parado esperando confirmação (`confirm_wait_ms`) e o número de esperas.

**Varredura (calibração, fora da análise):** saída enriquecida com RabbitMQ em 10, 20, 40 e 60 mil e Kafka em 20, 50, 100, 200 e 300 mil, os três modos, 2 repetições, 30 s; saída resumida em 2 cargas por broker (RabbitMQ 20 e 60 mil, Kafka 50 e 200 mil), 1 repetição.

**Escolha das cargas da matriz, a partir da varredura:**
- **Saída enriquecida:** 4 cargas por broker: a mais baixa da varredura, duas no trecho em que algum modo ainda sustenta a carga e a primeira em que algum modo deixa de sustentá-la. Se nenhum modo saturar no Kafka, a maior carga da varredura.
- **Saída resumida:** 1 carga por broker, a mesma em que a enriquecida mostrou a maior diferença de P99 de ponta a ponta entre o Direct e os outros modos.
- 10 repetições por combinação, em ordem sorteada dentro de cada broker.

**Código (tag `cenario-republicacao-v1.1`, 28/10/2026).** Antes da varredura, o painel passou a conferir o resultado com uma trava por faixa, somada no fim, em vez de uma trava única disputada pelas 4 faixas a cada mensagem. Muda só o lado que mede, e não o que é medido. A única rodada feita com a v1 foi descartada.

**Limite do painel.** O painel divide o núcleo 11 com o banco e as métricas. Uma carga em que ele passa de 50% desse núcleo (`e2e_cpu_avg` acima de 50) não entra na matriz: ali a latência de ponta a ponta mediria o limite do painel, e não o dos brokers ou dos modos.

**Cargas escolhidas pela varredura (28/10/2026, antes da matriz).** Varredura em `results/republicacao-varredura-runs.csv`: 66 rodadas, 0 falhas, digest igual em todas as cargas.

| Broker | Enriquecida | Resumida | Motivo |
| --- | --- | --- | --- |
| Kafka | 20, 50, 100 e 200 mil | 200 mil | 20 mil é a mais baixa; 50 e 200 mil, trecho em que algum modo sustenta; 100 mil, a primeira em que um modo (o Direct, teto de cerca de 64 mil) deixa de sustentar. 300 mil fica de fora: painel acima de 50% do núcleo. Resumida na carga de maior diferença entre o Direct e os outros, dentro do limite do painel |
| RabbitMQ | 10, 20, 30 e 40 mil | 40 mil | Só 10 e 20 mil ficaram no trecho sustentado. Para completar as quatro cargas, 30 mil entra entre a última sustentada e a primeira que falha (40 mil), para localizar o joelho; é um desvio da regra, decidido antes da matriz. 60 mil fica de fora: painel acima de 50% do núcleo e todos os modos saturados. Resumida em 40 mil, a carga de maior diferença dentro do limite do painel |

Matriz: 10 repetições, 60 s de medição, 300 rodadas.

**Hipóteses** (APROFUNDAMENTO §8.7):

| # | Hipótese |
| --- | --- |
| H5a | Na saída enriquecida, o Direct sustenta menos carga que Channels e Pipelines, ou tem P99 de ponta a ponta maior na mesma carga, porque para a cada espera de confirmação |
| H5b | Na saída resumida, os três modos empatam: sem espera frequente para esconder, vale a conclusão do primeiro cenário |
