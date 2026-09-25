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
- **Efeito do broker:** diferença entre Kafka e RabbitMQ no mesmo modo, nas cargas comuns aos dois (até 60 mil ev/s), e pelos pontos de saturação acima disso.
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
