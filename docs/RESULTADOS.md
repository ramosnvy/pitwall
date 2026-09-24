# Resultados da matriz oficial

Matriz containerizada, commit `7e283c2`: produtor, consumidor, brokers e PostgreSQL em containers na mesma rede Docker, com limites fixos de CPU e memória. Dataset: corrida do Bahrein 2024 (`session_key` 9472), carga obtida por multiplicação de frota. Cada rodada: 10 s de aquecimento descartado e 90 s de medição, persistência ligada, ordem aleatória, 5 repetições por célula.

Dados brutos em `results/matrix-v2-runs.csv`; consolidação em `results/summary-v2.csv` (`analysis/summarize.ps1`); teste estatístico em `analysis/mechanism-test.ps1`.

**Estado:** resultados completos para 10 mil a 200 mil ev/s. A 400 mil ev/s a máquina satura e as células não têm repetições válidas suficientes para análise — ver §6.

## 1. Validade

| | Rodadas |
| --- | --- |
| Executadas | 165 |
| Válidas | 154 |
| Inválidas por jitter do produtor acima de 50 ms | 10, todas a 400 mil ev/s |
| Inválidas por digest divergente | 1, a 400 mil ev/s |
| Falhas de execução | 0 |

Até 200 mil ev/s, **todas as 150 rodadas são válidas**, com digest idêntico em todas as rodadas de cada carga — nenhuma perda nem reordenação de evento.

A rodada com digest divergente (`kafka-direct#5` a 400 mil) teve o produtor encerrado por fila local cheia (`Local: Queue full`) após publicar 37,4 dos 40 milhões de eventos. Dois defeitos do instrumento foram corrigidos a partir dela (commit posterior): o produtor passou a aplicar contrapressão quando a fila enche, e o relatório do produtor passou a ser casado com o do consumidor pelo identificador da rodada — a rodada tinha herdado o jitter da rodada anterior e parecia válida; só o digest a barrou.

A dispersão entre repetições é pequena: o P99 do Kafka varia cerca de 1% entre as cinco repetições de uma célula. Em algumas células uma das cinco rodadas tem um pico isolado (por exemplo, `kafka-pipelines` a 100 mil, P99 de 196 ms numa rodada contra 5,6 a 5,8 ms nas demais); por isso as estatísticas usam a mediana, e o intervalo mínimo–máximo está em `summary-v2.csv`.

## 2. Vazão e teto de saturação

Até o teto de cada broker, todas as variantes entregaram exatamente a taxa pedida: abaixo da saturação, a vazão é determinada pela carga e não distingue arquiteturas. O que distingue é **até onde** cada uma sustenta, medido na varredura de saturação (docs/PILOTO.md):

| Broker | Teto sustentado |
| --- | --- |
| RabbitMQ (três modos) | ~60 mil ev/s |
| Kafka (três modos) | acima de 200 mil ev/s nesta máquina, com persistência |

O teto real do Kafka não foi encontrado: a 400 mil ev/s o que satura primeiro é a máquina e o módulo de persistência (§6), não o broker.

**Ressalva posterior sobre o RabbitMQ** ([IMPLEMENTACAO.md](IMPLEMENTACAO.md)). Duas assimetrias da implementação podem ter rebaixado o teto e inflado a cauda do RabbitMQ:
- as filas recebem 25%, 15%, 25% e 35% da carga, contra 25% em cada partição do Kafka;
- o broker RabbitMQ foi estrangulado pela cota de CPU em 4 a 8% dos períodos, contra 0% do Kafka.

Até a verificação, o teto de ~60 mil ev/s e a cauda do RabbitMQ valem para esta implementação, não para o broker em geral.

## 3. Latência: a resposta depende do percentil

Mediana entre as cinco repetições; os três mecanismos agrupados, por não diferirem de forma relevante (§4).

| Carga (ev/s) | RabbitMQ P50 | Kafka P50 | RabbitMQ P99 | Kafka P99 |
| --- | --- | --- | --- | --- |
| 10 mil | **0,48 ms** | 3,04 ms | **1,4 ms** | 5,8 ms |
| 20 mil | **0,52 ms** | 3,06 ms | **2,3 ms** | 5,7 ms |
| 40 mil | **0,64 ms** | 3,10 ms | 7,5 a 9,7 ms | **5,7 ms** |
| 60 mil | **0,99 ms** | 3,10 ms | 22,8 ms | **5,6 ms** |
| 100 mil | — | 3,2 ms | — | 5,7 ms |
| 200 mil | — | 3,2 ms | — | 5,6 ms |

**Na mediana, o RabbitMQ tem menor latência em todas as cargas em que opera** — de 3 a 6 vezes menor.

**Na cauda, há um cruzamento entre 20 e 40 mil ev/s.** O P99 do RabbitMQ cresce 16 vezes entre 10 e 60 mil ev/s. O do Kafka fica plano em ~5,7 ms numa faixa de 20 vezes de carga, de 10 mil a 200 mil ev/s.

Qual broker tem "menor latência" depende de otimizar para o caso típico ou para o pior caso, e da carga. É o resultado principal desta comparação.

**Ressalva obrigatória.** O piso de ~3 ms do Kafka vem em grande parte de `linger.ms=5`, padrão da librdkafka: o produtor espera até 5 ms para agrupar mensagens, o que dá cerca de 2,5 ms de espera média deliberada. É uma escolha de configuração que troca latência por eficiência de lote, não um limite da arquitetura.

## 4. O mecanismo interno: não muda a latência, muda o custo

Teste de Kruskal-Wallis entre direct, channels e pipelines, dentro de cada broker e carga (5 repetições por grupo, α = 0,05).

**Latência (P99).** Sem diferença significativa em 8 das 10 células. As duas exceções — Kafka a 10 e 20 mil ev/s, p < 0,01 — têm tamanho de efeito de 0,04 a 0,06 ms, cerca de 1%: a dispersão das medições é tão pequena que até diferenças irrelevantes se tornam estatisticamente detectáveis. **Significância estatística não é relevância prática**, e o texto deve reportar o tamanho do efeito junto com o p-valor.

**CPU do consumidor.** Diferença significativa em 11 das 12 células:

| Carga | Kafka direct | Kafka channels | Kafka pipelines | RabbitMQ direct | RabbitMQ channels | RabbitMQ pipelines |
| --- | --- | --- | --- | --- | --- | --- |
| 10 mil | 14% | 20% | 20% | 65% | 71% | 71% |
| 60 mil | 28% | 50% | 53% | 93% | 102% | 103% |
| 200 mil | 58% | 131% | 141% | — | — | — |

**Sob carga uniforme e processamento barato, desacoplar a busca do processamento não melhora a latência e custa CPU** — no Kafka, mais que o dobro a 200 mil ev/s. O processamento custa ~0,12 µs por evento, então não há bloqueio na cabeça da fila a mitigar. É o trade-off que o TCC1 pede para identificar, e é um resultado negativo legítimo. Ele motiva o experimento de evento lento (REVISAO-TECNICA.md §4.2), que mediria **quando** o desacoplamento passa a compensar.

**Channels e Pipelines são indistinguíveis** em latência e em CPU. A conclusão vale para o Pipelines usado por mensagem, como está implementado; ela não se generaliza para o uso em lote, para o qual a ferramenta foi projetada (REVISAO-TECNICA.md §2.1).

**Estas conclusões são do uso padrão dos mecanismos.** O excesso de CPU equivale a ~3,6 µs por evento, a ordem de grandeza de acordar uma thread. Com o uso atual, isso acontece a cada evento. Se o custo vem do uso e não das ferramentas é a pergunta Q1 de [APROFUNDAMENTO.md](APROFUNDAMENTO.md).

## 5. Custo de recursos

CPU total da arquitetura — produtor, broker, consumidor e banco —, em percentual de um núcleo:

| Carga | Kafka | RabbitMQ | Razão |
| --- | --- | --- | --- |
| 10 mil | 164 a 170% | 507 a 514% | 3,0× |
| 60 mil | 184 a 214% | 661 a 693% | 3,3× |
| 100 mil | 204 a 242% | — | |
| 200 mil | 244 a 328% | — | |

O Kafka a 200 mil ev/s consome menos CPU que o RabbitMQ a 10 mil. Só no broker, o RabbitMQ gasta de 5 a 6 vezes mais (216 a 299% contra 42 a 51%), e o broker Kafka mal varia com a carga: 42% a 10 mil, 49 a 51% a 200 mil.

Parte do custo do RabbitMQ decorre da garantia mais forte que ele oferece nesta configuração — confirma mensagem persistente só depois do fsync, enquanto o Kafka, num único nó, confirma com a mensagem no cache de página (AMEACAS-VALIDADE.md). O custo medido dessa diferença foi de até 7,3% na vazão; não explica a razão de 3×.

A CPU do produtor inclui a espera ativa do gerador em malha aberta, que ocupa cerca de um núcleo nos dois brokers; a diferença entre eles é o custo do cliente de publicação.

**Memória.** O broker Kafka aparece com 1,5 a 1,7 GB porque o heap da JVM é pré-alocado (`-Xms1536m`); o número reflete a configuração, não o consumo, e não deve ser comparado com os 150 a 250 MB do RabbitMQ. O consumidor usa 90 a 110 MB nas seis variantes.

## 6. Onde a arquitetura inteira satura

Até 200 mil ev/s nada saturou: zero janelas descartadas na persistência, banco com no máximo 17% de CPU. A 400 mil ev/s:

- **O broker Kafka segue folgado:** 52 a 54% de CPU.
- **A persistência não acompanha:** janelas descartadas em quase todas as rodadas.
- **O consumidor se aproxima do limite:** 213 a 225% de CPU nas variantes desacopladas, num teto de 300%.
- **A máquina satura:** 10 das 15 rodadas com jitter do produtor acima de 50 ms, e um produtor encerrado por fila local cheia.

A 400 mil, quem quebra primeiro é o conjunto máquina + persistência, não o broker. Como a arquitetura do TCC1 inclui o módulo de persistência em PostgreSQL, este é um resultado sobre ela inteira: nesta máquina, o limite prático da arquitetura proposta está entre 200 e 400 mil ev/s, e o gargalo é o banco e a CPU compartilhada, não o Kafka. As células de 400 mil têm 1 a 2 repetições válidas e não sustentam comparação estatística.

## 7. Síntese

- **RabbitMQ:** menor latência típica em carga moderada, pago com cauda que degrada rápido acima de ~30 mil ev/s, teto de ~60 mil ev/s e cerca de 3× mais CPU.
- **Kafka:** latência típica mais alta, dominada pelo agrupamento de 5 ms, mas estável numa faixa de 20× de carga; folga de ao menos 3× acima do teto do RabbitMQ nesta máquina; muito mais eficiente em CPU.
- **Mecanismo interno:** sob processamento barato e uniforme, irrelevante para a latência e caro em CPU. A pergunta "quando ele compensa" exige o experimento de evento lento.
- **Arquitetura completa:** o limite prático, nesta máquina, está no módulo de persistência e na CPU compartilhada, não no broker.

## 8. Limitações a declarar

Registradas em detalhe em AMEACAS-VALIDADE.md. As que afetam a leitura destes números:

- Máquina única: não há rede entre os componentes, e todos disputam os mesmos 12 núcleos.
- O piso de latência do Kafka é configuração (`linger.ms`), não arquitetura.
- Assimetria de durabilidade que favorece o Kafka.
- Pipelines usado por mensagem, não em lote.
- Uma única corrida como workload.
- Equalização pela ordem por carro, que expõe o RabbitMQ a head-of-line blocking que ele não teria na configuração de fila única.

## 9. Próximos passos

1. **Experimento de evento lento** — mede onde o desacoplamento passa a compensar; é o complemento direto do §4.
2. **Pipelines em lote** — variante adicional, para que a conclusão sobre Pipelines não dependa de um uso subótimo.
3. **Limite por fila do RabbitMQ** — repetir o teto com 8 faixas para testar a hipótese de que cada fila clássica, sendo um único processo Erlang, limita a vazão por fila.
4. **Figuras para o artigo** — curvas de P50 e P99 por carga, com o cruzamento; CPU total por carga.
