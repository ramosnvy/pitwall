# TCC2 — Decisões para validar com o orientador

23/09/2026 · Pedro Augusto Ramos de Sousa

A implementação está em andamento e expôs decisões que o TCC1 não define. Este documento lista o que precisa de aval, cada item com a recomendação e o que muda se for outra.

Versão compartilhável: https://claude.ai/code/artifact/61e96e12-c912-489c-a0ef-71520622dae5

## Decisões que mudam o escopo declarado no TCC1

Oito pontos. Todos já estão implementados na forma recomendada; mudar qualquer um agora é barato, depois dos experimentos não é.

| # | Decisão | Recomendação | Se for outra |
| --- | --- | --- | --- |
| 1 | Incluir a baseline *Direct* (processamento no próprio callback, sem mecanismo interno) | Incluir | A matriz volta a 2 × 2 e não é possível separar o efeito do broker do efeito do mecanismo interno |
| 2 | Como gerar carga alta a partir de 75 ev/s reais | Multiplicar carros (frota de 26.820 a 3,7 Hz) | Acelerar o tempo 1341× distorce o intervalo entre amostras de um mesmo carro |
| 3 | Formato da mensagem | Binário de 44 bytes | Com JSON, o tempo de parse domina a medição e achata a diferença entre Channels e Pipelines |
| 4 | Tamanho da mensagem como fator do experimento | Dois níveis: 44 B e ~1 KB | Sem isso o trabalho não mostra onde está o ponto de virada entre os brokers |
| 5 | Compressão nos brokers | Desligada nos dois | As réplicas têm payload quase idêntico; a compressão infla o throughput do Kafka por artefato do workload |
| 6 | O que o módulo de processamento calcula | Agregação em janela por carro + detecção de frenagem e troca de marcha | O TCC1 não define; sem definição as seis variantes não são comparáveis |
| 7 | Grau de paralelismo (consumers e workers) | Fixo e igual nos dois brokers, declarado no artigo | Vira fator escondido: a comparação mede configuração, não arquitetura |
| 8 | Semântica de entrega | At-least-once nos dois (`acks=all` e publisher confirms), sem idempotência | Garantias diferentes tornam a comparação inválida |

As decisões 1, 4 e 6 alteram os objetivos específicos e a Figura 1 do TCC1.

## A ameaça à validade que precisa de decisão

Produtor, broker, processamento e banco rodam todos na mesma máquina, sem rede entre eles. Isso tem dois efeitos opostos.

O lado bom: é o que torna a medição de latência válida. Produtor e consumidor compartilham o mesmo contador de tempo, então não existe erro de sincronização de relógios entre a publicação e o processamento.

O lado ruim: parte do que justifica o Kafka — replicação, tolerância a falhas, consumidores em máquinas distintas — não aparece nessa configuração. A conclusão do trabalho descreve uma implantação de nó único, não um cenário distribuído.

**Pergunta:** declarar essa limitação nas ameaças à validade e manter o escopo de nó único, ou buscar uma segunda máquina para uma bateria distribuída?

**Recomendação:** declarar e manter. Uma bateria distribuída exigiria sincronização de relógios entre as máquinas, o que acrescenta uma fonte de erro maior que o efeito medido.

## O que acrescentar ao texto

**Trabalhos Relacionados: falta a literatura de benchmarks.** O TCC1 cita apenas comparações entre brokers. A linha de benchmarks de stream processing é o que fundamenta a escolha das operações e do protocolo experimental, e responde antecipadamente a "por que essas operações?".

| Fonte | O que sustenta |
| --- | --- |
| [DEBS 2013 Grand Challenge](https://debs.org/grand-challenges/2013/) | Telemetria esportiva a 200 Hz e 2000 Hz, ~15 mil ev/s, como carga de referência. É precedente direto para usar telemetria de F1; avalia submissões por vazão, latência **e correção** |
| [Linear Road](https://www.researchgate.net/publication/2949008_Linear_Road_A_Stream_Data_Management_Benchmark) | Agregação em janela agrupada e detecção de evento sobre telemetria veicular |
| [RIoTBench](https://arxiv.org/abs/1701.08530) | Mede latência, throughput, CPU, memória e *jitter* em cargas de IoT |
| [ESPBench](https://arxiv.org/pdf/2103.06775) e [survey TPCTC 2024](https://hpi.de/fileadmin/user_upload/fachgebiete/rabl/publications/2024/streamsurvey_tpctc_2024.pdf) | Taxonomia das operações de referência |

**Metodologia: quatro itens ausentes.**

1. **Ponto de saturação** como métrica principal — a carga em que a latência dispara. É o número mais forte que o trabalho pode produzir e não está na lista de métricas.
2. **Correção do resultado** como critério de validade da rodada: as seis variantes precisam produzir resultado idêntico. Já implementado e verificado.
3. **Níveis de carga e repetições**: o TCC1 diz "diferentes níveis" sem valores. Proposta: 1k, 10k, 50k e 100k ev/s mais o teste de saturação, com 10 repetições em ordem aleatória.
4. **Protocolo de isolamento entre rodadas**: tópico recriado e tabelas truncadas a cada execução.

**Uma ressalva a declarar:** a OpenF1 entrega 3,7 Hz por carro, enquanto um carro de F1 real tem de 150 a 300 sensores amostrando até 100 Hz. Melhor dizer isso no texto do que deixar a banca apontar — a multiplicação de frota é a resposta.

## Pendências do documento do TCC1

- Nome do orientador truncado ("Prof. Dr. Evandro K.") e campo de coorientador vazio.
- Membros da banca ainda com o texto de exemplo do modelo.
- Legendas saem como "Figure" e "Table" no template SBC.
- Atualizar as datas de "Acesso em: maio 2025" nas referências.
- Figura 1 e objetivos específicos precisam incluir a baseline e o replayer.

## Estado da implementação

Já funciona e está verificado: coleta do dataset (3 corridas completas, 444 mil eventos em `car_data` no Bahrein 2024), infraestrutura em Docker (Kafka, RabbitMQ, PostgreSQL, Prometheus), gerador de carga em malha aberta, publicação nos dois brokers e módulo de processamento.

O gerador sustenta 500 mil ev/s com erro de 0,00%, cinco vezes o nível de carga mais alto planejado — qualquer saturação medida será da arquitetura, não do instrumento. O processamento roda a 8,6 milhões de ev/s em uma thread, o que indica que o experimento será dominado pelo transporte.

Falta implementar os consumers nos três modos, a persistência em lote, a coleta de métricas e os scripts de experimento.
