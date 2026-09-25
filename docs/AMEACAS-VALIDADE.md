# Ameaças à validade

Diário das ameaças identificadas durante a implementação, com a mitigação adotada. Atualizar assim que uma nova aparecer — o objetivo é que a seção de ameaças do artigo seja montada a partir daqui, e não reconstruída de memória no fim.

## Validade interna

### Execução em máquina única

**Decidido:** aceita e declarada.

Produtor, broker, processamento e banco rodam no mesmo host, sem rede entre os componentes.

*Efeito favorável:* torna a medição de latência válida sem sincronização de relógios — produtor e consumidor compartilham o mesmo contador monotônico.

*Efeito adverso:* parte do que justifica o Kafka (replicação, tolerância a falhas, consumidores distribuídos) não é exercitada. As conclusões valem para implantação de nó único.

*Mitigação:* declarar explicitamente no artigo e limitar o alcance das conclusões. Uma bateria distribuída exigiria sincronização de relógios entre máquinas, cujo erro seria maior que o efeito medido.

### Virtualização (Docker Desktop sobre WSL2)

Os containers rodam numa VM Linux com 8 GB, enquanto os processos .NET rodam no host Windows. Há uma camada de virtualização entre eles.

*Mitigação:* limites fixos de CPU e memória por container, repetições em ordem aleatória, hardware e configuração do WSL2 documentados.

### Variância entre execuções — medida, e alta

**Evidência coletada.** A mesma configuração (`kafka-channels`, 4 faixas, 20 mil ev/s, 200 mil eventos) produziu, em execuções sucessivas:

| Execução | Latência média | P99 |
| --- | --- | --- |
| 1 | 3,92 ms | 7,02 ms |
| 2 | 25,40 ms | 49,25 ms |
| 3 | 515,75 ms | 3.035,14 ms |
| 4 | 3,64 ms | 6,36 ms |
| 5 | 3,67 ms | 6,62 ms |

Três ordens de grandeza de diferença sem que nada na configuração mudasse.

*Consequência direta:* **uma execução isolada não significa nada neste ambiente.** Isso valida, com dado próprio, a exigência de repetições, ordem aleatória e intervalos de confiança — e é material para a seção de metodologia, não apenas para as ameaças.

*Episódio a registrar:* a execução 3 foi inicialmente interpretada como custo da persistência, que havia sido ligada naquela rodada. Ao repetir a medição com persistência ligada (execuções 4 e 5), a latência voltou a 3,6 ms. A hipótese estava errada: era ruído de ambiente, não custo de banco. O caso ilustra por que nenhuma conclusão pode sair de uma execução única.

*Mitigação:* descartar rodadas cujo atraso máximo de emissão exceda o limite, repetir o suficiente para o intervalo de confiança desejado, e verificar outliers antes de interpretá-los.

*Correção posterior desta interpretação:* **parte dessa variância não era ruído, era sistemática.** A execução 2 (25,40 ms de média, 49,25 ms de P99) tem a assinatura exata do problema de timer descrito abaixo. A execução 3, de 515 ms, continua sem explicação e segue tratada como ruído de ambiente.

### Clientes no host Windows: latência bimodal do Kafka

**Evidência coletada.** Com produtor e consumidor rodando no Windows, o Kafka apresentava latência bimodal — cada rodada inteira caía ora em 3,4 ms de média, ora em 25,4 ms, em ~43% das rodadas de carga baixa. O RabbitMQ não foi afetado.

*Hipóteses testadas e descartadas:* resolução do timer do Windows (26 de 60 rodadas lentas mesmo com `timeBeginPeriod(1)`) e algoritmo de Nagle (5 de 10 lentas com Nagle desligado). Uma primeira confirmação da hipótese do timer, com 3 rodadas por lado, era coincidência — registrada aqui porque é o erro que este diário existe para evitar.

*Isolamento:* o broker, medido dentro do container, é estável (0 de 8 lentas). Os mesmos clientes .NET em containers na rede Docker: 0 de 10 lentas, P99 entre 5,80 e 5,89 ms. A causa está no caminho Windows→VM do Docker Desktop; o mecanismo exato não foi identificado.

*Consequência:* a latência do Kafka em carga baixa da matriz do commit `25fa684` está contaminada. Sem o isolamento, o Kafka seria penalizado pelo ambiente, não pela arquitetura, e de forma intermitente — o pior tipo de viés, porque parece ruído.

*Mitigação:* clientes em containers na rede Docker a partir do commit `e779e7c`, como o TCC1 já previa. Detalhes em [REVISAO-TECNICA.md](REVISAO-TECNICA.md), §1.1.

### Assimetria de durabilidade

Com um único nó, o Kafka confirma a mensagem com ela apenas no cache de página — é o comportamento recomendado pelo próprio Kafka, que conta com replicação para durabilidade. O RabbitMQ confirma mensagem persistente só depois de gravá-la em disco. **Nesta configuração o RabbitMQ oferece garantia mais forte**, o que favorece o Kafka. Custo medido: até 7,3%. Declarado, não corrigido: equalizar exigiria ou mensagens transientes no RabbitMQ ou fsync por mensagem no Kafka, e as duas opções fogem da prática recomendada de cada fornecedor.

### Equalização pela ordem expõe o RabbitMQ a head-of-line blocking

Para garantir ordem por carro nos dois brokers, o RabbitMQ usa P filas com roteamento por carro, em vez de fila única com consumidores concorrentes. Com isso ele herda a exposição a head-of-line blocking que o Kafka tem por partição. É uma escolha que favorece o modelo do Kafka e precisa ser declarada ao lado do resultado.

### Persistência como gargalo oculto

Se o PostgreSQL saturar, as seis variantes parecem iguais e a arquitetura fica invisível.

*Mitigação:* tabelas UNLOGGED, `synchronous_commit=off`, escrita em lote com `COPY` binário, latência medida até o fim do processamento e bateria adicional sem persistência.

### Faixas desbalanceadas no RabbitMQ

As filas do RabbitMQ recebem 25%, 15%, 25% e 35% da carga, porque a faixa é `carro % 4` e as réplicas da frota somam múltiplos de 100. As partições do Kafka recebem 25% cada, pelo CRC32 da chave. A fila mais carregada limita o teto e domina a cauda do RabbitMQ, o que favorece o Kafka na comparação. Medido e detalhado em [IMPLEMENTACAO.md](IMPLEMENTACAO.md) §1.

*Mitigação proposta:* usar no sink do RabbitMQ o mesmo CRC32 da librdkafka e refazer as células do RabbitMQ.

*Resultado do 2×2 (IMPLEMENTACAO §7):* a direção do efeito era a oposta da suposta. A 40 e 60 mil ev/s, as filas desbalanceadas davam ao RabbitMQ **menor** latência e menor CPU. O CRC32 foi adotado (commit `247fd7c`) por dar aos dois brokers a mesma divisão dos carros, e a matriz refeita mostrará o RabbitMQ sem essa vantagem.

### Cota de CPU estrangula o broker RabbitMQ

Com `--cpus`, o broker RabbitMQ foi estrangulado em 4 a 8% dos períodos CFS, crescendo com a carga e com a cauda. O broker Kafka ficou em 0%. Parte da cauda do RabbitMQ pode ser efeito do jeito de limitar CPU, não do broker. Detalhado em [IMPLEMENTACAO.md](IMPLEMENTACAO.md) §2.

*Mitigação proposta:* A/B com `--cpuset-cpus`. Se o P99 mudar, o protocolo muda para os dois brokers.

*Resultado do 2×2 (IMPLEMENTACAO §7):* confirmado. Com núcleos fixos, o P99 do RabbitMQ caiu de 40% a 64% em três das quatro células, acima do limiar de 20% fixado antes de medir. O protocolo passa a usar núcleos fixos nos brokers, e o Kafka precisa ser conferido.

### Mecanismos internos usados no modo padrão

A matriz usa Channels e Pipelines no modo mais simples: um worker por faixa, um evento por despertar e flush do pipe a cada mensagem. O excesso de CPU medido em relação ao Direct, ~3,6 µs por evento, tem a ordem de grandeza de acordar uma thread, não de pôr um item numa fila. A conclusão "desacoplar custa CPU sem ganho de latência" pode ser do uso, não das ferramentas.

*Mitigação:* a conclusão da matriz é apresentada como resultado do **uso padrão**. As camadas de lote, escalonamento, topologia e contrapressão são medidas à parte, com hipóteses declaradas antes, em [APROFUNDAMENTO.md](APROFUNDAMENTO.md).

### Observação que disputa CPU com o que é medido

Prometheus e cAdvisor rodam durante toda rodada, com 0,5 CPU cada, e são os mesmos em todas as variantes. O painel local (Grafana, Loki e Alloy, profile `dash`) é outra coisa: serve para depurar e acompanhar, e não fez parte da matriz `7e283c2`.

*Mitigação:* o painel fica desligado nas rodadas oficiais. Se for usado numa bateria, o custo dele aparece no próprio painel, em "Custo da própria observação", e a bateria é rotulada como tal.

### Gravação periódica do disco virtual para o Kafka

A cada 15 a 30 s, o Linux da VM do WSL2 grava em lote as páginas que o Kafka escreveu, cerca de 300 MB, e durante a gravação todas as tarefas ficam paradas esperando disco. O broker para de confirmar e de entregar por 200 ms a 2 s (IMPLEMENTACAO §9). O Kafka confia no cache do sistema operacional e não força a gravação; o RabbitMQ, com mensagem persistente, grava aos poucos e confirma depois de cada gravação.

*Efeito:* episódios de latência alta em parte das rodadas do Kafka a partir de 100 mil ev/s, no Direct e no Channels.

*Mitigação:* declarar como comportamento do Kafka nesta máquina. A análise usa a mediana de 10 repetições e o descarte de Tukey (ANALISE.md), então um episódio isolado não decide o ponto de saturação. Ajustar a gravação da VM (`vm.dirty_background_bytes`, `vm.dirty_expire_centisecs`) mexe no kernel da máquina e fica fora do perfil padrão; entra só como teste de sensibilidade, se decidido.

### Coordinated omission

Gerador de carga em malha fechada desaceleraria sob saturação e deixaria de registrar as latências altas, justamente as que os percentis P95 e P99 medem.

*Mitigação:* gerador em malha aberta, que emite no instante previsto independentemente do destino. O atraso máximo de emissão é registrado por rodada; acima de 50 ms a rodada é descartada.

## Validade de construto

### O workload não é telemetria real em taxa real

A OpenF1 entrega 3,7 Hz por carro. Um carro de F1 real tem de 150 a 300 sensores amostrando até 100 Hz.

*Mitigação:* declarar a diferença e obter a carga por multiplicação de frota, que preserva a cadência real de cada sensor, em vez de aceleração do tempo, que a distorceria.

*A partir da matriz refeita:* o cenário principal usa 100 Hz por carro por interpolação (PLANO 1a). Isso resolve a cadência, mas cria outra ameaça.
- **Os pontos entre duas amostras reais não são medidos:** são interpolados, em linha reta nos canais contínuos e em degrau nos discretos.
- **O que isso não afeta:** para os brokers o conteúdo não importa, porque o evento tem 46 bytes do mesmo jeito. Para o processamento, frenagens e trocas de marcha são idênticas às da corrida medida, e há teste que garante isso.
- **O que muda:** a forma da carga. São menos carros distintos, mais eventos por carro e por janela, e menos janelas gravadas.
- **Lacunas:** intervalos acima de 1 s não são preenchidos. No Bahrein são ~28 por carro, segundo o replay.
- **Sensibilidade:** a bateria a 3,7 Hz no mesmo protocolo mostra o que depende da frequência.

### Os dois brokers no ar durante a matriz `7e283c2`

O runner não ligava nem desligava os brokers, e a matriz embaralhava rodadas dos dois. Os dois containers ficaram no ar o tempo todo, e o ocioso disputava a máquina com o medido, ainda que com pouca CPU. Isso contraria o PLANO ("um broker por vez").

*Mitigação:* desde o protocolo de núcleos exclusivos, o runner deixa só o broker medido no ar (`Use-Broker`), com as rodadas agrupadas por broker e embaralhadas dentro de cada bloco.

### Payload repetitivo favorece compressão

As réplicas de uma mesma amostra têm payload quase idêntico. A compressão em lote do Kafka renderia muito mais do que renderia com telemetria real.

*Mitigação:* compressão desligada nos dois brokers na comparação controlada. Registrar na discussão que Kafka em produção costuma comprimir, e que este workload superestimaria esse ganho.

### Equivalência de configuração entre brokers

**Decidido:** mesma garantia de entrega nos dois.

Configurações desiguais fariam a comparação medir ajuste, não arquitetura.

*Mitigação:* at-least-once dos dois lados (`acks=all` no Kafka, publisher confirms no RabbitMQ), sem idempotência, com todos os parâmetros numa tabela do artigo.

### Grau de paralelismo

**Decidido:** cada broker usa o mecanismo que lhe é natural, mas com o mesmo número de threads de processamento concorrente.

No Kafka o paralelismo vem das partições e do grupo de consumidores; no RabbitMQ, de múltiplos consumidores na mesma fila com `prefetch`. São mecanismos diferentes para o mesmo fim.

*Mitigação:* fixar o grau de paralelismo P e configurar cada broker para atingi-lo do seu jeito — P partições e P consumidores no Kafka, P consumidores com `prefetch` ajustado no RabbitMQ. Declarar P e os dois arranjos no artigo.

## Validade de conclusão

### Divergência entre variantes invalida a rodada

As seis variantes processam a mesma entrada e devem produzir resultado idêntico. Divergência indica perda de evento, quebra de ordem ou defeito.

*Mitigação:* `ResultDigest` combinado por soma e XOR, independente da ordem de fechamento das janelas. Verificado: resultado idêntico com 1, 2, 4 e 8 workers.

### Processamento leve demais pode achatar as diferenças

O processamento roda a 8,6 milhões de eventos/s em uma thread, cerca de 1% de um núcleo no nível de carga mais alto planejado. O experimento será dominado pelo transporte.

*Mitigação:* previsto e desejado para isolar a arquitetura. Se as variantes empatarem, seguir a escalada da seção abaixo.

### Empate entre variantes: escalada definida

Um empate só é resultado se for **medido**, não se for ruído. Antes de qualquer conclusão, definir o tamanho de efeito que importa — por exemplo 10% de vazão ou 20% no P99 — e dimensionar as repetições para que o experimento consiga detectá-lo. Sem isso, "não houve diferença" é apenas dizer que a medição foi imprecisa.

Ordem de escalada quando as variantes empatam:

1. **Subir a carga até a saturação.** Empate em carga baixa é o esperado: nada está sendo pressionado. As diferenças aparecem no joelho da curva. Um empate abaixo de certa carga já é um achado publicável — *abaixo de X ev/s, a escolha da arquitetura não afeta o desempenho*.
2. **Aumentar a mensagem para ~1 KB.** Mensagem maior carrega o caminho de bytes, onde Pipelines deve se distinguir de Channels.
3. **Ligar o custo sintético por evento** (5 e 20 µs, já implementado e desligado). Desloca o gargalo do transporte para o processamento, que é onde os mecanismos internos diferem.
   *Atualização de 24/09/2026:* a escalada preferida passou a ser o segundo cenário (APROFUNDAMENTO §8), em que o mecanismo interno tem espera real de broker para esconder, em vez de um custo inventado.
4. **Reportar o empate com intervalos de confiança.** "Sem diferença estatisticamente significativa" é resultado, não fracasso: a conclusão prática vira *escolha por critérios operacionais, não por desempenho* — o que é conselho de engenharia legítimo e útil.

## Segundo cenário (se aprovado)

Ameaças próprias do cenário de APROFUNDAMENTO §8. Valem só se ele for construído.

### A espera pela marca d'água se mistura à latência do broker

Para o resultado não depender da ordem de chegada entre tópicos, a telemetria espera a posição na pista do mesmo instante. Essa espera entra na latência de ponta a ponta sem ser custo do broker.

*Mitigação:* medir a espera à parte, em cada rodada, e reportar a latência com e sem ela.

### Roteamento nativo com custo diferente

O Kafka separa os tipos em tópicos; o RabbitMQ usa um exchange *topic*, cujo roteamento por padrão de chave custa mais que o *direct* do cenário atual. É o recurso nativo de cada um para o mesmo problema, mas o custo não é o mesmo.

*Mitigação:* medir o custo do exchange *topic* contra o *direct* na varredura e declarar no texto.

### A saída enriquecida dobra a carga do broker

Com uma mensagem de saída por mensagem de entrada, o mesmo broker carrega os dois saltos, e os pontos de saturação caem.

*Mitigação:* varredura própria antes da matriz. Os resultados do segundo cenário não são comparados ponto a ponto com os do primeiro, só as conclusões.

### Assimetria de durabilidade em dobro

Com mensagem persistente, o RabbitMQ grava em disco antes de confirmar, nos dois saltos; o Kafka não força a gravação em nenhum (AUDITORIA-CONFIG, assimetria A). Parte do ganho que H5a prevê para Channels e Pipelines no RabbitMQ pode vir de esconder essa gravação.

*Mitigação:* rodar a saída enriquecida também com mensagem transitória no RabbitMQ, em uma carga, para separar os dois efeitos.

### Posição na pista mais antiga que a telemetria

A telemetria está a 100 Hz e a posição a 3,7 Hz: o enriquecimento usa uma posição de até cerca de 270 ms antes. Afeta o significado do resultado (a curva atribuída), não o desempenho medido.

*Mitigação:* declarar. O trabalho não analisa a corrida (PLANO §1g).

### Consumidor final dividindo núcleo

O consumidor final precisa de um núcleo. Se ficar no núcleo 11, divide espaço com o banco e a coleta de métricas.

*Mitigação:* medir a CPU dele na fumaça. Acima de 50% do núcleo, rever a divisão de núcleos antes da varredura.

## Testes adiados, a lembrar

- **Bateria exploratória com compressão ligada no Kafka** (lz4 ou zstd), rotulada como não comparável com a bateria controlada, para registrar quanto o ganho seria superestimado por este workload.
- **Ruído por réplica e teste de sensibilidade**, para verificar empiricamente se a replicação da frota afeta latência e vazão. Se não afetar, a replicação fica justificada por medição e não por argumento.
- **Execução longa** (10 a 15 min) por arquitetura na carga de saturação, para confirmar que as janelas curtas de medição não escondem deriva causada por GC, rotação de segmento ou descarga de cache.
