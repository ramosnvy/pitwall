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

### Coordinated omission

Gerador de carga em malha fechada desaceleraria sob saturação e deixaria de registrar as latências altas, justamente as que os percentis P95 e P99 medem.

*Mitigação:* gerador em malha aberta, que emite no instante previsto independentemente do destino. O atraso máximo de emissão é registrado por rodada; acima de 50 ms a rodada é descartada.

## Validade de construto

### O workload não é telemetria real em taxa real

A OpenF1 entrega 3,7 Hz por carro. Um carro de F1 real tem de 150 a 300 sensores amostrando até 100 Hz.

*Mitigação:* declarar a diferença e obter a carga por multiplicação de frota, que preserva a cadência real de cada sensor, em vez de aceleração do tempo, que a distorceria.

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
4. **Reportar o empate com intervalos de confiança.** "Sem diferença estatisticamente significativa" é resultado, não fracasso: a conclusão prática vira *escolha por critérios operacionais, não por desempenho* — o que é conselho de engenharia legítimo e útil.

## Testes adiados, a lembrar

- **Bateria exploratória com compressão ligada no Kafka** (lz4 ou zstd), rotulada como não comparável com a bateria controlada, para registrar quanto o ganho seria superestimado por este workload.
- **Ruído por réplica e teste de sensibilidade**, para verificar empiricamente se a replicação da frota afeta latência e vazão. Se não afetar, a replicação fica justificada por medição e não por argumento.
- **Execução longa** (10 a 15 min) por arquitetura na carga de saturação, para confirmar que as janelas curtas de medição não escondem deriva causada por GC, rotação de segmento ou descarga de cache.
