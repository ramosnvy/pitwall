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

*Mitigação:* previsto e desejado para isolar a arquitetura. Se o piloto mostrar empate entre as variantes, ligar o custo sintético por evento (já implementado, desligado por padrão) e tratar intensidade de processamento como fator explícito.
