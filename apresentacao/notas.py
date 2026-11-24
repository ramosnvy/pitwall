"""Notas do apresentador, uma entrada por slide, na ordem final do arquivo.

Escritas para serem lidas na hora: frases curtas, em ordem igual à do slide,
com dicas entre colchetes (para onde apontar). Os parágrafos marcados
[se der tempo] são detalhes que podem ser pulados sem perder o argumento.
O build.py aplica estas notas por cima das que estão junto de cada slide.
Ritmo de referência: cerca de 140 palavras por minuto."""

NOTAS = [
# 1 --- capa
"""[0:25]

Bom dia (boa tarde) a todos. Meu nome é Pedro Augusto, e vou apresentar o meu TCC.

O trabalho é uma avaliação experimental. Eu medi, na prática, como se comportam arquiteturas de processamento de eventos em .NET quando se combina um broker de mensageria, o RabbitMQ ou o Kafka, com um mecanismo de concorrência dentro do consumidor, o Channels ou o Pipelines.

[Tempo previsto: cerca de 19 minutos sem os trechos marcados "se der tempo"; com todos eles, uns 22. Pule-os se estiver atrasado.]""",

# 2 --- contexto e problema
"""[1:30]

Sistemas de telemetria, de IoT e de monitoramento recebem milhares de eventos por segundo, e precisam tratar cada um quase em tempo real.

[aponte para as caixas, da esquerda para a direita]
Em .NET, essa arquitetura costuma ter duas camadas.
A primeira é o broker: o RabbitMQ ou o Kafka. Ele recebe os eventos do gerador e entrega ao consumidor.
A segunda fica dentro do consumidor: o mecanismo interno. Ele pode processar a mensagem ali mesmo, que é o que eu chamo de Direct, ou passar para outra thread com Channels ou Pipelines, que a Microsoft recomenda para esse tipo de cenário.

E por que estudar isso? São três motivos.
Primeiro, as comparações entre RabbitMQ e Kafka olham o broker sozinho, e quase sempre em Java. Mas numa aplicação .NET o que se sente é a cadeia inteira.
Segundo, Channels e Pipelines acrescentam uma fila e uma troca de thread a cada evento. Ninguém tinha medido se isso ajuda ou só custa.
Terceiro, uma comparação só é justa com ambiente e carga controlados. Senão, a gente mede a máquina, e não a arquitetura.

[aponte para a caixa rosada]
Daí as duas perguntas do trabalho: o mecanismo interno melhora, piora ou não muda o desempenho? E qual broker atende melhor cada faixa de carga, e quanto disso vem da configuração?""",

# 3 --- objetivos
"""[0:50]

O objetivo geral é avaliar experimentalmente essas arquiteturas compostas: o broker mais o mecanismo interno, em .NET.

Para isso, foram quatro objetivos específicos.
Um: implementar o consumidor para os dois brokers em três modos. Direct, Channels e Pipelines. O Direct é o grupo de controle: o mesmo consumidor, sem mecanismo nenhum. É a referência contra a qual eu meço o Channels e o Pipelines.
Dois: construir um gerador de carga reproduzível, em malha aberta, com dados reais da Fórmula 1, vindos da OpenF1.
Três: configurar os brokers de forma equivalente, em dois cenários. Um com os valores de fábrica, outro com a recomendação de cada fornecedor.
E quatro: medir latência, throughput, CPU e memória, e encontrar o ponto de saturação de cada uma das seis combinações.""",

# 4 --- conceitos
"""[1:25]

Antes de seguir, quatro conceitos rápidos.

[cartões da esquerda, de cima para baixo]
O Kafka é um log particionado. Ele junta as mensagens em lotes e grava em sequência, e a ordem é garantida dentro de cada partição.
O RabbitMQ é um broker de filas, do protocolo AMQP. Ele trata cada mensagem individualmente. E, com a mensagem persistente, grava em disco antes de confirmar que recebeu.

O Channels é uma fila de objetos entre threads. Quem recebe a mensagem coloca o evento na fila, e uma thread de trabalho tira e processa.
O Pipelines faz o mesmo, mas com bytes, e sem criar um objeto por evento.
[se der tempo] Ele passa os bytes crus, com buffers reaproveitados, e o leitor interpreta os registros direto no buffer.
Os dois tiram o processamento da thread que recebe.

[caixa rosada, à direita]
E, para medir, três ideias.
Uso percentis. O P99 é o valor abaixo do qual ficam 99% dos eventos. Ele mostra a cauda, e é a cauda que limita esses sistemas, não a média.
O gerador é de malha aberta: envia cada evento no horário marcado, sem esperar o sistema responder. Se esperasse, desaceleraria justamente quando o sistema atrasa e esconderia os piores atrasos.
[se der tempo] É o que o Tene chama de coordinated omission.
E o ponto de saturação é a maior carga que o sistema sustenta antes de a fila e a latência crescerem sem parar.""",

# 5 --- trabalhos relacionados
"""[0:55]

Estes são os cinco trabalhos mais próximos.

[percorra a tabela de cima para baixo]
O Kamiński compara os dois brokers em .NET, mas não olha o mecanismo interno.
[se der tempo] Ele usa Spring Boot e ASP.NET, e encontra o Kafka melhor em .NET.
O Umam e o Dobbelaere comparam os brokers em escalabilidade, throughput e latência: RabbitMQ para mensagens individuais com baixa latência, Kafka para alto volume. Nenhum dos dois em .NET.
O Fu propõe uma metodologia de comparação justa, com repetição, equivalência de configuração e percentis. Foi a base da minha metodologia.
E o Ortiz mostra que a camada de processamento acoplada ao broker afeta a latência, mas sem um grupo de controle.

[aponte para a caixa de baixo]
Então a lacuna é esta: nenhum combina os dois brokers com Channels e Pipelines, e nenhum mede o mecanismo interno contra um grupo de controle sem ele. Esse é o papel do modo Direct neste trabalho.""",

# 6 --- bancada
"""[2:05]

Esta é a bancada. Ela tem cinco etapas, da esquerda para a direita.

[etapa 1]
Os dados: telemetria real da OpenF1, da corrida do Bahrein de 2024, baixada uma única vez para uma cópia local.
A telemetria original vem a 3,7 leituras por segundo por carro. Eu reamostro para 100.
[se der tempo] Velocidade, rotação e acelerador são interpolados em linha reta; marcha, freio e DRS ficam em degrau. Então as frenagens e trocas de marcha são as da corrida real.
E, para chegar a centenas de milhares de eventos por segundo sem acelerar o tempo, a frota é multiplicada. Cada réplica ganha número de carro próprio e começa num ponto diferente da corrida.
[se der tempo] 100 mil eventos por segundo são mil carros.

[etapa 2]
O gerador publica em malha aberta, até 400 mil eventos por segundo.
[se der tempo] Cada evento é um registro binário de 46 bytes.

[etapa 3]
O broker divide a carga em quatro faixas: quatro partições no Kafka, quatro filas no RabbitMQ. O carro define a faixa pela mesma função nos dois, o CRC32. Assim, cada carro cai sempre na mesma faixa, e os eventos dele chegam em ordem.
Há um único leitor por faixa. Dois leitores poderiam processar fora de ordem os eventos de um carro, e a detecção de frenagens depende dessa ordem. O paralelismo vem das quatro faixas.

[etapa 4 — aponte para o quadro de baixo]
No consumidor estão os três modos. No Direct, o callback processa ele mesmo. No Channels e no Pipelines, ele entrega a um leitor numa thread de trabalho e volta a receber.
A única diferença entre as três linhas é essa entrega. Por isso o Direct é o grupo de controle: mesmo broker, mesma carga e mesmo código, só que sem o mecanismo. Então a diferença entre Channels e Direct é o efeito do Channels, e de nada mais.
Os três chamam o mesmo código: janelas de um segundo por carro e detecção de frenagens e trocas de marcha.
[se der tempo] São as operações de referência de benchmarks clássicos, como o Linear Road e o DEBS Grand Challenge.

[etapa 5]
E cada rodada gera um digest, uma impressão digital do resultado. Como todas as combinações processam a mesma entrada, todas têm de dar o mesmo digest. Se não derem, houve perda, duplicação ou quebra de ordem.""",

# 7 --- ambiente e medição
"""[0:40]

Tudo roda numa única máquina, em Docker.
[se der tempo] Um Core i5 de 12 threads, com 16 GB.

[aponte para os quadradinhos]
Cada componente tem núcleos exclusivos. Ninguém disputa CPU com ninguém.
[se der tempo] O gerador fica do 0 ao 3, o broker do 4 ao 7, o consumidor do 8 ao 10, e banco e métricas no 11. Durante a medição, só o broker medido fica ligado, e eu não uso o computador.

[caixa da direita]
A latência vai do envio ao fim do processamento. O gerador grava na mensagem o instante da publicação, e o consumidor compara com o mesmo relógio. Como tudo está na mesma máquina, não preciso sincronizar relógios.
CPU e memória vêm do Prometheus com o cAdvisor, por container.
[se der tempo] As latências vão para um HdrHistogram; alocação e coleta de lixo vêm dos contadores do .NET.
E os resultados vão para o PostgreSQL em lote. A latência termina antes da gravação, então o banco não entra na medida e não é gargalo.""",

# 8 --- metodologia
"""[1:20]

[caixas de cima]
Usei dois cenários de configuração.
No cenário padrão, os valores de fábrica, conferidos no código-fonte de cada versão. Só mudei o que impedia a equivalência entre os dois.
Uma diferença ficou de propósito, porque é o padrão de cada um: o RabbitMQ, com mensagem persistente, grava em disco antes de confirmar. O Kafka confirma com a mensagem no cache do sistema, como a documentação dele recomenda. Isso favorece o Kafka, e está declarado.
O cenário ajustado aplica a recomendação de cada fornecedor, e com isso a garantia fica igual nos dois.
[se der tempo] No Kafka, a espera para formar lotes, o linger.ms, cai para 1 milissegundo, e há um perfil para vazão com 20. No RabbitMQ, a mensagem passa a ser transitória, sem gravar em disco antes de confirmar, e o prefetch vai a 300.

[números do meio]
São seis combinações, dois brokers vezes três modos. Cada combinação e cada carga tem 10 repetições, de 90 segundos de medição, em ordem sorteada, até 400 mil eventos por segundo.

[caixa de baixo]
E as regras foram registradas antes da coleta.
Uma rodada é válida se o gerador não atrasou mais de 50 ms e se o digest bate.
O ponto de saturação é a maior carga com P99 abaixo de 50 ms.
E uma diferença só conta se passar de 20% no P99 ou de 10% no throughput. O teste de Kruskal-Wallis, a 5%, só acompanha.""",

# 9 --- comparação justa
"""[1:40]

Este slide responde a uma pergunta importante: como garanti que a comparação foi justa. São quatro frentes.

[cartão 1]
Primeiro, a mesma carga e o mesmo trabalho. Os mesmos dados e o mesmo código de processamento nos três modos. A mesma divisão em faixas nos dois brokers. E a fila interna do Channels e a do Pipelines com o mesmo volume: 10 mil eventos de 46 bytes são 460 kB. O digest prova que todos fizeram o mesmo trabalho.

[cartão 2]
Segundo, a mesma garantia de entrega: at-least-once nos dois. Nenhuma mensagem se perde, mas uma pode chegar duas vezes. No Kafka isso é o acks igual a all; no RabbitMQ, as confirmações ao produtor. Os valores de fábrica foram conferidos na fonte, para que nenhum broker fosse ajustado a favor.
[se der tempo] E desliguei a compressão: as réplicas da frota são quase iguais, e comprimir daria uma vantagem artificial ao Kafka.

[cartão 3]
Terceiro, as mesmas condições de execução: núcleos exclusivos, só o broker medido ligado, ordem das rodadas sorteada e o gerador em malha aberta.

[cartão 4]
E quarto, uma análise sem viés. As regras e o limite de relevância foram fixados antes de eu ver os dados, como recomendam Jain e Fu. O Direct como grupo de controle permite dizer se o mecanismo ajudou ou atrapalhou: sem ele, eu só saberia qual mecanismo é melhor, não se vale a pena usar algum.
[se der tempo] E dez repetições, com a regra de Tukey para tirar os discrepantes e a mediana para resumir.

[rodapé]
Cada decisão tem uma base: a documentação do fornecedor, a literatura ou uma medição da calibração. A lista completa está num slide de apoio, no final.""",

# 10 --- calibração
"""[1:10]

Antes da coleta oficial, fiz uma calibração: medições de teste para acertar a bancada. Ela mostrou que o ambiente pesava mais do que as próprias arquiteturas.

[da esquerda para a direita]
Com cota de CPU, o broker RabbitMQ era estrangulado. Com núcleos exclusivos, o P99 dele caiu de 40 a 64%. Então passei a usar núcleos exclusivos.
Com gerador e consumidor fora dos containers, o Kafka tinha latência com dois picos em 43% das rodadas de carga baixa. Coloquei tudo em containers.
Logo depois de uma pausa de disco da máquina virtual, a chance de a latência passar de 50 ms ficava 126 vezes maior. Por isso o atraso de envio virou critério de validade, e cada combinação tem dez repetições: um episódio isolado não decide o resultado.
E uma auditoria contra os valores de fábrica achou três defeitos de configuração e três assimetrias entre os brokers. Corrigi antes da coleta.

[rodapé]
[se der tempo] Também descartei o linger.ms zero: com ele, a mediana do Kafka passou de 400 ms já a 10 mil eventos por segundo.
Esses dados da calibração não entram na análise. Serviram só para consertar a bancada.""",

# 11 --- validade das rodadas
"""[0:45]

Antes dos resultados, a validade das rodadas.

[da esquerda para a direita]
Foram 390 rodadas no cenário padrão.
Zero divergências de digest. Ou seja: nenhum evento se perdeu, duplicou ou saiu de ordem, em nenhuma combinação.
Doze inválidas, todas do Kafka a 200 mil eventos por segundo ou mais, todas porque o gerador atrasou o envio. Isso coincide com as pausas de disco da máquina virtual que a calibração mostrou.
E a regra de Tukey tirou cerca de 8% das rodadas, como discrepantes.

[rodapé]
Como os pontos a 100 Hz são interpolados, repeti o Direct com os dados originais, a 3,7 Hz, sem interpolação. O P99 mudou menos de 10%, e as conclusões não mudam.""",

# 12 --- latência e throughput
"""[1:40]

Este é o resultado principal.
[aponte para a legenda] Laranja é RabbitMQ, azul é Kafka. O traço e o marcador indicam o modo. O eixo de baixo é a carga, em escala logarítmica, e a linha pontilhada é o limite de 50 ms.

[primeira mensagem — os brokers se cruzam]
Entre os brokers, a comparação muda de sinal com a carga.
Em carga baixa, o RabbitMQ é mais rápido: a 10 mil eventos por segundo, P99 de 2,3 ms, contra 5,8 do Kafka.
Mas, conforme a carga sobe, a cauda do RabbitMQ cresce. No P99 ele só ganha até 20 mil.
[se der tempo] Chega a 25 ms a 60 mil e a 75 ms a 80 mil.
O Kafka fica com o P99 entre 5,5 e 6,7 ms até 400 mil. Ele tem um piso: a mediana fica perto de 3 ms em qualquer carga. Esse piso é a espera para formar lotes.
[se der tempo] É o linger.ms, de 5 ms no padrão. Ele troca uns milissegundos de latência por lotes maiores e menos CPU.

[segunda mensagem — os modos se sobrepõem]
Entre os modos, as três linhas de cada broker ficam uma em cima da outra. A maior diferença no P99 foi de 11%, abaixo do limite de 20%.

[terceira mensagem — saturação; aponte para o gráfico de throughput]
O throughput é igual em todas as combinações, porque o gerador é de malha aberta: enquanto aguenta, cada uma processa exatamente o que chega. O que separa as combinações é até onde cada uma aguenta.
O RabbitMQ sustenta 60 mil e falha a 80 mil, com P99 acima de 50 ms.
O Kafka sustenta 400 mil, a maior carga que testei. O ponto de saturação dele fica acima do alcance da bancada.
E o modo não mudou o ponto de saturação em nenhum dos dois.""",

# 13 --- uso de recursos
"""[0:55]

Nos recursos, os quatro gráficos: CPU e memória, do consumidor e do broker.

[primeira mensagem; aponte para o gráfico de cima à esquerda]
A CPU do consumidor é a única medida em que os modos se separam, e para pior.
No Kafka, Channels e Pipelines custam de 1,9 a 3,3 vezes a CPU do Direct. São de 2,4 a 3,7 microssegundos a mais por evento, mais ou menos o custo de acordar uma thread.
Para comparar: o processamento em si leva 0,12 microssegundo. Passar o evento para outra thread custa de 20 a 30 vezes o próprio trabalho.
No RabbitMQ, o acréscimo é pequeno, de 4 a 9%.

[segunda mensagem; gráfico de cima à direita]
A CPU do broker explica a saturação. O RabbitMQ ocupa até 3,5 dos 4 núcleos. O Kafka fica em meio núcleo.
[se der tempo] O RabbitMQ trata cada mensagem sozinha: roteia, confirma e grava. O Kafka trabalha em lotes.

[terceira mensagem; gráficos de baixo]
A memória não depende do modo.
[se der tempo] No consumidor, de 87 a 117 MB em todas. No broker, a relação se inverte: o Kafka usa de 1,4 a 1,7 GB, contra 0,2 a 0,3 do RabbitMQ.
[se der tempo] E um ponto que parece estranho: o Pipelines foi feito para não criar um objeto por evento, mas a alocação foi igual nos três modos, cerca de 950 bytes por evento no Kafka. É porque quem aloca é a biblioteca cliente do broker, antes de o evento chegar ao mecanismo.""",

# 14 --- cenário ajustado
"""[1:15]

Agora o cenário ajustado, com a recomendação de cada fornecedor. O gráfico compara o modo Direct no padrão e no ajustado.

[primeira mensagem — RabbitMQ]
No RabbitMQ, a mensagem passou a ser transitória, a mesma garantia que o Kafka já tinha. E o prefetch, que é quantas mensagens o broker manda sem confirmação, foi para 300.
O P99 caiu de 43 a 76% em todas as cargas, e ele passou a sustentar 80 mil.
[se der tempo] De 2,3 para 0,65 ms a 10 mil, e de 25 para 10 ms a 60 mil. Sem a gravação em disco, sobrou CPU no broker.

[segunda mensagem — Kafka]
No Kafka, é uma troca.
Com o linger.ms de 1, a mediana caiu de 3 para 1 ms em todas as cargas. Isso confirma que o piso vinha da formação de lotes. Mas, com lotes menores, o broker recebe mais requisições: a CPU dele dobrou, e o P99 subiu nas cargas altas.
O perfil para vazão, com 20 ms, faz o contrário: P99 perto de 21 ms e 30% menos CPU.

[terceira mensagem — mecanismos]
E os mecanismos continuaram sem diferença de latência. A configuração muda os brokers, não os modos.
Ou seja: parte da diferença entre os brokers no cenário padrão vinha da configuração, e cada parâmetro troca latência por CPU.""",

# 15 --- onde cada combinação ganha
"""[0:55]

Juntando tudo numa tabela: onde cada combinação se sai melhor, o que ganha e o que custa.

[linhas do RabbitMQ]
O RabbitMQ com Direct ganha em carga baixa, com a menor latência da bancada, P99 de 2,3 ms. O custo é saturar em 60 mil.
Com Channels e Pipelines, o único caso próximo de um ganho foi a 40 mil: P99 6% menor, sem relevância.
[se der tempo] Sem significância estatística, e custando de 4 a 9% mais CPU.

[linhas do Kafka]
O Kafka com Direct ganha em carga alta: P99 estável até 400 mil, com o broker leve. O custo é o piso de 3 ms na mediana e mais memória.
Com Channels e Pipelines, no Kafka, não houve carga em que ganhassem. Só custaram: de 1,9 a 3,3 vezes a CPU.

[rodapé]
Em resumo: o broker decide o desempenho. O mecanismo interno decide só o custo.""",

# 16 --- conclusão
"""[1:40]

Voltando às duas perguntas.

[faixa rosada de cima]
A primeira: o mecanismo interno ajuda? Nesta carga, Channels e Pipelines não fizeram diferença no desempenho e pioraram o custo.
O motivo é que o processamento leva 0,12 microssegundo por evento. O consumidor termina um evento muito antes de o broker entregar o próximo. Quem limita é o broker. Não há trabalho acumulado para o mecanismo paralelizar.

[três colunas]
A segunda: qual broker? O RabbitMQ para carga baixa, com latência mínima. O Kafka para carga alta, com menos CPU.
A configuração pesa muito: um único parâmetro mudou o P99 em até 76%. Uma comparação entre brokers precisa de configurações auditadas.
E a literatura: o resultado confirma o Umam e o Dobbelaere, e, como o Fu defende, o ambiente pesou mais do que a arquitetura.
[se der tempo] Confirma também o Kamiński, que viu o Kafka melhor em .NET, mas só em carga alta.

[caixa da esquerda — estudo preliminar]
Um estudo preliminar mostra quando isso muda. O consumidor passou a republicar o resultado no Kafka e esperar a confirmação. No Direct, essa espera trava a thread que recebe, e ele parou em 64 mil eventos por segundo. Com Channels e Pipelines, a recepção continua enquanto outra thread espera, e eles chegaram a 200 mil.
Ou seja: quando o processamento espera algo, o mecanismo interno faz diferença.

[caixa da direita — limitações e trabalhos futuros]
As limitações viram os trabalhos futuros.
A carga era leve, de 46 bytes: aprofundar o estudo, com processamento mais pesado e mais leitores.
Uma máquina e um nó por broker: testar vários nós, com replicação.
[se der tempo] E filas clássicas, mensagem por mensagem: testar as filas de quórum, os streams e o processamento em lote.""",

# 17 --- referências
"""[0:10]

Estas são as referências citadas na apresentação.
[passe direto para o próximo slide]""",

# 18 --- encerramento
"""Muito obrigado pela atenção.

Fico à disposição para as perguntas da banca.

[Os slides de apoio, A1 e A2, vêm depois deste. Use só se perguntarem.]""",

# A1 --- apoio: decisões
"""Slide de apoio. Só para perguntas.

Cada linha é uma decisão: o que foi feito, por que e em que se baseia.
Se perguntarem "por que não fez X?", a resposta costuma estar na coluna do meio.

Atalhos para as perguntas mais prováveis:
- Por que o Direct? Separa o efeito do broker do efeito do mecanismo.
- Por que 100 Hz? Carga real sem acelerar o tempo; a 3,7 Hz, as conclusões não mudam.
- Por que um leitor por faixa? Para manter a ordem de cada carro; o paralelismo vem das quatro faixas.
- Por que o limite de 20%? Com dispersão pequena, até diferenças irrelevantes ficam estatisticamente significativas.""",

# A2 --- apoio: configuração
"""Slide de apoio. Só para perguntas.

São os parâmetros que saíram do valor de fábrica, e o motivo de cada um.

Se perguntarem sobre a assimetria do cenário padrão:
É deliberada, porque é o padrão de cada broker. O Kafka recomenda deixar a durabilidade para a replicação entre nós, e por isso confirma com a mensagem no cache. O RabbitMQ, com mensagem persistente, grava em disco antes. Na calibração, essa gravação custou 48% no P99 do RabbitMQ a 40 mil. O cenário ajustado iguala as garantias, e o RabbitMQ melhorou de 43 a 76%.

Se perguntarem sobre o prefetch:
No padrão é zero, que no RabbitMQ significa sem limite. No ajustado, 300, como o fornecedor recomenda. A confirmação em lotes de 100 cabe com folga nele.""",
]
