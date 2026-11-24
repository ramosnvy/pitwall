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
"""[1:10]

Sistemas de telemetria, de IoT e de monitoramento recebem milhares de eventos por segundo, e precisam tratar cada um quase em tempo real.

[aponte para as caixas, da esquerda para a direita]
Em .NET, essa arquitetura costuma ter duas camadas.
[se der tempo] A primeira é o broker: o RabbitMQ ou o Kafka. Ele recebe os eventos do gerador e entrega ao consumidor.
A segunda fica dentro do consumidor: o mecanismo interno. Ele pode processar a mensagem ali mesmo, que é o que eu chamo de Direct, ou passar para outra thread com Channels ou Pipelines, que a Microsoft recomenda para esse tipo de cenário.

E por que estudar isso? São dois motivos.
Primeiro, as comparações entre RabbitMQ e Kafka olham o broker sozinho, e quase sempre em Java. Mas numa aplicação .NET o que se sente é a cadeia inteira.
Segundo, Channels e Pipelines acrescentam uma fila e uma troca de thread a cada evento. Ninguém tinha medido se isso ajuda ou só custa.

[aponte para a caixa rosada]
Daí as duas perguntas do trabalho: o mecanismo interno melhora, piora ou não muda o desempenho? E qual broker atende melhor cada faixa de carga, e quanto disso vem da configuração?""",

# 3 --- objetivos
"""[0:55]

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
E como eu meço? Três ideias.
Primeiro, olho a cauda, não a média. O P99 é o tempo que 99% dos eventos não ultrapassam. É o evento atrasado que o usuário sente, e a média esconde ele.
Segundo, o gerador é de malha aberta: ele manda cada evento na hora marcada, sem esperar o sistema. Se esperasse, ele iria mais devagar justo quando o sistema atrasa, e os piores atrasos sumiriam da medição.
[se der tempo] O Tene chama esse erro de coordinated omission.
Terceiro, o ponto de saturação: a maior carga que o sistema aguenta antes de a latência disparar.""",

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
"""[2:45]

Esta é a bancada. Ela tem cinco etapas, da esquerda para a direita.

[etapa 1]
Os dados são da OpenF1: telemetria real da corrida do Bahrein de 2024.
A telemetria original vem a 3,7 leituras por segundo por carro. Eu reamostro para 100.
[se der tempo] Velocidade, rotação e acelerador são interpolados em linha reta; marcha, freio e DRS ficam em degrau. Então as frenagens e trocas de marcha são as da corrida real.
E, para chegar a centenas de milhares de eventos por segundo, eu multiplico a frota: cada cópia de carro ganha um número próprio e começa num ponto diferente da corrida.
[se der tempo] 100 mil eventos por segundo são mil carros.

[etapa 2]
O gerador publica em malha aberta, até 400 mil eventos por segundo.
Cada evento é um registro binário de 46 bytes, sempre do mesmo tamanho. Isso importa porque é uma mensagem pequena, como a de um sensor de IoT: o custo fica quase todo no transporte, e não no processamento. Isso ajuda a explicar os resultados.
[se der tempo] Como o tamanho é fixo, o Pipelines lê um registro atrás do outro sem procurar separador. E é com esses 46 bytes que eu igualo as filas internas: 10 mil eventos no Channels são os mesmos 460 kB do Pipe.

[etapa 3]
No broker, a carga é dividida em quatro faixas. No Kafka são quatro partições; no RabbitMQ, quatro filas.
Cada carro vai sempre para a mesma faixa. Quem decide a faixa é o CRC32: uma conta simples que transforma o número do carro num número fixo. O mesmo carro dá sempre o mesmo resultado, e esse resultado aponta a faixa.
[se der tempo] Usei a mesma conta nos dois brokers, então a divisão é igual nos dois.
E cada faixa tem um leitor só. Assim, os eventos de um carro são lidos na ordem em que aconteceram. Isso importa porque, para achar uma frenagem, eu comparo um evento com o anterior do mesmo carro.
[se der tempo] O paralelismo vem de ter quatro faixas lendo ao mesmo tempo.

[etapa 4 — aponte para o quadro de baixo]
No consumidor estão os três modos. No Direct, o callback processa ele mesmo. No Channels e no Pipelines, ele entrega a um leitor numa thread de trabalho e volta a receber.
A única diferença entre as três linhas é essa entrega. Por isso o Direct é o grupo de controle: mesmo broker, mesma carga e mesmo código, só que sem o mecanismo.
[se der tempo] Então a diferença entre Channels e Direct é o efeito do Channels, e de nada mais.
E o processamento é o mesmo nos três modos. Ele faz duas coisas.
Primeiro, junta os eventos de cada carro a cada segundo e resume: velocidade média, velocidade máxima, quantas frenagens e quantas trocas de marcha.
Segundo, compara cada evento com o anterior do mesmo carro. Se o freio passou de 50%, conta uma frenagem. Se a marcha mudou, conta uma troca de marcha.
[se der tempo] Escolhi essas duas operações porque são as que os benchmarks clássicos de fluxo de eventos usam, como o Linear Road e o DEBS Grand Challenge. Assim o trabalho do consumidor não é inventado.

[etapa 5]
E cada rodada gera um digest, uma impressão digital do resultado. Como todas as combinações processam a mesma entrada, todas têm de dar o mesmo digest. Se não derem, houve perda, duplicação ou quebra de ordem.""",

# 7 --- ambiente e medição
"""[0:25]

Tudo roda numa única máquina, em Docker.
[se der tempo] Um Core i5 de 12 threads, com 16 GB.

[aponte para os quadradinhos]
Cada componente tem núcleos exclusivos. Ninguém disputa CPU com ninguém.
[se der tempo] O gerador fica do 0 ao 3, o broker do 4 ao 7, o consumidor do 8 ao 10, e banco e métricas no 11. Durante a medição, só o broker medido fica ligado, e eu não uso o computador.

[caixa da direita]
A latência vai do envio ao fim do processamento. O gerador grava na mensagem o instante da publicação, e o consumidor compara com o mesmo relógio. Como tudo está na mesma máquina, não preciso sincronizar relógios.
CPU e memória vêm do Prometheus com o cAdvisor, por container.
[se der tempo] As latências vão para um HdrHistogram; alocação e coleta de lixo vêm dos contadores do .NET.
[se der tempo] E os resultados vão para o PostgreSQL em lote. A latência termina antes da gravação, então o banco não entra na medida e não é gargalo.""",

# 8 --- metodologia
"""[1:20]

[caixas de cima]
Usei dois cenários de configuração.
No cenário padrão, os valores de fábrica, conferidos no código-fonte de cada versão. Só mudei o que impedia a equivalência entre os dois.
Uma diferença ficou de propósito, porque é o padrão de cada um: o RabbitMQ, com mensagem persistente, grava em disco antes de confirmar. O Kafka confirma com a mensagem no cache do sistema, como a documentação dele recomenda. Isso favorece o Kafka, e está declarado.
O cenário ajustado aplica a recomendação de cada fornecedor, e com isso a garantia fica igual nos dois.
[se der tempo] No Kafka, mudei o linger.ms. É quanto tempo o gerador espera juntando mensagens antes de enviar o lote, como um ônibus que espera encher. O padrão é 5 ms; testei 1 ms, para latência, e 20 ms, para volume.
[se der tempo] No RabbitMQ, a mensagem passou a ser transitória: o broker confirma sem esperar gravar no disco, igual ao Kafka. E limitei a 300 as mensagens que ele manda ao consumidor sem confirmação. Isso é o prefetch.

[números do meio]
[se der tempo] São seis combinações, dois brokers vezes três modos. Cada combinação e cada carga tem 10 repetições, de 90 segundos de medição, em ordem sorteada, até 400 mil eventos por segundo.

[caixa de baixo]
E as regras foram registradas antes da coleta.
Uma rodada é válida se o gerador não atrasou mais de 50 ms e se o digest bate.
O ponto de saturação é a maior carga com P99 abaixo de 50 ms.
E uma diferença entre modos só conta se passar de 20% no P99 ou de 10% no throughput.
Além disso, uso o teste de Kruskal-Wallis. É um teste estatístico que diz se a diferença entre os três modos é real ou pode ser só sorte. Mas ele vem depois: primeiro a diferença precisa ser grande o bastante para importar na prática.""",

# 9 --- comparação justa
"""[1:35]

Este slide responde a uma pergunta importante: como garanti que a comparação foi justa. São quatro frentes.

[cartão 1]
Primeiro, a mesma carga e o mesmo trabalho. Os mesmos dados e o mesmo código de processamento nos três modos. A mesma divisão em faixas nos dois brokers. E a fila interna do Channels e a do Pipelines com o mesmo volume: 10 mil eventos de 46 bytes são 460 kB. O digest prova que todos fizeram o mesmo trabalho.

[cartão 2]
Segundo, a mesma garantia de entrega nos dois. Chama at-least-once, que quer dizer "pelo menos uma vez": nenhuma mensagem se perde, mas alguma pode chegar repetida.
Funciona como uma carta registrada. O broker avisa o gerador que recebeu, e o consumidor só avisa o broker depois de processar. Se um aviso falhar, a mensagem é enviada de novo.
[se der tempo] No Kafka, essa confirmação é o acks igual a all; no RabbitMQ, são os publisher confirms. Os valores de fábrica foram conferidos na fonte, para que nenhum broker fosse ajustado a favor.
[se der tempo] E desliguei a compressão: as réplicas da frota são quase iguais, e comprimir daria uma vantagem artificial ao Kafka.

[cartão 3]
[se der tempo] Terceiro, as mesmas condições de execução: núcleos exclusivos, só o broker medido ligado, ordem das rodadas sorteada e o gerador em malha aberta.

[cartão 4]
E quarto, uma análise sem viés. As regras e o limite de relevância foram fixados antes de eu ver os dados, como recomendam Jain e Fu. O Direct como grupo de controle permite dizer se o mecanismo ajudou ou atrapalhou: sem ele, eu só saberia qual mecanismo é melhor, não se vale a pena usar algum.
[se der tempo] E dez repetições, com a regra de Tukey para tirar os discrepantes e a mediana para resumir.

[rodapé]
Cada decisão tem uma base: a documentação do fornecedor, a literatura ou uma medição da calibração. A lista completa está num slide de apoio, no final.""",

# 10 --- calibração
"""[1:05]

Antes da coleta oficial, fiz uma calibração: medições de teste para acertar a bancada. Ela mostrou que o ambiente pesava mais do que as próprias arquiteturas.

[da esquerda para a direita]
Com cota de CPU, o broker RabbitMQ era estrangulado. Com núcleos exclusivos, o P99 dele caiu de 40 a 64%. Então passei a usar núcleos exclusivos.
Com gerador e consumidor fora dos containers, o Kafka tinha latência com dois picos em 43% das rodadas de carga baixa. Coloquei tudo em containers.
Às vezes a máquina virtual parava um instante esperando o disco. Logo depois dessas pausas, a chance de uma latência alta ficava 126 vezes maior.
Isso não tem nada a ver com a arquitetura. Então fiz duas coisas. Se o gerador atrasa mais de 50 ms, a rodada é inválida. E cada teste é repetido dez vezes, para que uma pausa azarada não decida o resultado.
[se der tempo] E uma auditoria contra os valores de fábrica achou três defeitos de configuração e três assimetrias entre os brokers. Corrigi antes da coleta.

[rodapé]
[se der tempo] Também descartei o linger.ms zero: com ele, a mediana do Kafka passou de 400 ms já a 10 mil eventos por segundo.
Esses dados da calibração não entram na análise. Serviram só para consertar a bancada.""",

# 11 --- validade das rodadas
"""[0:40]

Antes dos resultados, a validade das rodadas.

[da esquerda para a direita]
Foram 390 rodadas no cenário padrão.
Zero divergências de digest. Ou seja: nenhum evento se perdeu, duplicou ou saiu de ordem, em nenhuma combinação.
Doze inválidas, todas do Kafka a 200 mil eventos por segundo ou mais, todas porque o gerador atrasou o envio. Isso coincide com as pausas de disco da máquina virtual que a calibração mostrou.
[se der tempo] E a regra de Tukey tirou cerca de 8% das rodadas, como discrepantes.

[rodapé]
Como os pontos a 100 Hz são interpolados, repeti o Direct com os dados originais, a 3,7 Hz, sem interpolação. O P99 mudou menos de 10%, e as conclusões não mudam.""",

# 12 --- latência e throughput
"""[1:45]

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
Agora reparem nas três linhas de cada cor. Elas ficam uma em cima da outra.
Isso quer dizer que colocar Channels ou Pipelines não deixou nada mais rápido. Já é a primeira resposta do trabalho, do lado da latência.
[se der tempo] A maior diferença foi de 11% no P99, abaixo do limite de 20% que eu tinha fixado.

[terceira mensagem — saturação; aponte para o gráfico de throughput]
O throughput é igual em todas as combinações, porque o gerador é de malha aberta: enquanto aguenta, cada uma processa exatamente o que chega. O que separa as combinações é até onde cada uma aguenta.
O RabbitMQ sustenta 60 mil e falha a 80 mil, com P99 acima de 50 ms.
O Kafka sustenta 400 mil, a maior carga que testei. O ponto de saturação dele fica acima do alcance da bancada.
E o modo não mudou o ponto de saturação em nenhum dos dois.""",

# 13 --- uso de recursos
"""[1:05]

Nos recursos, os quatro gráficos: CPU e memória, do consumidor e do broker.

[primeira mensagem; aponte para o gráfico de cima à esquerda]
A CPU do consumidor é a única medida em que os modos se separam, e para pior.
No Kafka, Channels e Pipelines custam de 1,9 a 3,3 vezes a CPU do Direct. São de 2,4 a 3,7 microssegundos a mais por evento, mais ou menos o custo de acordar uma thread.
Para comparar: o processamento em si leva 0,12 microssegundo. Passar o evento para outra thread custa de 20 a 30 vezes o próprio trabalho.
[se der tempo] No RabbitMQ, o acréscimo é pequeno, de 4 a 9%.

[segunda mensagem; gráfico de cima à direita]
A CPU do broker explica a saturação. O RabbitMQ ocupa até 3,5 dos 4 núcleos. O Kafka fica em meio núcleo.
[se der tempo] O RabbitMQ trata cada mensagem sozinha: roteia, confirma e grava. O Kafka trabalha em lotes.

[terceira mensagem; gráficos de baixo]
Na memória, o modo não muda nada: o consumidor fica perto de 100 MB em todos.
Entre os brokers, o Kafka usa mais memória, perto de 1,5 GB, contra uns 0,3 do RabbitMQ. É a troca dele: gasta menos CPU e mais memória.
[se der tempo] Um detalhe: o Pipelines promete criar menos objetos, mas aqui isso não apareceu. Quem cria a maior parte dos objetos é a biblioteca do broker, antes de o evento chegar ao mecanismo.""",

# 14 --- cenário ajustado
"""[1:20]

Agora o cenário ajustado, com a recomendação de cada fornecedor. O gráfico compara o modo Direct no padrão e no ajustado.

[primeira mensagem — RabbitMQ]
No RabbitMQ, a principal mudança foi parar de gravar cada mensagem no disco antes de confirmar. É o que o Kafka já fazia desde o começo, então agora os dois têm a mesma garantia.
O resultado foi grande: a latência caiu quase pela metade, ou mais, em todas as cargas. E ele passou a aguentar 80 mil eventos por segundo, em vez de 60.
O motivo é simples: sem esperar o disco, sobra CPU no broker para entregar mais rápido.
[se der tempo] Os números: o P99 caiu de 43 a 76%. A 10 mil, de 2,3 para 0,65 ms. E também limitei o prefetch a 300 mensagens, como o fornecedor recomenda.

[segunda mensagem — Kafka]
No Kafka, é uma troca.
Com o linger.ms de 1, a mediana caiu de 3 para 1 ms em todas as cargas. Isso confirma que o piso vinha da formação de lotes. Mas, com lotes menores, o broker recebe mais requisições: a CPU dele dobrou, e o P99 subiu nas cargas altas.
[se der tempo] O perfil para vazão, com 20 ms, faz o contrário: P99 perto de 21 ms e 30% menos CPU.

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
"""[1:30]

Voltando às duas perguntas.

[faixa rosada de cima]
A primeira: o mecanismo interno ajuda? Nesta carga, Channels e Pipelines não fizeram diferença no desempenho e pioraram o custo.
O motivo é que o processamento leva 0,12 microssegundo por evento. O consumidor termina um evento muito antes de o broker entregar o próximo. Quem limita é o broker. Não há trabalho acumulado para o mecanismo paralelizar.

[três colunas]
A segunda: qual broker? O RabbitMQ para carga baixa, com latência mínima. O Kafka para carga alta, com menos CPU.
[se der tempo] A configuração pesa muito: um único parâmetro mudou o P99 em até 76%. Uma comparação entre brokers precisa de configurações auditadas.
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
