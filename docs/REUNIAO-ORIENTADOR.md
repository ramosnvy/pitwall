# TCC2 — Perguntas para o orientador

24/09/2026 · Pedro Augusto Ramos de Sousa

São 16 perguntas, em ordem de urgência. As sete primeiras travam a próxima bateria de experimentos (a matriz v3), que só roda depois das respostas. Cada uma traz o contexto, a pergunta e a minha proposta.

Versão compartilhável: https://claude.ai/code/artifact/61e96e12-c912-489c-a0ef-71520622dae5 · repositório: https://github.com/ramosnvy/pitwall

## Onde o trabalho está

O experimento compara seis arquiteturas: 2 brokers (Kafka e RabbitMQ) × 3 formas de processar dentro do consumidor (Direct, Channels e Pipelines). O Direct é o grupo de controle: processa a mensagem direto, sem mecanismo interno.

- **Carga:** um gerador reproduz a telemetria real da F1 (OpenF1) numa taxa fixa de eventos por segundo (ev/s), multiplicando os carros para chegar a cargas altas. Cada evento vira uma mensagem binária de 46 bytes.
- **Processamento:** o consumidor calcula, por carro, média e máxima de velocidade em janelas de 1 s, detecta frenagens e trocas de marcha, e grava no PostgreSQL.
- **Medição:** latência do envio até o fim do processamento. O P99 é o tempo abaixo do qual ficam 99% dos eventos. Cada rodada tem 10 s de aquecimento e 90 s de medição.
- **Ambiente:** tudo em Docker, numa máquina com 12 núcleos.

Já rodei uma matriz completa, um experimento de calibração (2×2) e uma varredura de saturação. As perguntas abaixo nasceram desses resultados.

## Antes da matriz

### 1. Posso usar telemetria a 100 Hz, com pontos interpolados?

**Contexto.** A OpenF1 fornece cerca de 3,7 amostras por segundo por carro, e um carro real de F1 amostra seus sensores a até 100 Hz. Para chegar a 100 Hz, crio os pontos intermediários: velocidade, rotação e acelerador em linha reta; marcha, freio e DRS mantêm o último valor. As frenagens e trocas de marcha detectadas continuam exatamente as da corrida real, o que está verificado em teste.

Outra vantagem: 100 mil ev/s passam a vir de 1.000 carros, e não de 27 mil.

**Pergunta.** Posso usar 100 Hz interpolados como cenário principal?

**Minha proposta.** Sim, com uma bateria a 3,7 Hz, o dado sem interpolação, como análise de sensibilidade. A interpolação fica declarada nas ameaças à validade.

**Se for outra.** Com 3,7 Hz, o banco grava 27 vezes mais janelas na mesma carga e vira o gargalo nas cargas altas, mascarando a comparação dos brokers.

### 2. Posso trocar o protocolo de CPU e usar a primeira matriz como calibração?

**Contexto.** Na primeira matriz, cada container tinha uma cota de CPU (tempo equivalente a 4 núcleos), mas podia rodar em qualquer núcleo da máquina. Fiz um experimento 2×2 comparando isso com núcleos exclusivos: gerador nos núcleos 0 a 3, broker de 4 a 7, consumidor de 8 a 10, banco e métricas no 11.

Com núcleos exclusivos, o P99 do RabbitMQ caiu de 40% a 64%. A 40 mil ev/s, por exemplo, foi de 10,2 para 6,1 ms. Parte da lentidão atribuída ao RabbitMQ era efeito da cota, não do broker.

**Pergunta.** Posso adotar núcleos exclusivos e apresentar a primeira matriz e o 2×2 na metodologia, como a calibração que justificou o protocolo?

**Minha proposta.** Sim. Os resultados oficiais passam a ser os da matriz v3.

### 3. Os dois brokers devem usar a mesma configuração, ou cada um a sua melhor?

**Contexto.** Os dois brokers dividem as mensagens em 4 filas paralelas, sempre a mesma fila para o mesmo carro, para manter a ordem por carro. O Kafka escolhe a fila (partição) por um hash CRC32 do número do carro. No RabbitMQ a escolha é feita no meu código, e passei a usar o mesmo CRC32 para os dois ficarem iguais.

O resultado foi inesperado. Com a divisão equilibrada do CRC32, o RabbitMQ gasta mais CPU e tem P99 maior do que com a divisão antiga (resto da divisão do número do carro por 4), que é desequilibrada. A 40 mil ev/s, o P99 vai de 6,1 para 11,0 ms.

**Pergunta.** A comparação deve usar a mesma configuração nos dois brokers, ou a melhor configuração de cada um?

**Minha proposta.** Mesma configuração na matriz principal, porque isola o efeito do broker. A melhor configuração de cada um entra numa bateria complementar.

### 4. Posso testar cada broker na sua própria faixa de carga?

**Contexto.** Na varredura de saturação, com 100 Hz e núcleos exclusivos, o RabbitMQ sustentou de 50 a 80 mil ev/s, conforme o modo, e o Kafka de 200 a 300 mil. Testar os dois nas mesmas cargas desperdiça rodadas: acima da saturação, o RabbitMQ só acumula fila.

**Pergunta.** Posso usar faixas de carga diferentes para cada broker?

**Minha proposta.** Kafka em 10, 20, 40, 60, 100, 200 e 300 mil ev/s; RabbitMQ em 10, 20, 40 e 60 mil. As cargas até 60 mil são comuns aos dois e permitem comparação direta. Acima disso, a comparação é pelo ponto de saturação (pergunta 6).

### 5. Cinco repetições por combinação são suficientes?

**Contexto.** Cada combinação de broker, modo e carga é repetida, em ordem sorteada, para medir a variação entre rodadas. Com 5 repetições, a matriz tem 165 rodadas, mais 20 da bateria a 3,7 Hz feita só com o Direct: 185 no total. A matriz roda de madrugada, sem uso da máquina.

**Pergunta.** Cinco repetições bastam, ou o senhor prefere 10?

**Minha proposta.** Cinco, e aumentar só nas combinações em que a dispersão entre repetições for alta.

### 6. O ponto de saturação pode ser a métrica principal?

**Contexto.** O TCC1 lista latência, vazão e uso de recursos, mas não define como comparar arquiteturas que saturam em cargas diferentes. O ponto de saturação é a maior carga que a arquitetura sustenta, e é o número que resume a comparação. Ele precisa de um critério fixado antes de medir, para não ser escolhido depois de ver os dados.

**Pergunta.** O ponto de saturação pode ser a métrica principal? Que critério o senhor considera adequado?

**Minha proposta.** A maior carga em que a vazão entregue fica em pelo menos 99% da oferecida e o P99 abaixo de 50 ms. Os 50 ms vêm do domínio: decisões de pit wall usam dados com menos de 50 ms de idade.

### 7. Posso descartar rodadas inválidas?

**Contexto.** Uma rodada só vale se o gerador enviou no ritmo certo e o resultado está correto. Os critérios são dois:
- o atraso máximo de envio fica em até 50 ms;
- o resumo (digest) dos resultados é igual ao esperado, porque todas as arquiteturas precisam calcular exatamente o mesmo.

No 2×2, 35 de 40 rodadas passaram.

**Pergunta.** Posso descartar rodadas por esses critérios, registrando quantas e por quê?

**Minha proposta.** Sim, publicando o número de descartes por combinação junto com os resultados.

## Escopo

### 8. Posso aprofundar como Channels e Pipelines são usados?

**Contexto.** Hoje Channels e Pipelines são usados na configuração padrão, e o processamento é barato: uma thread processa 8,6 milhões de eventos por segundo. Nesse cenário, os mecanismos internos não melhoram a latência e gastam mais CPU, cerca de 3,6 µs por evento, a ordem de grandeza de acordar uma thread.

O trabalho ainda não responde quando eles compensam: com processamento mais pesado, lotes maiores ou mais workers.

**Pergunta.** Posso incluir uma segunda parte nos resultados, "uso ajustado", testando essas configurações?

**Minha proposta.** Sim, sem bibliotecas novas e com uma regra de parada: só entra na matriz o fator que mostrar efeito num teste rápido. A conclusão vira uma tabela de quando usar cada mecanismo.

**Se for outra.** O trabalho conclui só sobre o uso padrão.

### 9. RabbitMQ Streams entra no trabalho?

**Contexto.** O Kafka guarda as mensagens num log: quem lê só avança um marcador. A fila clássica do RabbitMQ apaga cada mensagem quando o consumidor confirma. O RabbitMQ também oferece o Streams, um log parecido com o do Kafka. Incluí-lo separaria o efeito do broker do efeito do modelo de armazenamento (log ou fila).

**Pergunta.** O Streams entra no trabalho ou fica como trabalho futuro?

**Minha proposta.** Trabalho futuro. Ele aumentaria a matriz em 50%.

### 10. O tamanho da mensagem entra como fator?

**Contexto.** Todas as mensagens têm 46 bytes. Com mensagens maiores, o custo de rede e disco pesa mais, e a vantagem de um broker sobre o outro pode mudar.

**Pergunta.** Testo também mensagens de cerca de 1 KB?

**Minha proposta.** Não na matriz principal, que dobraria. Se entrar, só com o Direct e em duas cargas.

### 11. Quais destes extras valem a pena?

**Contexto.** Todos fortalecem o trabalho, mas custam tempo de execução:
- rodadas longas, de 10 a 15 minutos, para ver efeitos que só aparecem com o tempo, como a limpeza de memória e a compactação do log;
- uma bateria sem gravar no banco, para isolar o custo da persistência;
- mais corridas no workload: baixei 9 e uso 1.

**Pergunta.** Quais desses entram?

**Minha proposta.** Rodadas longas numa só carga por arquitetura; os outros dois como sensibilidade pequena.

## Análise e validade

### 12. Que análise estatística o senhor espera?

**Contexto.** As latências não seguem distribuição normal, porque têm cauda longa. Hoje comparo as arquiteturas com Kruskal-Wallis, que não assume normalidade. O plano do TCC1 citava o método do Jain: um projeto fatorial com ANOVA, que mede quanto da variação vem de cada fator (broker, modo e carga) e das interações entre eles.

**Pergunta.** Qual análise o senhor espera?

**Minha proposta.** As duas. Kruskal-Wallis para dizer se as diferenças são reais, e o fatorial do Jain sobre o logaritmo da latência para dizer quanto cada fator pesa.

### 13. Posso declarar como limitação que tudo roda numa máquina só?

**Contexto.** Gerador, broker, consumidor e banco rodam na mesma máquina, sem rede entre eles, com um só nó de cada broker. O lado bom: gerador e consumidor usam o mesmo relógio, então não há erro de sincronização na medição da latência. O lado ruim: o que justifica o Kafka em produção, como replicação e tolerância a falhas, não aparece.

**Pergunta.** Posso declarar isso nas ameaças à validade e manter o escopo de nó único?

**Minha proposta.** Sim. Com máquinas separadas, o erro de sincronização dos relógios seria maior que as diferenças que estou medindo.

### 14. Como reporto resultados instáveis?

**Contexto.** O Kafka com Channels teve rodadas muito diferentes entre si. A 100 mil ev/s, uma rodada deu P99 de 200 ms e as duas seguintes, 5,9 ms. A 200 mil, deu 78 ms e 20 ms.

**Pergunta.** Mostro a dispersão como resultado, ou investigo a causa antes?

**Minha proposta.** Investigar antes da matriz, o que já está no plano. Se não houver causa no meu código, reportar a dispersão com todas as rodadas.

## Texto e prazos

### 15. Posso atualizar a Figura 1 e os objetivos específicos do TCC1?

**Contexto.** A Figura 1 do TCC1 tem cinco módulos: API OpenF1, Ingestão, Mensageria, Processamento e Persistência. Desde então entraram o grupo de controle Direct, o gerador de carga, os 100 Hz, os núcleos exclusivos e o ponto de saturação.

**Pergunta.** Posso atualizar a figura e os objetivos específicos?

**Minha proposta.** Manter o formato da Figura 1, com as cinco etapas lado a lado, e mostrar dentro de cada uma os passos atuais. O desenho já está pronto.

### 16. Formato, prazo e banca

**Contexto.** O TCC1 foi escrito no modelo de artigo da SBC. O cronograma do TCC1 terminava em setembro e precisa ser refeito.

**Perguntas.**
- O TCC2 continua como artigo no modelo SBC?
- Qual a data prevista da defesa?
- Quem compõe a banca?
- Há coorientador?

## Confirmações rápidas

Decisões já implementadas. Basta um sim, ou a indicação do que mudar.

| # | Decisão | Como está |
| --- | --- | --- |
| 1 | Grupo de controle | Direct incluído: matriz 2 brokers × 3 modos |
| 2 | Carga alta | Multiplicação de carros, cada réplica com número próprio e defasagem para não enviarem todas juntas |
| 3 | Formato da mensagem | Binário, 46 bytes. Com JSON, o tempo de leitura dominaria a medição |
| 4 | Compressão | Desligada nos dois. As réplicas são quase idênticas e inflariam o Kafka |
| 5 | Processamento | Janela de 1 s por carro, frenagens e trocas de marcha; idêntico nos 3 modos |
| 6 | Paralelismo | 4 filas ou partições e uma linha de consumo por faixa nos dois brokers |
| 7 | Garantia de entrega | At-least-once nos dois: `acks=all` no Kafka e publisher confirms no RabbitMQ |
| 8 | Isolamento entre rodadas | Tópicos e filas recriados e tabelas limpas a cada rodada |

## Literatura para o texto

O TCC1 cita apenas comparações entre brokers. A literatura de benchmarks de stream processing fundamenta a escolha das operações e do protocolo, e responde antecipadamente a "por que essas operações?".

| Fonte | O que sustenta |
| --- | --- |
| [DEBS 2013 Grand Challenge](https://debs.org/grand-challenges/2013/) | Telemetria esportiva a 200 Hz e 2000 Hz como carga de referência; avalia vazão, latência **e correção** |
| [Linear Road](https://www.researchgate.net/publication/2949008_Linear_Road_A_Stream_Data_Management_Benchmark) | Agregação em janela agrupada e detecção de evento sobre telemetria veicular |
| [RIoTBench](https://arxiv.org/abs/1701.08530) | Mede latência, throughput, CPU, memória e *jitter* em cargas de IoT |
| [ESPBench](https://arxiv.org/pdf/2103.06775) e [survey TPCTC 2024](https://hpi.de/fileadmin/user_upload/fachgebiete/rabl/publications/2024/streamsurvey_tpctc_2024.pdf) | Taxonomia das operações de referência |

## Pendências do documento do TCC1

- Nome do orientador truncado ("Prof. Dr. Evandro K.") e campo de coorientador vazio.
- Membros da banca ainda com o texto de exemplo do modelo.
- Legendas saem como "Figure" e "Table" no template SBC.
- Datas de "Acesso em: maio 2025" nas referências precisam ser atualizadas.
