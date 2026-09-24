# TCC2 — Perguntas para o orientador

24/09/2026 · Pedro Augusto Ramos de Sousa

São 9 perguntas. As seis primeiras precisam de resposta antes da próxima rodada grande de testes. Cada uma explica o assunto em poucas linhas, faz a pergunta e diz o que eu proponho.

Versão compartilhável: https://claude.ai/code/artifact/61e96e12-c912-489c-a0ef-71520622dae5 · código: https://github.com/ramosnvy/pitwall

## Em poucas palavras

O trabalho compara seis combinações: 2 sistemas de mensagens (Kafka e RabbitMQ) × 3 jeitos de processar as mensagens no programa que as recebe (Direct, Channels e Pipelines). O Direct é o jeito mais simples e serve de referência.

- **Os dados:** telemetria real de corridas de F1, como velocidade, marcha e freio. Um programa envia esses dados num ritmo fixo, por exemplo 40 mil mensagens por segundo.
- **O processamento:** quem recebe calcula médias por carro a cada segundo, conta frenagens e trocas de marcha, e salva no banco de dados.
- **O que eu meço:** quanto tempo cada mensagem leva do envio até o fim do processamento. Olho principalmente para as mensagens mais lentas (o 1% mais lento), porque são elas que mostram quando o sistema começa a engasgar.
- **Onde roda:** tudo no mesmo computador, cada parte no seu próprio contêiner Docker.

Já fiz uma primeira rodada completa de testes e alguns testes menores. As perguntas vieram desses resultados.

## Antes da próxima rodada de testes

### 1. Posso completar os dados para 100 leituras por segundo?

**O assunto.** A fonte pública de dados da F1 (OpenF1) dá cerca de 4 leituras por segundo de cada carro. Um carro de verdade mede até 100 vezes por segundo. Para chegar a 100, preencho os espaços entre as leituras reais: a velocidade sobe ou desce em linha reta entre dois pontos, e o freio e a marcha repetem o último valor. As frenagens e trocas de marcha continuam exatamente as da corrida real.

**Pergunta.** Posso usar esses dados completados nos testes principais, deixando claro no texto que parte dos pontos foi calculada?

**Minha proposta.** Sim, e faço também um teste menor com os dados originais, sem preenchimento, para comparar.

### 2. Posso mudar como o computador é dividido entre as partes do teste?

**O assunto.** Nos primeiros testes, cada parte (quem envia, o sistema de mensagens e quem recebe) tinha um limite de uso do processador, mas todas disputavam os mesmos núcleos. Testei outra forma: cada parte com seus próprios núcleos, sem dividir com ninguém. Assim, as mensagens mais lentas do RabbitMQ ficaram de 40% a 64% mais rápidas. Parte da lentidão que eu atribuía ao RabbitMQ vinha da divisão do computador.

**Pergunta.** Posso usar núcleos separados daqui para frente, e contar no texto os primeiros testes como o motivo da mudança?

**Minha proposta.** Sim. Os resultados que valem passam a ser os da nova rodada.

### 3. Os dois sistemas devem ser configurados igual, ou cada um do seu melhor jeito?

**O assunto.** Nos dois sistemas, as mensagens são divididas em 4 filas, e cada carro vai sempre para a mesma fila. No Kafka essa divisão é automática. No RabbitMQ sou eu que faço, e passei a usar a mesma regra do Kafka para os dois ficarem iguais. Só que, com essa regra, o RabbitMQ ficou mais lento do que com a regra que eu usava antes.

**Pergunta.** Na comparação, uso a mesma regra nos dois, que é mais justo, ou a melhor regra para cada um, que é mais realista?

**Minha proposta.** A mesma regra nos testes principais, e um teste extra com a melhor configuração de cada um.

### 4. Posso testar cada sistema até um volume diferente?

**O assunto.** O RabbitMQ começa a travar entre 50 e 80 mil mensagens por segundo. O Kafka aguenta até 200 ou 300 mil. Testar o RabbitMQ acima do limite dele só acumula fila e gasta tempo de máquina.

**Pergunta.** Posso testar o RabbitMQ até 60 mil e o Kafka até 300 mil mensagens por segundo?

**Minha proposta.** Sim. Até 60 mil os dois são comparados lado a lado. Acima disso, compara-se até onde cada um aguenta (pergunta 5).

### 5. "Até onde cada um aguenta" pode ser o resultado principal?

**O assunto.** O TCC1 fala em medir tempo de resposta, volume e uso do computador, mas não diz como comparar sistemas que aguentam volumes muito diferentes. Uma resposta simples é medir o volume máximo que cada combinação aguenta sem travar. Para isso, preciso definir o que é "travar" antes de ver os resultados, e não depois.

**Pergunta.** Esse limite pode ser a medida principal do trabalho? Como o senhor definiria "travar"?

**Minha proposta.** O sistema aguenta enquanto entrega pelo menos 99% das mensagens e 99% delas chegam em menos de 50 milissegundos. Os 50 ms vêm da própria F1: a equipe toma decisões com dados de menos de 50 ms.

### 6. Posso descartar testes que deram errado?

**O assunto.** Um teste só vale se duas coisas deram certo: o envio seguiu o ritmo combinado, com atraso de no máximo 50 ms, e o resultado do cálculo bateu com o esperado. Nos últimos testes, 35 de 40 passaram nessa checagem.

**Pergunta.** Posso descartar os que falharem, informando no texto quantos foram e por quê?

**Minha proposta.** Sim, com uma tabela de descartes junto dos resultados.

## Escopo e análise

### 7. Posso estudar mais a fundo Channels e Pipelines?

**O assunto.** Hoje uso Channels e Pipelines do jeito padrão, e o cálculo que faço é muito leve. Nesse caso, eles não deixam nada mais rápido e só gastam mais processador. Falta descobrir em que situação eles ajudam: com um cálculo mais pesado, ou ajustando como agrupam e distribuem o trabalho.

**Pergunta.** Posso incluir uma segunda parte nos resultados, testando esses ajustes?

**Minha proposta.** Sim, com um limite: só sigo com o ajuste que mostrar diferença num teste rápido. O resultado vira um guia de quando vale usar cada um.

### 8. Incluo uma terceira opção de RabbitMQ?

**O assunto.** Kafka e RabbitMQ guardam as mensagens de jeitos diferentes. O Kafka mantém tudo numa lista contínua, e o RabbitMQ apaga cada mensagem depois de entregue. O RabbitMQ tem um modo chamado Streams, que guarda como o Kafka. Testá-lo mostraria se a diferença vem do sistema ou desse jeito de guardar.

**Pergunta.** Incluo o RabbitMQ Streams, ou deixo para trabalhos futuros?

**Minha proposta.** Trabalhos futuros, porque aumenta os testes em 50%.

### 9. Que tipo de análise estatística o senhor espera?

**O assunto.** Os tempos medidos não seguem a curva normal: a maioria é rápida e alguns são muito lentos. Por isso uso um teste que não depende da curva normal (Kruskal-Wallis). Ele diz se as diferenças são reais, mas não quanto cada fator pesa. O TCC1 citava o método de Jain, que mede quanto do resultado vem do sistema de mensagens, do jeito de processar e do volume.

**Pergunta.** Qual análise o senhor espera?

**Minha proposta.** As duas: uma para confirmar que as diferenças são reais, e outra para dizer quanto cada fator influencia.

## Confirmações rápidas

Decisões que já estão em uso. Basta um sim, ou dizer o que mudar.

| # | Assunto | Como está |
| --- | --- | --- |
| 1 | Referência | Incluí o Direct, que processa sem Channels nem Pipelines, para ter com o que comparar |
| 2 | Volume alto | Crio carros extras a partir dos reais, cada um com número próprio e enviando em momentos um pouco diferentes |
| 3 | Formato da mensagem | Binário e pequeno (46 bytes). Em texto (JSON), só ler a mensagem já custaria mais que o resto |
| 4 | Compactação | Desligada nos dois. Os carros copiados têm dados quase iguais e isso favoreceria o Kafka |
| 5 | Cálculo | O mesmo nos três jeitos: médias por segundo, frenagens e trocas de marcha |
| 6 | Leitores em paralelo | 4 filas e o mesmo número de leitores nos dois sistemas |
| 7 | Garantia de entrega | Nos dois, nenhuma mensagem se perde; numa falha, uma mensagem pode chegar repetida |
| 8 | Testes independentes | Filas e tabelas são recriadas antes de cada teste |

## Proposta do segundo cenário (a enviar)

Resposta à dúvida sobre o que se ganha com Channels e Pipelines. Desenho completo em APROFUNDAMENTO §8.

> Professor, uma proposta para responder à sua dúvida sobre o que se ganha com Channels e Pipelines.
>
> Hoje o teste usa um só tipo de dado da F1 e um cálculo muito leve. Nesse cenário o tempo está quase todo no Kafka e no RabbitMQ, e os mecanismos do .NET não fazem diferença, como o senhor apontou. Vou mostrar isso com números, mas sozinho é uma conclusão pequena.
>
> A ideia é criar um segundo cenário, mais parecido com o que uma equipe usa de verdade: vários tipos de dados da própria API (telemetria, posição na pista, clima, bandeiras e paradas nos boxes), cada um no seu próprio canal, e um processamento em etapas que cruza essas informações, por exemplo em qual curva o carro freou e a diferença de tempo entre os carros. Nos dois sistemas de mensagens, cada tipo de dado vai para o seu canal usando o recurso próprio de cada um, sem ajuste para favorecer nenhum dos dois.
>
> No fim do processamento, o resultado (alertas e dados enriquecidos) seria publicado num segundo canal, lido por um programa que faz o papel do painel da equipe. É um padrão comum em sistemas reais: consumir, processar e publicar de novo. É justamente nessa espera pela confirmação do broker que Channels e Pipelines podem ajudar, porque deixam o programa continuar recebendo enquanto espera. A medida passa a ser o tempo do sensor até o alerta.
>
> Eu construiria em dois passos. Primeiro só a republicação, sobre os dados que já uso, o que é pouco código e já mostra se Channels e Pipelines fazem diferença. Depois, se fizer sentido, os vários tipos de dados.
>
> O primeiro cenário continua como está e roda antes. O segundo eu só construiria com o seu aval, porque é um trabalho grande. Faz sentido?

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
