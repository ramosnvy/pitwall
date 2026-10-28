# Estado do projeto

Retrato de 28/10/2026, depois de todas as medições e com o texto completo: matriz v3 do cenário padrão (tag `cenario-padrao-v1.2`), matriz ajustada (`cenario-ajustado-v1.1`) e matriz do segundo cenário (`cenario-republicacao-v1.1`). Decisões em [PLANO.md](PLANO.md); números em [RESULTADOS.md](RESULTADOS.md); achados de implementação em [IMPLEMENTACAO.md](IMPLEMENTACAO.md); configurações em [AUDITORIA-CONFIG.md](AUDITORIA-CONFIG.md).

## 1. Em que momento estamos

**Medições encerradas e texto pronto para o orientador.** Falta só o que depende do Pedro (seção 8).

- **Cenário padrão** (matriz v3, 22/10): 430 rodadas em duas sessões seguidas, 0 falhas técnicas, 417 válidas, digest idêntico em todas as cargas. Análise pré-registrada em `results/analise-v3-*.csv`. RabbitMQ satura em 60 mil ev/s nos três modos; Kafka sustenta 400 mil, a maior carga testada. Channels e Pipelines sem diferença relevante de latência, throughput ou alocação; custam de 2,4 a 3,3 vezes a CPU do consumidor no Kafka.
- **Cenário ajustado** (27/10): 240 rodadas, 0 falhas técnicas, 232 válidas, digest idêntico. Análise em `results/analise-fase7-*.csv` e `results/fase7-{latencia,vazao}-*.csv`. Com `linger.ms` 1 o P50 do Kafka cai de 3 para 1 ms, mas a cauda sobe a partir de 40 mil. O RabbitMQ com mensagem transitória e `prefetch` 300 corta o P99 de 43 a 76% e passa a sustentar 80 mil.
- **Segundo cenário** (27 e 28/10): 300 rodadas, 0 falhas técnicas, 295 válidas, digest do painel idêntico. Análise em `results/analise-republicacao-*.csv` (`analysis/analise.ps1 -Republicacao`). No Kafka com saída enriquecida, o Direct trava em ~64 mil ev/s e Channels e Pipelines sustentam 200 mil: o mecanismo interno passa a decidir o resultado.
- **Texto** (fase 10): `texto/tcc2.tex` tem todas as seções, com os três cenários, a comparação com os trabalhos relacionados, o apêndice com todas as métricas do cenário padrão e as quatro figuras da arquitetura, que o texto percorre passo a passo. Compilado no Overleaf da UTFPR em 28/10: 35 páginas, sem erros nem avisos além dos tipográficos. O Overleaf tem a mesma versão do git (`3e6baf2`). Nenhum `\pendente` sobra.

## 2. Ambiente

| Ferramenta | Situação |
| --- | --- |
| .NET SDK 10.0.401 | pronto |
| Docker Desktop 4.91 | 12 CPUs e 8 GB na VM do WSL2 |
| Kafka 4.1.0, RabbitMQ 4.3.6, PostgreSQL 17 | em containers, um broker ligado por vez |
| Repositório | https://github.com/ramosnvy/pitwall (público) |
| Texto | Overleaf da UTFPR; o `main.tex` de lá é cópia de `texto/tcc2.tex` |

## 3. O que está decidido

| Tema | Decisão | Onde |
| --- | --- | --- |
| Combinações | 2 brokers × 3 modos (Direct, Channels, Pipelines); o Direct é o grupo de controle | PLANO §1c |
| Cenários de configuração | **Padrão** como linha de base, depois **ajustado** com a recomendação de cada fornecedor (Kafka `linger.ms` 1 e, para vazão, 20; RabbitMQ transitório com `prefetch` 300) | PLANO §1g, pedido do orientador; ANALISE §11 |
| Carga | Telemetria da OpenF1, frota multiplicada, **100 Hz com pontos interpolados**; 130 s da corrida do Bahrein 2024 a partir do minuto 10 (aquecimento, medição e margem) | PLANO §1a; aprovado pelo orientador |
| Sensibilidade | Bateria a 3,7 Hz, o dado sem interpolação, só no cenário padrão | PLANO §1a |
| No texto | O trabalho não analisa a corrida; a conferência do resultado mostra que o processamento está certo | PLANO §1g |
| Mensagem | Binária de 46 bytes, sem compressão, entrega at-least-once nos dois brokers | PLANO §1a, §1f |
| Divisão em filas | 4 faixas, CRC32 nos dois, cada carro sempre na mesma faixa | PLANO §1g; aprovado pelo orientador |
| Equivalência | Limites de armazenamento equivalentes entre os brokers | AUDITORIA-CONFIG |
| Ambiente de medição | Núcleos exclusivos (gerador 0–3, broker 4–7, consumidor 8–10, banco, métricas e painel 11), painéis de visualização desligados, máquina sem uso | PLANO §1a; IMPLEMENTACAO §7 |
| Rodada | 10 s de aquecimento e 90 s de medição (60 s no segundo cenário) | PLANO §2 |
| Validade | Atraso de envio até 50 ms e resultado idêntico entre as seis combinações; no segundo cenário, também painel presente, nenhuma saída recusada e digest do painel | ANALISE |
| Faixas de carga | Padrão: Kafka de 10 mil a 400 mil ev/s, RabbitMQ de 10 mil a 80 mil. Segundo cenário: Kafka 20, 50, 100 e 200 mil, RabbitMQ 10, 20, 30 e 40 mil (enriquecida); 200 e 40 mil (resumida) | ANALISE; DESENVOLVIMENTO |
| Matriz ajustada reduzida | Direct em todas as cargas; Channels e Pipelines em duas por broker; sem 3,7 Hz | ANALISE §11 (27/10, antes de medir) |
| Estatística | Descritiva: 10 repetições, mediana e desvio padrão; Kruskal-Wallis como apoio; sem ANOVA | PLANO §1g |
| Descarte de discrepantes | Regra de Tukey sobre o P99, dentro de cada combinação, depois dos critérios de validade | ANALISE |
| Ponto de saturação | Maioria das rodadas válida, throughput de pelo menos 99% da carga e P99 abaixo de 50 ms | ANALISE |
| Segundo cenário | Passo A feito: republicação do resultado no mesmo broker, at-least-once de ponta a ponta, saída enriquecida e resumida, painel medindo a latência de ponta a ponta. Passo B (vários fluxos, etapas) não feito; só se o orientador pedir | PLANO §1h; APROFUNDAMENTO §8; IMPLEMENTACAO §11 |
| Picos do Kafka a partir de 100 mil ev/s | Comportamento do Kafka sobre o disco virtual da VM, declarado nas ameaças; os arquivos do tópico anterior são apagados antes de cada rodada | IMPLEMENTACAO §9 |
| Figuras da arquitetura | Quatro figuras com ícones no estilo dos diagramas de nuvem, só com o essencial; a explicação fica no texto. Fontes em `texto/figuras/*.html`, exportadas por `texto/figuras/exportar.ps1` | sessão de 28/10 |
| Fora do escopo | RabbitMQ Streams (trabalho futuro), tamanho de mensagem, rodadas longas, mais corridas, 8 filas no RabbitMQ | decisões de 24/09 |

## 4. O que já foi medido

| Experimento | Commit | Resultado |
| --- | --- | --- |
| Primeira matriz: 3,7 Hz, cota de CPU, 165 rodadas | `7e283c2` | Vira calibração: a cota distorcia o RabbitMQ |
| 2×2: cota × núcleos exclusivos, divisão antiga × CRC32 | `7cee9c2` | Núcleos exclusivos reduzem o P99 do RabbitMQ em 40% a 64%. O CRC32 aumenta a CPU do broker em 50 a 130 pontos percentuais e o P99. 35 de 40 rodadas válidas |
| Varredura de saturação a 100 Hz | `7b47f88` | Kafka sustenta de 200 mil a 300 mil ev/s; RabbitMQ, de 50 mil a 80 mil. Banco abaixo de 2% de CPU |
| Auditoria das configurações | `e36b3f8` | Nagle ligado só no Kafka, por escolha nossa; limite de memória do RabbitMQ calculado sobre a VM, não sobre o container; assimetrias em gravação em disco, fila do produtor e busca antecipada do consumidor |
| Medições de decisão da fase 2 | `caf07be` | Nagle sem efeito na latência; mensagem persistente custa 48% no P99 do RabbitMQ a 40 mil ev/s; `prefetch` sem limite e janela de 100 mil decididos |
| Instabilidade do Kafka (fase 4) | `7a2e450`, `d4c2e49` | Picos de 200 ms a 2,1 s no Direct e no Channels, 126 vezes mais prováveis logo depois de a VM parar esperando disco. O log do Kafka estava na camada do container |
| **Matriz v3, cenário padrão** | `79f8aed`, `b6d7cfa` | 430 rodadas, 417 válidas; RESULTADOS §1 |
| Diagnóstico do `linger.ms` | `e26bea6` | `linger.ms` 0 degenera (P50 de ~400 ms a 10 mil); o perfil ajustado usa 1. IMPLEMENTACAO §10 |
| Varredura do segundo cenário | `e6c3841` | 66 rodadas, 0 falhas; cargas da matriz escolhidas pela regra registrada; o painel passa de meio núcleo a 60 mil no RabbitMQ e 300 mil no Kafka |
| **Matriz ajustada** e **matriz do segundo cenário** | `4f3871e` | 240 rodadas (232 válidas) e 300 rodadas (295 válidas); RESULTADOS §2 e §3 |

## 5. Planejado no TCC1 × implementado

| Módulo do TCC1 | Implementação | Relação com o TCC1 |
| --- | --- | --- |
| Ingestão: consome a OpenF1 e publica | `openf1-downloader` offline + `Pitwall.Replayer` em malha aberta, com multiplicação de frota e interpolação a 100 Hz | mudou: a API ao vivo não gera alta frequência (PLANO §1a) |
| Mensageria: RabbitMQ ou Kafka | Kafka 4.1 com 4 partições; RabbitMQ 4.3.6 com 4 filas clássicas; CRC32 nos dois | como planejado |
| Processamento: Channels ou Pipelines | Direct, Channels e Pipelines sobre o mesmo `Processing.Core`; no segundo cenário, as variantes com republicação em `src/Pitwall.Consumer/Republish` | mudou: 2 × 3 em vez de 2 × 2 (PLANO §1c, §1d), mais o segundo cenário |
| Persistência em PostgreSQL | Janelas agregadas por `COPY` binário, tabelas UNLOGGED | como planejado |
| Coleta de métricas | HdrHistogram no consumidor e no painel; cAdvisor e Prometheus; alocações e coletas de lixo por rodada | como planejado, mais atraso de envio e conferência como critérios de validade |
| Tudo em Docker | Containers com núcleos exclusivos | como planejado, com o protocolo de núcleos de IMPLEMENTACAO §7 |
| — | Painel local (Grafana, Loki) e replay 2D das corridas | acréscimo; fora da medição |

## 6. Fases

A sequência completa, com tarefas e critérios de pronto, está em [DESENVOLVIMENTO.md](DESENVOLVIMENTO.md).

| Fase | Entrega | Situação |
| --- | --- | --- |
| 1 | Correções da auditoria, espera síncrona e registro incompleto no Pipe, perfis de configuração, configuração efetiva por rodada | **concluída** (24/09) |
| 2 | Medições de decisão: Nagle, mensagem persistente, `prefetch`, janelas do produtor | **concluída** (25/09; IMPLEMENTACAO §8) |
| 3 | Cenário padrão congelado | **concluída** (13/10; tag `cenario-padrao-v1.2`) |
| 4 | Instabilidade do Kafka com Channels | **concluída** (25/09; IMPLEMENTACAO §9): é a gravação periódica do disco virtual da VM, não o Channels |
| 5 | Análise pré-registrada: Tukey, critério de saturação, `ANALISE.md` | **concluída** (25/09): `analysis/analise.ps1` |
| 6 | Matriz v3 no cenário padrão (`experiments/matriz-v3.ps1`) | **concluída** (22/10): 430 rodadas, 417 válidas |
| 7 | Cenário ajustado (`experiments/matriz-ajustada.ps1`) | **concluída** (27/10): 240 rodadas, 232 válidas |
| 8 | Parte .NET: memória e coleta de lixo | **concluída** (22/10): alocação e coletas iguais nos três modos; o custo é CPU |
| 9A | Segundo cenário, passo A: republicação e latência de ponta a ponta (`experiments/republicacao.ps1`) | **concluída** (27 e 28/10): 300 rodadas, 295 válidas |
| 9B | Segundo cenário, passo B: vários fluxos, etapas e tempo do evento | **não feita**: o passo A já mostrou o efeito; só se o orientador pedir |
| 10 | Texto e figuras | **concluída** (28/10): compilado no Overleaf, 35 páginas; revisão geral feita |

## 7. Pontos de atenção para a conversa com o orientador

- **Resumo longo:** o resumo tem 357 palavras. Se a UTFPR ou o orientador pedirem um limite, o corte natural são os números do cenário ajustado.
- **Matrizes reduzidas:** no cenário ajustado, Channels e Pipelines foram medidos em duas cargas por broker. A decisão foi registrada antes de medir (ANALISE §11) e está nas ameaças.
- **Janela de confirmação do Direct no segundo cenário:** fixa em 100 saídas e não variada. O teto do Direct no Kafka depende dela; está nas ameaças.
- **Disco virtual da VM:** invalidou rodadas do Kafka por atraso de envio: 13 no padrão e 8 no ajustado, todas a 200 mil ev/s ou mais, e 5 no segundo cenário, de 50 a 200 mil. Declarado nas ameaças.
- **Sem resposta do orientador:** núcleos exclusivos (mantidos) e RabbitMQ Streams (trabalho futuro). O critério de saturação e a regra de descarte foram decididos por nós em 24/09 (DESENVOLVIMENTO), sem passar por ele.

## 8. Pendências, por quem resolve

| # | Pendência | Quem | Depende de |
| --- | --- | --- | --- |
| 1 | ~~Matriz v3, análise e resultados no texto~~ | — | **feito** (22/10) |
| 2 | ~~Fase 7: cenário ajustado~~ | — | **feito**: medido em 27/10, no texto |
| 3 | ~~Fase 8: custo do .NET~~ | — | **feito** no texto |
| 4 | ~~Segundo cenário, passo A~~ | — | **feito**: medido em 27 e 28/10, no texto |
| 5 | ~~Compilar no Overleaf~~ | — | **feito** em 28/10: 35 páginas, sem erros |
| 6 | ~~Figuras da arquitetura e texto que as percorre~~ | — | **feito** em 28/10 |
| 7 | Nome completo do orientador (hoje "Prof. Dr. Evandro K.") e banca no `tcc2.tex` | Pedro | — |
| 8 | Datas "Acesso em: maio 2025" no `sbc-template.bib` (só no Overleaf) | Pedro | — |
| 9 | Conferir nas fontes os trabalhos relacionados vindos do TCC1 e as 8 referências novas (`texto/referencias-novas.bib`); afeta a seção Comparação com os Trabalhos Relacionados | Pedro | — |
| 10 | Enviar o texto ao orientador | Pedro | itens 7 a 9, se quiser enviá-los resolvidos |
| 11 | Data de aprovação | — | defesa |
| 12 | Opcional: tirar do Overleaf e do repositório a figura antiga `arquitetura_geral.*`, que o texto não usa mais | Pedro decide | — |

**Trechos `\pendente` no texto:** nenhum desde 28/10.
