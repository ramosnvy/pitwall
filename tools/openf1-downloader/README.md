# openf1-downloader

Coleta unica do dataset de telemetria usado nos experimentos.

Os experimentos **nunca** chamam a OpenF1. Os dados sao baixados uma vez para
disco e reproduzidos localmente pelo replayer. Isso mantem o workload
reproduzivel (a mesma corrida, sempre) e respeita os limites da API.

## Uso

Descobrir as sessoes de um ano:

```powershell
dotnet run -- --list-sessions 2024
```

Baixar a telemetria de uma sessao:

```powershell
dotnet run -- --session-key 9472 --endpoints car_data --out ../../data/raw
```

| Opcao | Padrao | Descricao |
| --- | --- | --- |
| `--session-key <n>` | — | Sessao a baixar |
| `--list-sessions <ano>` | — | Lista as sessoes do ano e sai |
| `--endpoints <a,b>` | `car_data` | Endpoints a coletar (ex.: `car_data,location`) |
| `--out <dir>` | `data/raw` | Diretorio de saida |
| `--chunk-minutes <n>` | `10` | Janela de tempo por requisicao |

## Saida

```
data/raw/9472/car_data.jsonl   um evento por linha
data/raw/9472/manifest.json    origem, contagens e data da coleta
```

O `manifest.json` e o que torna o dataset citavel no artigo: registra de qual
sessao os dados vieram, quando foram coletados e quantos eventos cada endpoint
rendeu. O diretorio `data/` nao e versionado (ver `.gitignore`); o manifesto
permite que outra pessoa refaca exatamente a mesma coleta.

## Limites da API

O plano gratuito permite 3 requisicoes por segundo e 30 por minuto. O segundo
limite domina: uma corrida completa de `car_data` sao cerca de 240
requisicoes, entao a coleta leva uns 8 minutos. Respostas 429 sao repetidas
honrando o cabecalho `Retry-After`.

Dados historicos (de 2023 em diante) sao gratuitos e nao exigem autenticacao.
Dados ao vivo exigem assinatura paga e **nao** sao usados neste trabalho.

## Detalhes de implementacao

- A coleta e fatiada por piloto e por janela de tempo: a resposta de uma
  corrida inteira para um unico piloto passa de 25 mil registros.
- A gravacao vai para um arquivo `.partial` e so e renomeada no fim, para que
  uma coleta interrompida nao deixe um dataset incompleto parecendo pronto.
- Arquivos `.jsonl` ja existentes sao preservados: refazer do zero gastaria
  dezenas de minutos de limite de requisicoes a toa. Apague o arquivo para
  recoletar.
