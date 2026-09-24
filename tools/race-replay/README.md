# race-replay

Replay 2D das corridas baixadas pelo `openf1-downloader`: pista, carros em
movimento, classificação, telemetria do carro selecionado e as frenagens que o
`Processing.Core` detecta.

**Não faz parte do experimento.** Serve para explicar o workload (o que é um
evento de telemetria, quantos chegam por carro) e para conferir a olho uma
propriedade do processamento: no modo "Acumuladas", as frenagens detectadas
se concentram na entrada das curvas, onde elas fisicamente acontecem.

## Gerar os dados

Precisa de `car_data.jsonl`, `location.jsonl`, `drivers.json`, `laps.json` e
`position.json` em `data/raw/<sessao>/`. Os três últimos vêm do downloader a
partir desta versão; para uma sessão já baixada, rodar o downloader de novo
só busca o que falta (uma requisição por arquivo).

```powershell
dotnet run -c Release -- --raw ../../data/raw --out web/data
```

Gera `web/data/<sessao>.json` (12 a 17 MB cada) e `web/data/index.json`. A
pasta fica fora do Git, como `data/`.

## Abrir

Sobe junto com o painel local:

```powershell
cd ../../infra
docker compose --profile metrics --profile dash up -d
# http://localhost:3001            (ou http://localhost:3001/#9523 para Mônaco)
```

Espaço reproduz e pausa; as setas avançam e recuam 10 s.

## Como as frenagens são marcadas

As marcas vêm da própria classe `TelemetryProcessor`, com a mesma regra da
matriz: freio passa de abaixo para acima do limiar (50). A ferramenta a roda
com janela de 1 ms, e assim cada amostra cai na própria janela e a janela que
conta uma frenagem aponta o instante exato dela. A detecção não depende do
tamanho da janela, e a ferramenta confere isso: o total com janela de 1 ms
tem de ser igual ao total com a janela de 1 s usada na matriz. Nas nove
corridas, é.

## Limitações

- Posição e telemetria são amostradas a ~3,7 Hz por carro; entre amostras, a
  posição é interpolada em linha reta.
- Carro que abandona parado na pista continua mandando posição e não aparece
  como "OUT" até os dados dele acabarem.
- A contagem de frenagens começa no início da sessão, então inclui a volta de
  apresentação.
