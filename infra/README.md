# Infraestrutura dos experimentos

Tudo containerizado, para que o ambiente seja o mesmo em todas as rodadas.

## Primeiro uso

```powershell
copy .env.example .env   # ajuste os limites ao hardware da maquina
```

## Subir

Cada bateria sobe apenas o broker que esta sendo avaliado. Deixar o outro
ligado consumiria CPU e memoria da mesma maquina e contaminaria a medicao.

```powershell
docker compose --profile kafka up -d      # bateria do Kafka
docker compose --profile rabbit up -d     # bateria do RabbitMQ
docker compose --profile metrics up -d    # Prometheus + cAdvisor
docker compose down                       # encerra (volumes preservados)
docker compose down -v                    # encerra e apaga os dados
```

O PostgreSQL nao tem profile: sobe sempre, porque as duas baterias persistem
no mesmo banco.

## Portas

| Servico | Porta | Observacao |
| --- | --- | --- |
| PostgreSQL | 5432 | usuario e senha `pitwall` |
| Kafka | 9092 | listener externo; dentro da rede e `kafka:19092` |
| RabbitMQ | 5672 | AMQP |
| RabbitMQ (UI) | 15672 | painel de gerenciamento |
| RabbitMQ (metricas) | 15692 | formato Prometheus |
| Prometheus | 9090 | |
| cAdvisor | 8080 | CPU e memoria por container |
| Grafana | 3000 | so em `127.0.0.1`, sem login; profile `dash` |

## Painel local (profile `dash`)

Grafana com tres paineis, na pasta PitWall:

| Painel | Fonte | Mostra |
| --- | --- | --- |
| Rodada ao vivo | Prometheus | CPU, memoria e rede de cada container; filas e taxas do RabbitMQ; logs da rodada |
| Logs | Loki | logs de todos os containers `pitwall-*`, filtraveis por arquitetura, carga e rodada |
| Resultados da matriz | PostgreSQL | P50 e P99 por carga, CPU, mediana por celula e rodadas invalidas |

```powershell
docker compose --profile metrics --profile dash up -d
../analysis/load-results.ps1 -In results/matrix-v2-runs.csv   # alimenta "Resultados"
# http://localhost:3000
```

Os logs chegam pelo Alloy, que le os containers direto do Docker. O runner
rotula produtor e consumidor com a rodada (`pitwall.run_id`,
`pitwall.architecture`, `pitwall.rate`, `pitwall.replication`,
`pitwall.role`), e os logs sobrevivem a remocao dos containers.

**Na matriz oficial, desligue o painel** (`docker compose stop grafana loki alloy`):
ele disputa CPU com o que esta sendo medido. O painel "Rodada ao vivo" mostra
o custo dele mesmo em "Custo da propria observacao".

## Criar o topico do Kafka

A criacao automatica de topicos esta desligada, para que o numero de
particoes seja sempre o declarado no experimento e nunca o padrao.

```powershell
docker exec pitwall-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:19092 --create `
  --topic telemetry --partitions 4 --replication-factor 1
```

## Decisoes registradas aqui

- **Limites fixos de CPU e memoria** nos dois brokers. Sem isso, a diferenca
  medida poderia vir apenas de um broker ter conseguido usar mais recursos.
- **Tabelas UNLOGGED** no PostgreSQL e `synchronous_commit=off`: o alvo da
  medicao e o pipeline de eventos, e um banco saturado esconderia a diferenca
  entre as arquiteturas.
- **cAdvisor como fonte de CPU e memoria**, medindo todos os containers pela
  mesma regua, inclusive os brokers.
- **Retencao curta no Kafka** (15 min): as rodadas sao de minutos e o disco nao
  precisa guardar historico entre elas.

Qualquer parametro alterado aqui precisa ir para a tabela de configuracao do
artigo (docs/PLANO.md, secao 1f).
