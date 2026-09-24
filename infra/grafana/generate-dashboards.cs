// Gera os paineis do Grafana em infra/grafana/dashboards. Os JSONs sao
// gerados: edite aqui e rode, a partir de infra/grafana,
//   dotnet run generate-dashboards.cs -- dashboards

using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;

var outDir = args[0];
var json = new JsonSerializerOptions { WriteIndented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

var prom = new JsonObject { ["type"] = "prometheus", ["uid"] = "prometheus" };
var loki = new JsonObject { ["type"] = "loki", ["uid"] = "loki" };
var pg = new JsonObject { ["type"] = "grafana-postgresql-datasource", ["uid"] = "postgres" };
JsonObject Ds(JsonObject d) => (JsonObject)d.DeepClone();

int nextId = 1;
JsonObject Grid(int x, int y, int w, int h) => new() { ["x"] = x, ["y"] = y, ["w"] = w, ["h"] = h };

JsonObject Row(string title, int y) => new()
{
    ["type"] = "row", ["id"] = nextId++, ["title"] = title, ["collapsed"] = false,
    ["gridPos"] = Grid(0, y, 24, 1), ["panels"] = new JsonArray()
};

JsonObject Series(string title, string desc, JsonObject grid, string unit, params (string expr, string legend)[] targets)
{
    var t = new JsonArray();
    var refId = 'A';
    foreach (var (expr, legend) in targets)
        t.Add(new JsonObject { ["refId"] = (refId++).ToString(), ["datasource"] = Ds(prom), ["expr"] = expr, ["legendFormat"] = legend, ["range"] = true });

    return new JsonObject
    {
        ["type"] = "timeseries", ["id"] = nextId++, ["title"] = title, ["description"] = desc,
        ["datasource"] = Ds(prom), ["gridPos"] = grid, ["targets"] = t,
        ["fieldConfig"] = new JsonObject
        {
            ["defaults"] = new JsonObject
            {
                ["unit"] = unit,
                ["custom"] = new JsonObject { ["lineWidth"] = 1, ["fillOpacity"] = 8, ["showPoints"] = "never" }
            },
            ["overrides"] = new JsonArray()
        },
        ["options"] = new JsonObject
        {
            ["legend"] = new JsonObject { ["displayMode"] = "table", ["placement"] = "right", ["calcs"] = new JsonArray("mean", "max", "lastNotNull") },
            ["tooltip"] = new JsonObject { ["mode"] = "multi", ["sort"] = "desc" }
        }
    };
}

JsonObject Logs(string title, string desc, JsonObject grid, string expr) => new()
{
    ["type"] = "logs", ["id"] = nextId++, ["title"] = title, ["description"] = desc,
    ["datasource"] = Ds(loki), ["gridPos"] = grid,
    ["targets"] = new JsonArray(new JsonObject { ["refId"] = "A", ["datasource"] = Ds(loki), ["expr"] = expr, ["queryType"] = "range" }),
    ["options"] = new JsonObject
    {
        ["showTime"] = true, ["showLabels"] = false, ["showCommonLabels"] = false, ["wrapLogMessage"] = true,
        ["sortOrder"] = "Descending", ["enableLogDetails"] = true, ["dedupStrategy"] = "none", ["prettifyLogMessage"] = false
    }
};

JsonObject Sql(string refId, string sql) => new()
{
    ["refId"] = refId, ["datasource"] = Ds(pg), ["rawQuery"] = true, ["editorMode"] = "code",
    ["format"] = "table", ["rawSql"] = sql
};

// Barras agrupadas por carga: as cargas ficam igualmente espacadas, o que
// mantem legivel a faixa de 10 a 60 mil ev/s, onde o RabbitMQ opera.
// Cor pela familia do broker (azuis Kafka, laranjas RabbitMQ), tom pelo modo.
var archColors = new Dictionary<string, string>
{
    ["kafka-direct"] = "dark-blue", ["kafka-channels"] = "blue", ["kafka-pipelines"] = "super-light-blue",
    ["rabbitmq-direct"] = "dark-orange", ["rabbitmq-channels"] = "orange", ["rabbitmq-pipelines"] = "super-light-orange"
};

JsonObject Bars(string title, string desc, JsonObject grid, string unit, string sql, bool logY = false)
{
    var custom = new JsonObject { ["lineWidth"] = 0, ["fillOpacity"] = 85, ["gradientMode"] = "none", ["axisSoftMin"] = 0 };
    if (logY)
    {
        custom.Remove("axisSoftMin");
        custom["scaleDistribution"] = new JsonObject { ["type"] = "log", ["log"] = 10 };
    }

    var overrides = new JsonArray();
    foreach (var (arch, color) in archColors)
        overrides.Add(new JsonObject
        {
            ["matcher"] = new JsonObject { ["id"] = "byName", ["options"] = arch },
            ["properties"] = new JsonArray(new JsonObject { ["id"] = "color", ["value"] = new JsonObject { ["mode"] = "fixed", ["fixedColor"] = color } })
        });

    return new JsonObject
    {
        ["type"] = "barchart", ["id"] = nextId++, ["title"] = title, ["description"] = desc,
        ["datasource"] = Ds(pg), ["gridPos"] = grid,
        ["targets"] = new JsonArray(Sql("A", sql)),
        ["fieldConfig"] = new JsonObject
        {
            ["defaults"] = new JsonObject { ["unit"] = unit, ["decimals"] = 1, ["custom"] = custom },
            ["overrides"] = overrides
        },
        ["options"] = new JsonObject
        {
            ["xField"] = "carga", ["orientation"] = "vertical", ["groupWidth"] = 0.8, ["barWidth"] = 0.95,
            ["showValue"] = "never", ["stacking"] = "none", ["xTickLabelRotation"] = 0,
            ["legend"] = new JsonObject { ["displayMode"] = "list", ["placement"] = "bottom", ["showLegend"] = true },
            ["tooltip"] = new JsonObject { ["mode"] = "multi", ["sort"] = "none" }
        }
    };
}

JsonObject Table(string title, string desc, JsonObject grid, string sql) => new()
{
    ["type"] = "table", ["id"] = nextId++, ["title"] = title, ["description"] = desc,
    ["datasource"] = Ds(pg), ["gridPos"] = grid,
    ["targets"] = new JsonArray(Sql("A", sql)),
    ["fieldConfig"] = new JsonObject { ["defaults"] = new JsonObject(), ["overrides"] = new JsonArray() },
    ["options"] = new JsonObject { ["showHeader"] = true, ["cellHeight"] = "sm" }
};

// Pinta de verde, amarelo e vermelho as colunas de coeficiente de variacao:
// abaixo de 5% a medida e precisa; acima de 15%, ruidosa.
JsonObject CvColors(params string[] fields)
{
    var overrides = new JsonArray();
    foreach (var f in fields)
    {
        overrides.Add(new JsonObject
        {
            ["matcher"] = new JsonObject { ["id"] = "byName", ["options"] = f },
            ["properties"] = new JsonArray(
                new JsonObject { ["id"] = "custom.cellOptions", ["value"] = new JsonObject { ["type"] = "color-background", ["mode"] = "basic" } },
                new JsonObject
                {
                    ["id"] = "thresholds",
                    ["value"] = new JsonObject
                    {
                        ["mode"] = "absolute",
                        ["steps"] = new JsonArray(
                            new JsonObject { ["color"] = "green", ["value"] = null },
                            new JsonObject { ["color"] = "yellow", ["value"] = 5 },
                            new JsonObject { ["color"] = "red", ["value"] = 15 })
                    }
                })
        });
    }
    return new JsonObject { ["defaults"] = new JsonObject(), ["overrides"] = overrides };
}

JsonObject MultiSql(JsonObject panel, params (string refId, string sql)[] queries)
{
    var t = new JsonArray();
    foreach (var (refId, sql) in queries) t.Add(Sql(refId, sql));
    panel["targets"] = t;
    return panel;
}

// Dispersao: painel "trend" (eixo x numerico) so com pontos. Cada serie e
// uma coluna, nula nas linhas que nao sao dela. O "xychart" desta versao do
// Grafana exige uma configuracao de series que muda entre versoes; o trend
// e estavel.
JsonObject Scatter(string title, string desc, JsonObject grid, string xField, string unit, string sql) => new()
{
    ["type"] = "trend", ["id"] = nextId++, ["title"] = title, ["description"] = desc,
    ["datasource"] = Ds(pg), ["gridPos"] = grid,
    ["targets"] = new JsonArray(Sql("A", sql)),
    ["fieldConfig"] = new JsonObject
    {
        ["defaults"] = new JsonObject
        {
            ["unit"] = unit,
            ["custom"] = new JsonObject { ["drawStyle"] = "points", ["lineWidth"] = 0, ["showPoints"] = "always", ["pointSize"] = 8, ["spanNulls"] = false }
        },
        ["overrides"] = new JsonArray(new JsonObject
        {
            ["matcher"] = new JsonObject { ["id"] = "byName", ["options"] = xField },
            ["properties"] = new JsonArray(new JsonObject { ["id"] = "unit", ["value"] = "percent" })
        })
    },
    ["options"] = new JsonObject
    {
        ["xField"] = xField,
        ["legend"] = new JsonObject { ["displayMode"] = "list", ["placement"] = "bottom", ["showLegend"] = true },
        ["tooltip"] = new JsonObject { ["mode"] = "single" }
    }
};

JsonObject Histogram(string title, string desc, JsonObject grid, string unit, params (string refId, string sql)[] queries) => MultiSql(new JsonObject
{
    ["type"] = "histogram", ["id"] = nextId++, ["title"] = title, ["description"] = desc,
    ["datasource"] = Ds(pg), ["gridPos"] = grid,
    ["fieldConfig"] = new JsonObject
    {
        ["defaults"] = new JsonObject { ["unit"] = unit, ["custom"] = new JsonObject { ["fillOpacity"] = 60, ["lineWidth"] = 1 } },
        ["overrides"] = new JsonArray()
    },
    ["options"] = new JsonObject
    {
        ["bucketCount"] = 24, ["combine"] = false,
        ["legend"] = new JsonObject { ["displayMode"] = "list", ["placement"] = "bottom", ["showLegend"] = true }
    }
}, queries);

JsonObject Dashboard(string uid, string title, string desc, JsonArray panels, JsonArray vars, string from, string refresh, params string[] tags) => new()
{
    ["uid"] = uid, ["title"] = title, ["description"] = desc,
    ["tags"] = new JsonArray(tags.Select(t => (JsonNode)t).ToArray()),
    ["timezone"] = "browser", ["schemaVersion"] = 39, ["version"] = 1, ["editable"] = true,
    ["graphTooltip"] = 1, ["refresh"] = refresh,
    ["time"] = new JsonObject { ["from"] = from, ["to"] = "now" },
    ["templating"] = new JsonObject { ["list"] = vars },
    ["annotations"] = new JsonObject { ["list"] = new JsonArray() },
    ["links"] = new JsonArray(
        new JsonObject
        {
            ["type"] = "dashboards", ["tags"] = new JsonArray("pitwall"), ["asDropdown"] = false,
            ["title"] = "PitWall", ["includeVars"] = false, ["keepTime"] = true
        },
        new JsonObject
        {
            ["type"] = "link", ["title"] = "Replay 2D", ["url"] = "http://localhost:3001",
            ["targetBlank"] = true, ["icon"] = "external link", ["tooltip"] = "Replay das corridas (tools/race-replay)"
        }),
    ["panels"] = panels
};

void Save(string file, JsonObject d) => File.WriteAllText(Path.Combine(outDir, file), d.ToJsonString(json) + "\n");

// ---------------------------------------------------------------- ao vivo
const string Measured = "name=~\"pitwall-(producer|consumer|kafka|rabbitmq|postgres)\"";
const string Observer = "name=~\"pitwall-(prometheus|cadvisor|grafana|loki|alloy)\"";

nextId = 1;
var live = new JsonArray
{
    Row("Containers da rodada", 0),
    Series("CPU por container", "Percentual de um núcleo, como em RESULTADOS.md. Limites: produtor 400%, consumidor 300%, broker 400%, banco 200%.",
        Grid(0, 1, 12, 9), "percent",
        ($"sum by (name) (rate(container_cpu_usage_seconds_total{{{Measured}}}[$__rate_interval])) * 100", "{{name}}")),
    Series("Memória por container", "Working set, a mesma métrica que o runner grava. No Kafka, o heap da JVM é pré-alocado.",
        Grid(12, 1, 12, 9), "bytes",
        ($"max by (name) (container_memory_working_set_bytes{{{Measured}}})", "{{name}}")),
    Series("Rede recebida por container", "Bytes recebidos por segundo. No broker, aproxima a taxa de publicação: cada evento tem 46 bytes de payload mais o envelope do protocolo.",
        Grid(0, 10, 12, 8), "Bps",
        ($"sum by (name) (rate(container_network_receive_bytes_total{{{Measured}}}[$__rate_interval]))", "{{name}}")),
    Series("Custo da própria observação", "CPU do Prometheus, do cAdvisor e do painel. Em rodada oficial, só Prometheus e cAdvisor devem aparecer: o profile dash fica desligado.",
        Grid(12, 10, 12, 8), "percent",
        ($"sum by (name) (rate(container_cpu_usage_seconds_total{{{Observer}}}[$__rate_interval])) * 100", "{{name}}")),

    Row("RabbitMQ (só com o profile rabbit)", 18),
    Series("Taxa de mensagens", "Contadores globais do plugin Prometheus do RabbitMQ.",
        Grid(0, 19, 12, 8), "short",
        ("sum(rate(rabbitmq_global_messages_received_total[$__rate_interval]))", "publicadas"),
        ("sum(rate(rabbitmq_global_messages_confirmed_total[$__rate_interval]))", "confirmadas ao produtor"),
        ("sum(rate(rabbitmq_global_messages_delivered_total[$__rate_interval]))", "entregues ao consumidor"),
        ("sum(rate(rabbitmq_global_messages_acknowledged_total[$__rate_interval]))", "confirmadas pelo consumidor")),
    Series("Mensagens nas filas", "Prontas e não confirmadas, por fila. Fila crescendo significa consumidor atrás do produtor.",
        Grid(12, 19, 12, 8), "short",
        ("sum by (queue) (rabbitmq_queue_messages_ready)", "{{queue}} prontas"),
        ("sum by (queue) (rabbitmq_queue_messages_unacked)", "{{queue}} sem ack")),

    Row("Logs", 27),
    Logs("Produtor e consumidor", "Logs dos containers da rodada, coletados pelo Alloy. O resumo de cada rodada aparece quando o consumidor termina.",
        Grid(0, 28, 24, 12), "{role=~\"producer|consumer\"}"),

    Row("Cota de CPU", 40),
    Series("Estrangulamento pela cota", "Percentual dos períodos CFS (100 ms) em que o container esgotou a cota e ficou congelado até o período seguinte. Zero com núcleos fixos (cpuset). Ver docs/IMPLEMENTACAO.md, seção 2.",
        Grid(0, 41, 24, 8), "percent",
        ($"100 * sum by (name) (rate(container_cpu_cfs_throttled_periods_total{{{Measured}}}[$__rate_interval])) / sum by (name) (rate(container_cpu_cfs_periods_total{{{Measured}}}[$__rate_interval]))", "{{name}}"))
};
Save("rodada-ao-vivo.json", Dashboard("pitwall-live", "PitWall · Rodada ao vivo",
    "CPU, memória e rede dos containers, filas do RabbitMQ e logs da rodada em andamento.",
    live, new JsonArray(), "now-30m", "5s", "pitwall"));

// ---------------------------------------------------------------- logs
JsonObject LokiVar(string name, string label, string allValue) => new()
{
    ["type"] = "query", ["name"] = name, ["label"] = label, ["datasource"] = Ds(loki),
    ["query"] = $"label_values({name})", ["refresh"] = 2, ["includeAll"] = true, ["multi"] = true,
    ["allValue"] = allValue, ["current"] = new JsonObject { ["text"] = "All", ["value"] = "$__all" }, ["sort"] = 1
};

var sel = "{container=~\"$container\", architecture=~\"$architecture\", rate=~\"$rate\", run_id=~\"$run_id\"}";
nextId = 1;
var logs = new JsonArray
{
    Retarget(Series("Linhas de log por container", "Volume de log. Um salto fora do fim de rodada costuma ser erro ou reconexão.",
        Grid(0, 0, 24, 6), "short"), loki, $"sum by (container) (count_over_time({sel} |~ \"$busca\" [$__auto]))", "{{container}}"),
    Logs("Avisos e erros", "Linhas com aviso, erro, exceção ou falha.", Grid(0, 6, 24, 9),
        sel + " |~ \"(?i)(aviso|warn|erro|error|exce|fail|queue full)\""),
    Logs("Todos os logs", "Filtros no topo; a caixa de busca aceita expressão regular.", Grid(0, 15, 24, 16),
        sel + " |~ \"$busca\"")
};
var logVars = new JsonArray
{
    LokiVar("container", "Container", "pitwall-.+"),
    LokiVar("architecture", "Arquitetura", ".*"),
    LokiVar("rate", "Carga", ".*"),
    LokiVar("run_id", "Rodada", ".*"),
    new JsonObject { ["type"] = "textbox", ["name"] = "busca", ["label"] = "Busca", ["query"] = "", ["current"] = new JsonObject { ["text"] = "", ["value"] = "" } }
};
Save("logs.json", Dashboard("pitwall-logs", "PitWall · Logs",
    "Logs de todos os containers pitwall-*, filtráveis pelos rótulos da rodada.",
    logs, logVars, "now-6h", "30s", "pitwall"));

// ---------------------------------------------------------------- resultados
string[] archs = ["kafka-direct", "kafka-channels", "kafka-pipelines", "rabbitmq-direct", "rabbitmq-channels", "rabbitmq-pipelines"];
string Pivot(string metric) =>
    "SELECT (target_rate / 1000) || ' mil' AS carga,\n" +
    string.Join(",\n", archs.Select(a => $"  percentile_cont(0.5) WITHIN GROUP (ORDER BY {metric}) FILTER (WHERE architecture = '{a}') AS \"{a}\"")) +
    "\nFROM v_matrix\nWHERE valid AND source = '$source'\nGROUP BY target_rate\nORDER BY target_rate";

nextId = 1;
var results = new JsonArray
{
    Table("Rodadas", "Validade pelas mesmas regras de analysis/summarize.ps1.", Grid(0, 0, 8, 5),
        "SELECT count(*) AS rodadas,\n  count(*) FILTER (WHERE valid) AS \"válidas\",\n  count(*) FILTER (WHERE NOT valid) AS \"inválidas\",\n  min(code_commit) AS commit\nFROM v_matrix WHERE source = '$source'"),
    Table("Rodadas inválidas", "Motivo: produtor sem relatório, jitter acima de 50 ms ou digest diferente do da maioria na carga.", Grid(8, 0, 16, 5),
        "SELECT architecture AS arquitetura, target_rate AS carga, replication AS rep,\n  invalid_reason AS motivo, round(producer_jitter_ms::numeric, 1) AS jitter_ms, windows_dropped AS janelas_descartadas\nFROM v_matrix WHERE source = '$source' AND NOT valid\nORDER BY target_rate, architecture, replication"),
    Bars("P50 por carga", "Mediana entre as repetições válidas de cada célula. Sem barra: carga acima do teto daquela arquitetura.", Grid(0, 5, 12, 10), "ms", Pivot("p50_ms")),
    Bars("P99 por carga", "Escala logarítmica. O cruzamento das caudas fica entre 20 e 40 mil ev/s.", Grid(12, 5, 12, 10), "ms", Pivot("p99_ms"), logY: true),
    Bars("CPU total da arquitetura", "Produtor + broker + consumidor + banco, em percentual de um núcleo.", Grid(0, 15, 12, 10), "percent", Pivot("total_cpu")),
    Bars("CPU do consumidor", "Container do consumidor: é onde Direct, Channels e Pipelines se diferenciam.", Grid(12, 15, 12, 10), "percent", Pivot("consumer_container_cpu_avg")),
    Table("Mediana por célula", "Uma linha por arquitetura e carga, só rodadas válidas.", Grid(0, 25, 24, 12),
        "SELECT architecture AS arquitetura, target_rate AS carga, count(*) AS validas,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY throughput)::numeric, 0) AS vazao,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY p50_ms)::numeric, 2) AS p50_ms,\n" +
        "  round(min(p99_ms)::numeric, 2) AS p99_min,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY p99_ms)::numeric, 2) AS p99_ms,\n" +
        "  round(max(p99_ms)::numeric, 2) AS p99_max,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY consumer_container_cpu_avg)::numeric, 1) AS cpu_consumidor,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY broker_cpu_avg)::numeric, 1) AS cpu_broker,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY total_cpu)::numeric, 1) AS cpu_total,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY windows_dropped)::numeric, 0) AS janelas_descartadas\n" +
        "FROM v_matrix WHERE valid AND source = '$source'\nGROUP BY architecture, target_rate\nORDER BY target_rate, architecture")
};
var resultVars = new JsonArray
{
    new JsonObject
    {
        ["type"] = "query", ["name"] = "source", ["label"] = "Matriz", ["datasource"] = Ds(pg),
        ["query"] = "SELECT DISTINCT source FROM matrix_run ORDER BY 1 DESC", ["definition"] = "SELECT DISTINCT source FROM matrix_run ORDER BY 1 DESC",
        ["refresh"] = 1, ["includeAll"] = false, ["multi"] = false, ["sort"] = 0
    }
};
Save("resultados.json", Dashboard("pitwall-results", "PitWall · Resultados da matriz",
    "Rodadas carregadas por analysis/load-results.ps1: latência, CPU e validade por arquitetura e carga.",
    results, resultVars, "now-7d", "", "pitwall"));

// ---------------------------------------------------------------- estatisticas
nextId = 1;
var stats = new JsonArray
{
    Row("Visão geral de tudo o que foi capturado", 0),
    Table("Rodadas por fonte", "Cada CSV carregado por analysis/load-results.ps1. Inválidas pelas regras do summarize.ps1: sem relatório do produtor, jitter acima de 50 ms ou digest diferente do da maioria na carga.",
        Grid(0, 1, 24, 6),
        "SELECT source AS fonte, count(*) AS rodadas,\n" +
        "  count(*) FILTER (WHERE valid) AS validas,\n" +
        "  count(*) FILTER (WHERE invalid_reason LIKE '%jitter%') AS inval_jitter,\n" +
        "  count(*) FILTER (WHERE invalid_reason LIKE '%digest%') AS inval_digest,\n" +
        "  count(*) FILTER (WHERE invalid_reason LIKE '%produtor%') AS inval_produtor,\n" +
        "  count(DISTINCT architecture) AS arquiteturas, count(DISTINCT target_rate) AS cargas,\n" +
        "  min(finished_at) AS inicio, max(finished_at) AS fim,\n" +
        "  string_agg(DISTINCT code_commit, ', ') AS commits\n" +
        "FROM v_matrix GROUP BY source ORDER BY min(finished_at)"),

    Row("Precisão da medição", 7),
    WithFieldConfig(Table("Dispersão entre repetições", "Só rodadas válidas. CV = desvio padrão / média: abaixo de 5% (verde) a medida é precisa; acima de 15% (vermelho), a célula é ruidosa e a mediana deve ser lida com o mínimo e o máximo.",
        Grid(0, 8, 24, 12),
        "SELECT architecture AS arquitetura, target_rate AS carga, count(*) AS n,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY p99_ms)::numeric, 2) AS p99_mediana_ms,\n" +
        "  round(min(p99_ms)::numeric, 2) AS p99_min, round(max(p99_ms)::numeric, 2) AS p99_max,\n" +
        "  round(stddev_samp(p99_ms)::numeric, 2) AS p99_desvio,\n" +
        "  round((100 * stddev_samp(p99_ms) / nullif(avg(p99_ms), 0))::numeric, 1) AS p99_cv_pct,\n" +
        "  round((100 * stddev_samp(p50_ms) / nullif(avg(p50_ms), 0))::numeric, 1) AS p50_cv_pct,\n" +
        "  round((100 * stddev_samp(throughput) / nullif(avg(throughput), 0))::numeric, 2) AS vazao_cv_pct,\n" +
        "  round((100 * stddev_samp(total_cpu) / nullif(avg(total_cpu), 0))::numeric, 1) AS cpu_total_cv_pct\n" +
        "FROM v_matrix WHERE valid AND source = '$source'\nGROUP BY architecture, target_rate ORDER BY target_rate, architecture"),
        CvColors("p99_cv_pct", "p50_cv_pct", "vazao_cv_pct", "cpu_total_cv_pct")),

    Row("Distribuições", 20),
    Histogram("Distribuição do P99 por broker", "Uma observação por rodada válida da fonte escolhida.", Grid(0, 21, 12, 10), "ms",
        ("A", "SELECT p99_ms AS \"Kafka\" FROM v_matrix WHERE valid AND source = '$source' AND broker = 'kafka'"),
        ("B", "SELECT p99_ms AS \"RabbitMQ\" FROM v_matrix WHERE valid AND source = '$source' AND broker = 'rabbitmq'")),
    Scatter("P99 × estrangulamento do broker", "Cada ponto é uma rodada (válida ou não). Só aparecem as fontes que registram o estrangulamento, como o experimento 2x2 (asym-ab-runs.csv).",
        Grid(12, 21, 12, 10), "estrangulado", "ms",
        // O trend exige x estritamente crescente; rodadas com cpuset empatam em
        // 0%. O deslocamento de 0,001 ponto por linha desempata sem aparecer.
        "SELECT broker_throttled_pct + 0.001 * row_number() OVER (ORDER BY broker_throttled_pct, run_id) AS estrangulado,\n" +
        "  CASE WHEN broker_cpu_mode LIKE 'quota%' AND lane_hash = 'modulo' THEN p99_ms END AS \"cota, módulo\",\n" +
        "  CASE WHEN broker_cpu_mode LIKE 'quota%' AND lane_hash = 'crc32' THEN p99_ms END AS \"cota, crc32\",\n" +
        "  CASE WHEN broker_cpu_mode NOT LIKE 'quota%' AND lane_hash = 'modulo' THEN p99_ms END AS \"núcleos fixos, módulo\",\n" +
        "  CASE WHEN broker_cpu_mode NOT LIKE 'quota%' AND lane_hash = 'crc32' THEN p99_ms END AS \"núcleos fixos, crc32\"\n" +
        "FROM v_matrix WHERE source = '$source' AND broker_throttled_pct IS NOT NULL\nORDER BY broker_throttled_pct"),

    Row("Assimetrias: faixas × CPU do broker (experimento 2x2)", 31),
    Table("Células do 2x2", "Medianas de todas as rodadas e só das válidas. Estrangulado = percentual de períodos CFS congelados. Recarregue com analysis/load-results.ps1 para ver as rodadas novas.",
        Grid(0, 32, 24, 9),
        "SELECT target_rate AS carga, lane_hash AS faixas,\n" +
        "  CASE WHEN broker_cpu_mode LIKE 'quota%' THEN 'cota' ELSE 'núcleos fixos' END AS cpu,\n" +
        "  count(*) AS rodadas, count(*) FILTER (WHERE valid) AS validas,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY p99_ms)::numeric, 2) AS p99_todas_ms,\n" +
        "  round((percentile_cont(0.5) WITHIN GROUP (ORDER BY p99_ms) FILTER (WHERE valid))::numeric, 2) AS p99_validas_ms,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY p50_ms)::numeric, 2) AS p50_ms,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY broker_throttled_pct)::numeric, 1) AS estrangulado_pct,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY producer_jitter_ms)::numeric, 1) AS jitter_ms,\n" +
        "  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY broker_cpu_avg)::numeric, 1) AS cpu_broker_pct\n" +
        "FROM v_matrix WHERE lane_hash IS NOT NULL AND broker_cpu_mode IS NOT NULL AND source = '$source'\n" +
        "GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"),

    Row("Ao vivo (Prometheus)", 41),
    Series("Estrangulamento pela cota", "Percentual dos períodos CFS em que cada container ficou congelado.",
        Grid(0, 42, 12, 8), "percent",
        ($"100 * sum by (name) (rate(container_cpu_cfs_throttled_periods_total{{{Measured}}}[$__rate_interval])) / sum by (name) (rate(container_cpu_cfs_periods_total{{{Measured}}}[$__rate_interval]))", "{{name}}")),
    Series("CPU: percentil 95 móvel de 5 min", "Percentil 95 do uso de CPU de cada container nos últimos 5 minutos, em percentual de um núcleo.",
        Grid(12, 42, 12, 8), "percent",
        ($"quantile_over_time(0.95, (sum by (name) (rate(container_cpu_usage_seconds_total{{{Measured}}}[30s])) * 100)[5m:15s])", "{{name}}"))
};
var statVars = new JsonArray
{
    new JsonObject
    {
        ["type"] = "query", ["name"] = "source", ["label"] = "Fonte", ["datasource"] = Ds(pg),
        ["query"] = "SELECT DISTINCT source FROM matrix_run ORDER BY 1 DESC", ["definition"] = "SELECT DISTINCT source FROM matrix_run ORDER BY 1 DESC",
        ["refresh"] = 1, ["includeAll"] = false, ["multi"] = false, ["sort"] = 0
    }
};
Save("estatisticas.json", Dashboard("pitwall-stats", "PitWall · Estatísticas",
    "Estatísticas de tudo o que o experimento captura: rodadas e validade por fonte, precisão entre repetições, distribuições, o 2x2 das assimetrias e a cota de CPU ao vivo.",
    stats, statVars, "now-30m", "30s", "pitwall"));

static JsonObject WithFieldConfig(JsonObject panel, JsonObject fieldConfig)
{
    panel["fieldConfig"] = fieldConfig;
    return panel;
}

static JsonObject Retarget(JsonObject panel, JsonObject ds, string expr, string legend)
{
    panel["datasource"] = ds.DeepClone();
    panel["targets"] = new JsonArray(new JsonObject { ["refId"] = "A", ["datasource"] = ds.DeepClone(), ["expr"] = expr, ["legendFormat"] = legend, ["queryType"] = "range" });
    return panel;
}
