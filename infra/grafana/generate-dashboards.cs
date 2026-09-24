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
    Series("CPU por container", "Percentual de um nÃºcleo, como em RESULTADOS.md. Limites: produtor 400%, consumidor 300%, broker 400%, banco 200%.",
        Grid(0, 1, 12, 9), "percent",
        ($"sum by (name) (rate(container_cpu_usage_seconds_total{{{Measured}}}[$__rate_interval])) * 100", "{{name}}")),
    Series("MemÃ³ria por container", "Working set, a mesma mÃ©trica que o runner grava. No Kafka, o heap da JVM Ã© prÃ©-alocado.",
        Grid(12, 1, 12, 9), "bytes",
        ($"max by (name) (container_memory_working_set_bytes{{{Measured}}})", "{{name}}")),
    Series("Rede recebida por container", "Bytes recebidos por segundo. No broker, aproxima a taxa de publicaÃ§Ã£o: cada evento tem 46 bytes de payload mais o envelope do protocolo.",
        Grid(0, 10, 12, 8), "Bps",
        ($"sum by (name) (rate(container_network_receive_bytes_total{{{Measured}}}[$__rate_interval]))", "{{name}}")),
    Series("Custo da prÃ³pria observaÃ§Ã£o", "CPU do Prometheus, do cAdvisor e do painel. Em rodada oficial, sÃ³ Prometheus e cAdvisor devem aparecer: o profile dash fica desligado.",
        Grid(12, 10, 12, 8), "percent",
        ($"sum by (name) (rate(container_cpu_usage_seconds_total{{{Observer}}}[$__rate_interval])) * 100", "{{name}}")),

    Row("RabbitMQ (sÃ³ com o profile rabbit)", 18),
    Series("Taxa de mensagens", "Contadores globais do plugin Prometheus do RabbitMQ.",
        Grid(0, 19, 12, 8), "short",
        ("sum(rate(rabbitmq_global_messages_received_total[$__rate_interval]))", "publicadas"),
        ("sum(rate(rabbitmq_global_messages_confirmed_total[$__rate_interval]))", "confirmadas ao produtor"),
        ("sum(rate(rabbitmq_global_messages_delivered_total[$__rate_interval]))", "entregues ao consumidor"),
        ("sum(rate(rabbitmq_global_messages_acknowledged_total[$__rate_interval]))", "confirmadas pelo consumidor")),
    Series("Mensagens nas filas", "Prontas e nÃ£o confirmadas, por fila. Fila crescendo significa consumidor atrÃ¡s do produtor.",
        Grid(12, 19, 12, 8), "short",
        ("sum by (queue) (rabbitmq_queue_messages_ready)", "{{queue}} prontas"),
        ("sum by (queue) (rabbitmq_queue_messages_unacked)", "{{queue}} sem ack")),

    Row("Logs", 27),
    Logs("Produtor e consumidor", "Logs dos containers da rodada, coletados pelo Alloy. O resumo de cada rodada aparece quando o consumidor termina.",
        Grid(0, 28, 24, 12), "{role=~\"producer|consumer\"}")
};
Save("rodada-ao-vivo.json", Dashboard("pitwall-live", "PitWall Â· Rodada ao vivo",
    "CPU, memÃ³ria e rede dos containers, filas do RabbitMQ e logs da rodada em andamento.",
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
    Retarget(Series("Linhas de log por container", "Volume de log. Um salto fora do fim de rodada costuma ser erro ou reconexÃ£o.",
        Grid(0, 0, 24, 6), "short"), loki, $"sum by (container) (count_over_time({sel} |~ \"$busca\" [$__auto]))", "{{container}}"),
    Logs("Avisos e erros", "Linhas com aviso, erro, exceÃ§Ã£o ou falha.", Grid(0, 6, 24, 9),
        sel + " |~ \"(?i)(aviso|warn|erro|error|exce|fail|queue full)\""),
    Logs("Todos os logs", "Filtros no topo; a caixa de busca aceita expressÃ£o regular.", Grid(0, 15, 24, 16),
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
Save("logs.json", Dashboard("pitwall-logs", "PitWall Â· Logs",
    "Logs de todos os containers pitwall-*, filtrÃ¡veis pelos rÃ³tulos da rodada.",
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
        "SELECT count(*) AS rodadas,\n  count(*) FILTER (WHERE valid) AS \"vÃ¡lidas\",\n  count(*) FILTER (WHERE NOT valid) AS \"invÃ¡lidas\",\n  min(code_commit) AS commit\nFROM v_matrix WHERE source = '$source'"),
    Table("Rodadas invÃ¡lidas", "Motivo: produtor sem relatÃ³rio, jitter acima de 50 ms ou digest diferente do da maioria na carga.", Grid(8, 0, 16, 5),
        "SELECT architecture AS arquitetura, target_rate AS carga, replication AS rep,\n  invalid_reason AS motivo, round(producer_jitter_ms::numeric, 1) AS jitter_ms, windows_dropped AS janelas_descartadas\nFROM v_matrix WHERE source = '$source' AND NOT valid\nORDER BY target_rate, architecture, replication"),
    Bars("P50 por carga", "Mediana entre as repetiÃ§Ãµes vÃ¡lidas de cada cÃ©lula. Sem barra: carga acima do teto daquela arquitetura.", Grid(0, 5, 12, 10), "ms", Pivot("p50_ms")),
    Bars("P99 por carga", "Escala logarÃ­tmica. O cruzamento das caudas fica entre 20 e 40 mil ev/s.", Grid(12, 5, 12, 10), "ms", Pivot("p99_ms"), logY: true),
    Bars("CPU total da arquitetura", "Produtor + broker + consumidor + banco, em percentual de um nÃºcleo.", Grid(0, 15, 12, 10), "percent", Pivot("total_cpu")),
    Bars("CPU do consumidor", "Container do consumidor: Ã© onde Direct, Channels e Pipelines se diferenciam.", Grid(12, 15, 12, 10), "percent", Pivot("consumer_container_cpu_avg")),
    Table("Mediana por cÃ©lula", "Uma linha por arquitetura e carga, sÃ³ rodadas vÃ¡lidas.", Grid(0, 25, 24, 12),
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
Save("resultados.json", Dashboard("pitwall-results", "PitWall Â· Resultados da matriz",
    "Rodadas carregadas por analysis/load-results.ps1: latÃªncia, CPU e validade por arquitetura e carga.",
    results, resultVars, "now-7d", "", "pitwall"));

static JsonObject Retarget(JsonObject panel, JsonObject ds, string expr, string legend)
{
    panel["datasource"] = ds.DeepClone();
    panel["targets"] = new JsonArray(new JsonObject { ["refId"] = "A", ["datasource"] = ds.DeepClone(), ["expr"] = expr, ["legendFormat"] = legend, ["queryType"] = "range" });
    return panel;
}
