"""Tabelas completas de metricas do cenario padrao (apendice do TCC2).

Le as rodadas usadas pela analise pre-registrada (results/analise-v3-rodadas.csv:
validas e nao descartadas por Tukey, a 100 Hz) e os CSVs brutos da matriz v3,
e escreve texto/tab_metricas.tex com duas tabelas, uma linha por combinacao,
cada valor a mediana das rodadas usadas:

  latencia  vazao e latencia media, P50, P95, P99 e maxima (ms)
  recursos  CPU media e de pico (nucleos) do consumidor, do broker e do gerador;
            memoria media e de pico (MB) do consumidor e do broker

CPU do consumidor: a media e a do container, a mesma do texto e da Figura 3; o
pico e o da amostragem do proprio processo. Memoria do consumidor: a do processo.
Broker e gerador: cAdvisor.

Uso, na raiz do repositorio:  python texto/figuras/tabelas.py
"""
import csv
import os
from collections import defaultdict
from statistics import median

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RES = os.path.join(ROOT, "results")
OUT = os.path.join(ROOT, "texto", "tab_metricas.tex")

BROKERS = [("rabbitmq", "RabbitMQ"), ("kafka", "Kafka")]
MODES = [("direct", "Direto"), ("channels", "Channels"), ("pipelines", "Pipelines")]


def read(name):
    with open(os.path.join(RES, name), encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def num(text):
    return float(text.replace(",", ".")) if text not in ("", None) else float("nan")


usadas = {r["run_id"] for r in read("analise-v3-rodadas.csv")
          if r["hz"] == "100" and r["valid"] == "True" and r["tukey"] == "False"}

combos = defaultdict(list)
for name in ("matriz-v3-rabbit-runs.csv", "matriz-v3-kafka-runs.csv"):
    for r in read(name):
        if r["run_id"] in usadas:
            broker, mode = r["architecture"].split("-", 1)
            combos[(broker, mode, int(r["target_rate"]))].append(r)


def med(rows, field, scale=1.0):
    return median(num(r[field]) for r in rows) * scale


def fmt(value, digits):
    return f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def rows_for(cells):
    lines = []
    for broker, broker_label in BROKERS:
        keys = sorted(k for k in combos if k[0] == broker)
        loads = sorted({k[2] for k in keys})
        first_broker = True
        for mode, mode_label in MODES:
            first_mode = True
            for load in loads:
                rows = combos[(broker, mode, load)]
                b = f"\\multirow{{{3 * len(loads)}}}{{*}}{{{broker_label}}}" if first_broker else ""
                m = f"\\multirow{{{len(loads)}}}{{*}}{{{mode_label}}}" if first_mode else ""
                lines.append(f"{b} & {m} & {load // 1000} & " + " & ".join(cells(rows)) + " \\\\")
                first_broker = first_mode = False
            lines.append("\\cline{2-%d}" % (3 + len(cells(rows))) if mode != "pipelines" else "\\hline")
    return "\n".join(lines)


def latency(rows):
    return [fmt(med(rows, "throughput", 1e-3), 1),
            fmt(med(rows, "mean_us", 1e-3), 2), fmt(med(rows, "p50_us", 1e-3), 2),
            fmt(med(rows, "p95_us", 1e-3), 2), fmt(med(rows, "p99_us", 1e-3), 2),
            fmt(med(rows, "max_us", 1e-3), 1)]


def resources(rows):
    return [fmt(med(rows, "consumer_container_cpu_avg", 0.01), 2), fmt(med(rows, "cpu_peak", 0.01), 2),
            fmt(med(rows, "broker_cpu_avg", 0.01), 2), fmt(med(rows, "broker_cpu_peak", 0.01), 2),
            fmt(med(rows, "producer_cpu_avg", 0.01), 2), fmt(med(rows, "producer_cpu_peak", 0.01), 2),
            fmt(med(rows, "mem_avg_mb"), 0), fmt(med(rows, "mem_peak_mb"), 0),
            fmt(med(rows, "broker_mem_avg_mb"), 0), fmt(med(rows, "broker_mem_peak_mb"), 0)]


tex = f"""% Gerado por texto/figuras/tabelas.py; nao editar a mao.
\\begin{{table}}[p]
\\centering
\\caption{{Cenário padrão a 100~Hz: throughput e latência por combinação. Mediana das
rodadas válidas e não descartadas; latências em ms.}}
\\label{{tab:metricas-latencia}}
\\footnotesize
\\begin{{tabular}}{{|l|l|r|r|r|r|r|r|r|}}
\\hline
\\textbf{{Broker}} & \\textbf{{Modo}} & \\textbf{{Carga (mil)}} & \\textbf{{Vazão (mil)}} & \\textbf{{Média}} & \\textbf{{P50}} & \\textbf{{P95}} & \\textbf{{P99}} & \\textbf{{Máx.}} \\\\
\\hline
{rows_for(latency)}
\\end{{tabular}}
\\end{{table}}

\\begin{{table}}[p]
\\centering
\\caption{{Cenário padrão a 100~Hz: CPU (núcleos; 1,0 = um núcleo inteiro) e memória (MB)
por combinação, médias e picos de cada rodada. Mediana das rodadas válidas e não
descartadas. A CPU média do consumidor é a do container; o pico de CPU e a memória do
consumidor, as do próprio processo; broker e gerador, as dos containers. Cons.:
consumidor; ger.: gerador.}}
\\label{{tab:metricas-recursos}}
\\scriptsize
\\setlength{{\\tabcolsep}}{{4pt}}
\\begin{{tabular}}{{|l|l|r|r|r|r|r|r|r|r|r|r|r|}}
\\hline
\\multirow{{2}}{{*}}{{\\textbf{{Broker}}}} & \\multirow{{2}}{{*}}{{\\textbf{{Modo}}}} & \\multirow{{2}}{{*}}{{\\textbf{{Carga}}}} & \\multicolumn{{2}}{{c|}}{{\\textbf{{CPU cons.}}}} & \\multicolumn{{2}}{{c|}}{{\\textbf{{CPU broker}}}} & \\multicolumn{{2}}{{c|}}{{\\textbf{{CPU ger.}}}} & \\multicolumn{{2}}{{c|}}{{\\textbf{{Mem. cons.}}}} & \\multicolumn{{2}}{{c|}}{{\\textbf{{Mem. broker}}}} \\\\
\\cline{{4-13}}
 & & & \\textbf{{méd.}} & \\textbf{{pico}} & \\textbf{{méd.}} & \\textbf{{pico}} & \\textbf{{méd.}} & \\textbf{{pico}} & \\textbf{{méd.}} & \\textbf{{pico}} & \\textbf{{méd.}} & \\textbf{{pico}} \\\\
\\hline
{rows_for(resources)}
\\end{{tabular}}
\\end{{table}}
"""

with open(OUT, "w", encoding="utf-8") as f:
    f.write(tex)
print("escrito:", os.path.relpath(OUT, ROOT), "-", len(combos), "combinacoes,",
      sum(len(v) for v in combos.values()), "rodadas")
