"""Figuras de resultados do TCC2, a partir da analise pre-registrada.

Le results/analise-v3-resumo.csv (medianas por combinacao) e
results/analise-v3-rodadas.csv (cada rodada, para a faixa entre o menor e o
maior P99 das rodadas usadas) e escreve, em texto/:

  fig_p99.pdf               P99 por carga, um painel por broker, uma linha por modo
  fig_cpu_consumidor.pdf    CPU do consumidor (nucleos) por carga, idem
  fig_ajustado.pdf          P99 do modo direto por carga, uma linha por perfil
                            (results/analise-fase7-resumo.csv)
  fig_republicacao.pdf      P99 de ponta a ponta na saida enriquecida, por carga
                            e modo, em escala logaritmica
                            (results/analise-republicacao-resumo.csv)

Uso, na raiz do repositorio:  python texto/figuras/graficos.py
"""
import csv
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RES = os.path.join(ROOT, "results")
OUT = os.path.join(ROOT, "texto")

# Paleta categorica validada (tres primeiras posicoes da paleta de referencia,
# aprovadas em todos os pares para daltonismo) e marcador proprio por modo,
# para a figura continuar legivel impressa em tons de cinza.
MODES = [
    ("direct", "Direct", "#2a78d6", "o", "-"),
    ("channels", "Channels", "#eb6834", "s", "--"),
    ("pipelines", "Pipelines", "#1baf7a", "^", ":"),
]
BROKERS = [("rabbitmq", "RabbitMQ"), ("kafka", "Kafka")]


def num(text):
    return float(text.replace(",", ".")) if text not in ("", None) else float("nan")


def read(name):
    with open(os.path.join(RES, name), encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


resumo = [r for r in read("analise-v3-resumo.csv") if r["hz"] == "100"]
rodadas = [r for r in read("analise-v3-rodadas.csv") if r["hz"] == "100"]

# Faixa: rodadas validas e nao descartadas por Tukey, as mesmas da mediana.
faixa = defaultdict(list)
for r in rodadas:
    if r["valid"] == "True" and r["tukey"] == "False":
        faixa[(r["broker"], r["mode"], int(r["rate"]))].append(num(r["p99"]))

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 8,
    "axes.titlesize": 9,
    "axes.labelsize": 8,
    "legend.fontsize": 8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.edgecolor": "#52514e",
    "axes.labelcolor": "#0b0b0b",
    "xtick.color": "#52514e",
    "ytick.color": "#52514e",
    "grid.color": "#e4e3df",
    "grid.linewidth": 0.6,
})


def series(broker, mode, field, scale=1.0):
    rows = sorted((r for r in resumo if r["broker"] == broker and r["modo"] == mode),
                  key=lambda r: int(r["carga"]))
    return [int(r["carga"]) / 1000 for r in rows], [num(r[field]) * scale for r in rows]


def figure(field, ylabel, filename, scale=1.0, band=False, limit=None):
    fig, axes = plt.subplots(1, 2, figsize=(6.3, 2.5), constrained_layout=True)
    for ax, (broker, title) in zip(axes, BROKERS):
        for mode, label, color, marker, dash in MODES:
            x, y = series(broker, mode, field, scale)
            if band:
                lo = [min(faixa[(broker, mode, int(v * 1000))]) for v in x]
                hi = [max(faixa[(broker, mode, int(v * 1000))]) for v in x]
                ax.fill_between(x, lo, hi, color=color, alpha=0.12, linewidth=0)
            ax.plot(x, y, color=color, linestyle=dash, linewidth=1.6, marker=marker,
                    markersize=4.5, markeredgecolor="white", markeredgewidth=0.6, label=label)
        if limit is not None:
            ax.axhline(limit, color="#52514e", linewidth=0.8, linestyle=(0, (1, 2)))
            ax.annotate(f"limite de {limit:g} ms", xy=(0, limit), xycoords=("axes fraction", "data"),
                        xytext=(4, 3), textcoords="offset points", ha="left", va="bottom",
                        fontsize=7, color="#52514e")
        ax.set_title(title, loc="left", fontweight="bold")
        ax.set_xlabel("carga oferecida (mil eventos/s)")
        ax.set_ylim(bottom=0)
        ax.grid(axis="y")
        ax.set_axisbelow(True)
    axes[0].set_ylabel(ylabel)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="outside upper center", ncol=3, frameon=False)
    fig.savefig(os.path.join(OUT, filename))
    # PNG ao lado, so para conferir a figura sem compilar o texto.
    fig.savefig(os.path.join(OUT, filename.replace(".pdf", ".png")), dpi=200)
    plt.close(fig)
    print("escrito:", filename)


figure("p99_ms", "latência P99 (ms)", "fig_p99.pdf", band=True, limit=50)
figure("cpu_consumidor", "CPU do consumidor (núcleos)", "fig_cpu_consumidor.pdf", scale=0.01)


# --- cenario ajustado: P99 do modo direto em cada perfil -----------------------

PROFILES = [
    ("padrao", "padrão", "#2a78d6", "o", "-"),
    ("ajustado", "ajustado", "#eb6834", "s", "--"),
    ("ajustado-vazao", "ajustado para vazão", "#1baf7a", "^", ":"),
]
fase7 = [r for r in read("analise-fase7-resumo.csv") if r["hz"] == "100" and r["modo"] == "direct"]

fig, axes = plt.subplots(1, 2, figsize=(6.3, 2.5), constrained_layout=True)
for ax, (broker, title) in zip(axes, BROKERS):
    for profile, label, color, marker, dash in PROFILES:
        rows = sorted((r for r in fase7 if r["broker"] == broker and r["perfil"] == profile),
                      key=lambda r: int(r["carga"]))
        if not rows:
            continue
        ax.plot([int(r["carga"]) / 1000 for r in rows], [num(r["p99_ms"]) for r in rows],
                color=color, linestyle=dash, linewidth=1.6, marker=marker, markersize=4.5,
                markeredgecolor="white", markeredgewidth=0.6, label=label)
    ax.axhline(50, color="#52514e", linewidth=0.8, linestyle=(0, (1, 2)))
    ax.set_title(title, loc="left", fontweight="bold")
    ax.set_xlabel("carga oferecida (mil eventos/s)")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y")
    ax.set_axisbelow(True)
axes[0].set_ylabel("latência P99 (ms)")
handles, labels = axes[1].get_legend_handles_labels()
fig.legend(handles, labels, loc="outside upper center", ncol=3, frameon=False)
fig.savefig(os.path.join(OUT, "fig_ajustado.pdf"))
fig.savefig(os.path.join(OUT, "fig_ajustado.png"), dpi=200)
plt.close(fig)
print("escrito: fig_ajustado.pdf")


# --- segundo cenario: P99 de ponta a ponta, saida enriquecida -------------------

rep = [r for r in read("analise-republicacao-resumo.csv") if r["saida"] == "enriquecida"]

fig, axes = plt.subplots(1, 2, figsize=(6.3, 2.6), constrained_layout=True)
for ax, (broker, title) in zip(axes, BROKERS):
    for mode, label, color, marker, dash in MODES:
        rows = sorted((r for r in rep if r["broker"] == broker and r["modo"] == mode),
                      key=lambda r: int(r["carga"]))
        ax.plot([int(r["carga"]) / 1000 for r in rows], [num(r["p99_ms"]) for r in rows],
                color=color, linestyle=dash, linewidth=1.6, marker=marker, markersize=4.5,
                markeredgecolor="white", markeredgewidth=0.6, label=label)
    ax.axhline(50, color="#52514e", linewidth=0.8, linestyle=(0, (1, 2)))
    ax.annotate("limite de 50 ms", xy=(0, 50), xycoords=("axes fraction", "data"),
                xytext=(4, 3), textcoords="offset points", ha="left", va="bottom",
                fontsize=7, color="#52514e")
    ax.set_yscale("log")
    ax.set_title(title, loc="left", fontweight="bold")
    ax.set_xlabel("carga oferecida (mil eventos/s)")
    ax.grid(axis="y", which="major")
    ax.set_axisbelow(True)
axes[0].set_ylabel("P99 de ponta a ponta (ms, log)")
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, loc="outside upper center", ncol=3, frameon=False)
fig.savefig(os.path.join(OUT, "fig_republicacao.pdf"))
fig.savefig(os.path.join(OUT, "fig_republicacao.png"), dpi=200)
plt.close(fig)
print("escrito: fig_republicacao.pdf")
