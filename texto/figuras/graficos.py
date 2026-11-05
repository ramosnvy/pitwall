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
import matplotlib.ticker

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


def zoom(ax, broker, log=None, ylim=None, limit=None):
    """Aproxima cada painel da faixa em que as linhas diferem: escala log onde a
    latencia cresce uma ordem de grandeza, e limites justos onde ela e plana. Um
    limite de 50 ms fora da escala vira uma nota no canto do painel."""
    if log and broker in log:
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
        ax.grid(axis="y", which="major")
    if ylim and broker in ylim:
        ax.set_ylim(*ylim[broker])
        if limit is not None and ylim[broker][1] < limit:
            ax.annotate(f"limite de {limit:g} ms acima da escala", xy=(0, 1), xycoords="axes fraction",
                        xytext=(4, -3), textcoords="offset points", ha="left", va="top",
                        fontsize=7, color="#52514e")
            return False
    return True


def figure(field, ylabel, filename, scale=1.0, band=False, limit=None, log=None, ylim=None):
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
        ax.set_title(title, loc="left", fontweight="bold")
        ax.set_xlabel("carga oferecida (mil eventos/s)")
        ax.grid(axis="y")
        ax.set_axisbelow(True)
        if not (log and broker in log) and not (ylim and broker in ylim):
            ax.set_ylim(bottom=0)
        if zoom(ax, broker, log, ylim, limit) and limit is not None:
            ax.axhline(limit, color="#52514e", linewidth=0.8, linestyle=(0, (1, 2)))
            ax.annotate(f"limite de {limit:g} ms", xy=(0, limit), xycoords=("axes fraction", "data"),
                        xytext=(4, 3), textcoords="offset points", ha="left", va="bottom",
                        fontsize=7, color="#52514e")
    axes[0].set_ylabel(ylabel)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="outside upper center", ncol=3, frameon=False)
    fig.savefig(os.path.join(OUT, filename))
    # PNG ao lado, so para conferir a figura sem compilar o texto.
    fig.savefig(os.path.join(OUT, filename.replace(".pdf", ".png")), dpi=200)
    plt.close(fig)
    print("escrito:", filename)


figure("cpu_consumidor", "CPU do consumidor (núcleos)", "fig_cpu_consumidor.pdf", scale=0.01)


# --- latencia: os dois brokers no mesmo painel, modo direto ---------------------
# A comparacao da secao de latencia e entre brokers; a dos modos esta em
# fig_efeito_modos. Escalas log nos dois eixos, para que as cargas baixas, em que o
# RabbitMQ e mais rapido, nao fiquem espremidas.

BROKER_STYLE = {"rabbitmq": ("RabbitMQ", "#eb6834", "s", "--"),
                "kafka": ("Kafka", "#2a78d6", "o", "-")}
CARGAS = [10, 20, 40, 80, 200, 400]
YTICKS = {"p50_ms": [0.3, 0.5, 1, 2, 3, 5], "p99_ms": [2, 5, 10, 20, 50, 100]}

fig, axes = plt.subplots(1, 2, figsize=(6.3, 2.5), constrained_layout=True)
for ax, (field, title) in zip(axes, [("p50_ms", "P50"), ("p99_ms", "P99")]):
    for broker, (label, color, marker, dash) in BROKER_STYLE.items():
        x, y = series(broker, "direct", field)
        ax.plot(x, y, color=color, linestyle=dash, linewidth=1.6, marker=marker, markersize=4.5,
                markeredgecolor="white", markeredgewidth=0.6, label=label)
    if field == "p99_ms":
        ax.axhline(50, color="#52514e", linewidth=0.8, linestyle=(0, (1, 2)))
        ax.annotate("limite de 50 ms", xy=(1, 50), xycoords=("axes fraction", "data"),
                    xytext=(-4, 3), textcoords="offset points", ha="right", va="bottom",
                    fontsize=7, color="#52514e")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(CARGAS)
    ax.set_xticklabels([str(c) for c in CARGAS])
    ax.xaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    ax.set_yticks(YTICKS[field])
    ax.set_ylim(YTICKS[field][0] * 0.85, YTICKS[field][-1] * 1.15)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}".replace(".", ",")))
    ax.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    ax.set_title(title, loc="left", fontweight="bold")
    ax.set_xlabel("carga oferecida (mil eventos/s)")
    ax.grid(axis="y", which="major")
    ax.set_axisbelow(True)
axes[0].set_ylabel("latência (ms)")
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, loc="outside upper center", ncol=2, frameon=False)
fig.savefig(os.path.join(OUT, "fig_p99.pdf"))
fig.savefig(os.path.join(OUT, "fig_p99.png"), dpi=200)
plt.close(fig)
print("escrito: fig_p99.pdf")


# --- efeito de cada mecanismo em relacao ao modo direto, por carga --------------

def efeito(broker, mode, field):
    base = {int(r["carga"]): num(r[field]) for r in resumo
            if r["broker"] == broker and r["modo"] == "direct"}
    rows = sorted((r for r in resumo if r["broker"] == broker and r["modo"] == mode),
                  key=lambda r: int(r["carga"]))
    return [int(r["carga"]) for r in rows], [(num(r[field]) / base[int(r["carga"])] - 1) * 100 for r in rows]


pct = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:+.0f}%".replace("+0%", "0%").replace("-", "−"))
vezes = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}×".replace(".", ","))

fig, axes = plt.subplots(2, 2, figsize=(6.3, 4.2), constrained_layout=True,
                         gridspec_kw={"width_ratios": [5, 8]})
for col, (broker, title) in enumerate(BROKERS):
    for row, field in enumerate(["p99_ms", "cpu_consumidor"]):
        ax = axes[row][col]
        width = 0.38
        if row == 0:
            # faixa de irrelevancia registrada antes da coleta
            ax.axhspan(-20, 20, color="#eceae4", linewidth=0)
            if broker == "kafka":
                ax.text(0.98, 0.2, "faixa de ±20%: sem diferença relevante", transform=ax.transAxes,
                        ha="right", va="bottom", fontsize=7, color="#52514e")
        for k, (mode, label, color, marker, dash) in enumerate(MODES[1:]):
            loads, deltas = efeito(broker, mode, field)
            pos = [i + (k - 0.5) * width for i in range(len(loads))]
            if row == 0:
                ax.bar(pos, deltas, width=width, color=color, label=label,
                       edgecolor="white", linewidth=0.4)
            else:
                # CPU como multiplo da do modo direto: barra a partir de 1x
                ax.bar(pos, [d / 100 for d in deltas], bottom=1, width=width, color=color,
                       label=label, edgecolor="white", linewidth=0.4)
        if row == 0:
            ax.axhline(0, color="#52514e", linewidth=0.8)
            ax.set_ylim(-25, 25)
            ax.set_yticks([-20, -10, 0, 10, 20])
            ax.yaxis.set_major_formatter(pct)
            ax.set_title(title, loc="left", fontweight="bold")
            if broker == "rabbitmq":
                i = loads.index(40000)
                ax.annotate("único ganho:\n−6% a 40 mil", xy=(i, -6.5), xytext=(i, -19),
                            ha="center", va="bottom", fontsize=7, color="#0b0b0b",
                            arrowprops={"arrowstyle": "-", "color": "#52514e", "linewidth": 0.6})
        else:
            ax.axhline(1, color="#52514e", linewidth=0.8)
            ax.set_ylim(1, 1.12 if broker == "rabbitmq" else 3.6)
            ax.set_yticks([1, 1.05, 1.1] if broker == "rabbitmq" else [1, 2, 3])
            ax.yaxis.set_major_formatter(vezes)
            ax.set_xlabel("carga oferecida (mil eventos/s)")
        ax.set_xticks(range(len(loads)))
        ax.set_xticklabels([f"{c // 1000}" for c in loads])
        ax.grid(axis="y")
        ax.set_axisbelow(True)
axes[0][0].set_ylabel("P99 em relação\nao Direct")
axes[1][0].set_ylabel("CPU do consumidor\n(vezes a do Direct)")
handles, labels = axes[0][1].get_legend_handles_labels()
fig.legend(handles, labels, loc="outside upper center", ncol=2, frameon=False)
fig.savefig(os.path.join(OUT, "fig_efeito_modos.pdf"))
fig.savefig(os.path.join(OUT, "fig_efeito_modos.png"), dpi=200)
plt.close(fig)
print("escrito: fig_efeito_modos.pdf")


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
    ax.set_title(title, loc="left", fontweight="bold")
    ax.set_xlabel("carga oferecida (mil eventos/s)")
    ax.grid(axis="y")
    ax.set_axisbelow(True)
    if zoom(ax, broker, log={"rabbitmq"}, ylim={"rabbitmq": (0.4, 120), "kafka": (0, 25)}, limit=50):
        ax.axhline(50, color="#52514e", linewidth=0.8, linestyle=(0, (1, 2)))
        ax.annotate("limite de 50 ms", xy=(0, 50), xycoords=("axes fraction", "data"),
                    xytext=(4, 3), textcoords="offset points", ha="left", va="bottom",
                    fontsize=7, color="#52514e")
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
