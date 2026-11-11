"""Figuras de resultados do TCC2, a partir da analise pre-registrada.

Todas as figuras usam o mesmo formato, pedido pelo orientador (audios de
09/11/2026): linhas, com todas as combinacoes no mesmo painel, uma cor por
broker e um traco e marcador por modo; o eixo horizontal e sempre a carga
oferecida, em escala logaritmica, com as mesmas marcas.

Le results/analise-v3-resumo.csv e results/analise-fase7-resumo.csv (medianas
por combinacao) e escreve, em texto/:

  fig_latencia_throughput.pdf   P50, P99 e throughput das seis combinacoes
  fig_recursos.pdf              CPU e memoria do consumidor e do broker, idem
  fig_ajustado.pdf              modo direto no cenario padrao e nos ajustados:
                                P50, P99 e CPU do broker

As figuras de versoes anteriores do texto (fig_p99, fig_efeito_modos,
fig_cpu_consumidor, fig_republicacao) ficaram no repositorio, mas nao sao mais
geradas aqui; o script que as gerava esta no historico do git.

Uso, na raiz do repositorio:  python texto/figuras/graficos.py
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.lines
import matplotlib.ticker

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RES = os.path.join(ROOT, "results")
OUT = os.path.join(ROOT, "texto")

# Cor por broker (pares aprovados para daltonismo); traco e marcador por modo,
# para a figura continuar legivel impressa em tons de cinza.
BROKERS = [("rabbitmq", "RabbitMQ", "#eb6834"), ("kafka", "Kafka", "#2a78d6")]
MODES = [("direct", "Direct", "o", "-"), ("channels", "Channels", "s", "--"),
         ("pipelines", "Pipelines", "^", ":")]
PROFILES = [("padrao", "padrão", "o", "-"), ("ajustado", "ajustado", "s", "--"),
            ("ajustado-vazao", "ajustado para vazão", "^", ":")]
CARGAS = [10, 20, 40, 80, 200, 400]
CINZA = "#52514e"


def num(text):
    return float(text.replace(",", ".")) if text not in ("", None) else float("nan")


def read(name):
    with open(os.path.join(RES, name), encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


resumo = [r for r in read("analise-v3-resumo.csv") if r["hz"] == "100"]
fase7 = [r for r in read("analise-fase7-resumo.csv") if r["hz"] == "100" and r["modo"] == "direct"]

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 8,
    "axes.titlesize": 9,
    "axes.labelsize": 8,
    "legend.fontsize": 8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.edgecolor": CINZA,
    "axes.labelcolor": "#0b0b0b",
    "xtick.color": CINZA,
    "ytick.color": CINZA,
    "grid.color": "#e4e3df",
    "grid.linewidth": 0.6,
})

virgula = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}".replace(".", ","))


def painel(ax, titulo, yticks=None, ylim=None, xlabel=True):
    """Eixos iguais em todas as figuras: carga em log, mesmas marcas."""
    ax.set_xscale("log")
    ax.set_xticks(CARGAS)
    ax.set_xticklabels([str(c) for c in CARGAS])
    ax.xaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    ax.set_xlim(8, 500)
    if yticks:  # escala logaritmica com marcas explicitas
        ax.set_yscale("log")
        ax.set_yticks(yticks)
        ax.set_ylim(yticks[0] * 0.8, yticks[-1] * 1.25)
        ax.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    if ylim:
        ax.set_ylim(*ylim)
    ax.yaxis.set_major_formatter(virgula)
    ax.set_title(titulo, loc="left", fontweight="bold")
    if xlabel:
        ax.set_xlabel("carga oferecida (mil eventos/s)")
    ax.grid(axis="y")
    ax.set_axisbelow(True)


def linhas(ax, rows_of, field, scale=1.0, estilos=MODES, chave="modo"):
    """Uma linha por broker e por modo (ou perfil), sempre com o mesmo estilo."""
    for broker, nome, cor in BROKERS:
        for valor, rotulo, marcador, traco in estilos:
            rows = sorted((r for r in rows_of if r["broker"] == broker and r[chave] == valor),
                          key=lambda r: int(r["carga"]))
            if not rows:
                continue
            ax.plot([int(r["carga"]) / 1000 for r in rows], [num(r[field]) * scale for r in rows],
                    color=cor, linestyle=traco, linewidth=1.4, marker=marcador, markersize=4,
                    markeredgecolor="white", markeredgewidth=0.5, label=f"{nome} {rotulo}")


def limite_50ms(ax):
    ax.axhline(50, color=CINZA, linewidth=0.8, linestyle=(0, (1, 2)))
    ax.annotate("limite de 50 ms", xy=(0, 50), xycoords=("axes fraction", "data"),
                xytext=(3, 2), textcoords="offset points", ha="left", va="bottom",
                fontsize=6.5, color=CINZA)


def salva(fig, ax_legenda, nome, ncol=3):
    handles, labels = ax_legenda.get_legend_handles_labels()
    # Uma linha da legenda por broker: a legenda e preenchida por colunas, entao
    # as entradas sao intercaladas, com um espaco vazio onde um broker tem menos.
    por_broker = [[(h, l) for h, l in zip(handles, labels) if l.startswith(nome)]
                  for _, nome, _ in BROKERS]
    vazio = (matplotlib.lines.Line2D([], [], linestyle="none"), "")
    for linha in por_broker:
        linha.extend([vazio] * (ncol - len(linha)))
    pares = [linha[c] for c in range(ncol) for linha in por_broker]
    handles, labels = [h for h, _ in pares], [l for _, l in pares]
    fig.legend(handles, labels, loc="outside upper center", ncol=ncol, frameon=False,
               columnspacing=1.4, handlelength=2.6)
    fig.savefig(os.path.join(OUT, nome + ".pdf"))
    # PNG ao lado, so para conferir a figura sem compilar o texto.
    fig.savefig(os.path.join(OUT, nome + ".png"), dpi=200)
    plt.close(fig)
    print("escrito:", nome + ".pdf")


# --- latencia e throughput: as seis combinacoes ---------------------------------

fig, axes = plt.subplots(1, 3, figsize=(6.3, 2.75), constrained_layout=True)
linhas(axes[0], resumo, "p50_ms")
painel(axes[0], "Latência P50 (ms)", yticks=[0.1, 0.3, 1, 3, 10, 30, 100])
linhas(axes[1], resumo, "p99_ms")
painel(axes[1], "Latência P99 (ms)", yticks=[0.1, 0.3, 1, 3, 10, 30, 100])
limite_50ms(axes[1])
linhas(axes[2], resumo, "vazao_mediana", scale=0.001)
painel(axes[2], "Throughput (mil ev/s)", yticks=[10, 20, 40, 80, 200, 400])
salva(fig, axes[0], "fig_latencia_throughput")


# --- recursos: CPU e memoria do consumidor e do broker --------------------------

fig, axes = plt.subplots(2, 2, figsize=(6.3, 4.1), constrained_layout=True)
linhas(axes[0][0], resumo, "cpu_consumidor", scale=0.01)
painel(axes[0][0], "CPU do consumidor (núcleos)", ylim=(0, 2), xlabel=False)
linhas(axes[0][1], resumo, "cpu_broker", scale=0.01)
painel(axes[0][1], "CPU do broker (núcleos)", ylim=(0, 4), xlabel=False)
linhas(axes[1][0], resumo, "mem_consumidor_mb")
painel(axes[1][0], "Memória do consumidor (MB)", ylim=(0, 150))
linhas(axes[1][1], resumo, "mem_broker_mb", scale=0.001)
painel(axes[1][1], "Memória do broker (GB)", ylim=(0, 2))
salva(fig, axes[0][0], "fig_recursos")


# --- cenario ajustado: modo direto, padrao contra ajustado ----------------------

fig, axes = plt.subplots(1, 3, figsize=(6.3, 2.75), constrained_layout=True)
linhas(axes[0], fase7, "p50_ms", estilos=PROFILES, chave="perfil")
painel(axes[0], "Latência P50 (ms)", yticks=[0.1, 0.3, 1, 3, 10, 30, 100])
linhas(axes[1], fase7, "p99_ms", estilos=PROFILES, chave="perfil")
painel(axes[1], "Latência P99 (ms)", yticks=[0.1, 0.3, 1, 3, 10, 30, 100])
limite_50ms(axes[1])
linhas(axes[2], fase7, "cpu_broker", scale=0.01, estilos=PROFILES, chave="perfil")
painel(axes[2], "CPU do broker (núcleos)", ylim=(0, 4))
salva(fig, axes[1], "fig_ajustado")
