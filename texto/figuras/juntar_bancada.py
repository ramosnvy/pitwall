"""Gera bancada.html: a Figura da arquitetura em cima e, embaixo, o detalhe
da etapa 4 com os tres modos (modos.html), ligados por linhas de zoom. As
duas fontes continuam sendo arquitetura.html e modos.html; rode este script
de novo depois de mudar qualquer uma delas, e depois exportar.ps1 -Nomes bancada.
"""
import re
from pathlib import Path

aqui = Path(__file__).parent


def partes(nome):
    html = (aqui / nome).read_text(encoding='utf-8')
    css = re.search(r'<style>(.*?)</style>', html, re.S).group(1)
    corpo = re.search(r'<svg[^>]*>(.*?)</svg>', html, re.S).group(1)
    return css, corpo


css_a, svg_a = partes('arquitetura.html')
css_m, svg_m = partes('modos.html')

# Os ids do detalhe ganham prefixo, para nao colidir com os da arquitetura.
svg_m = re.sub(r'id="([^"]+)"', r'id="m-\1"', svg_m)
svg_m = re.sub(r'href="#([^"]+)"', r'href="#m-\1"', svg_m)
svg_m = re.sub(r'url\(#([^)]+)\)', r'url(#m-\1)', svg_m)


def regras(css):
    # so as regras de classe; :root, @page, html e svg ficam definidos aqui
    return [r.strip() for r in re.findall(r'\n(\.[^{]+\{[^}]*\})', css)]


vistas, extra = set(), []
for r in regras(css_a) + regras(css_m):
    seletor = r.split('{')[0].strip()
    if seletor not in vistas:
        vistas.add(seletor)
        extra.append(r)

ESCALA = 0.97
DY = 246                       # topo do detalhe
ALTURA = round(DY + 262 * ESCALA + 8)

html = f"""<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8">
<title>Figura 1</title>
<!-- Gerado por texto/figuras/juntar_bancada.py a partir de arquitetura.html e
     modos.html; nao edite a mao. PDF e PNG: texto/figuras/exportar.ps1. -->
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700&family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;600&display=block">
<style>
:root {{
  --ink: #17202A; --ink-2: #3A4550; --muted: #5E6872; --white: #FFFFFF;
  --data: #5E6872; --prod: #2B5797; --brk: #A5620F; --cons: #2D7552; --infra: #3A4550;
  --cli: #2D7552; --wrk: #2B5797; --fila: #7A848D;
  --display: "Barlow Condensed", "Arial Narrow", sans-serif;
  --sans: "IBM Plex Sans", "Segoe UI", sans-serif;
  --mono: "IBM Plex Mono", Consolas, monospace;
}}
@page {{ size: 720px {ALTURA}px; margin: 0; }}
html, body {{ margin: 0; padding: 0; background: var(--white); }}
svg {{ display: block; width: 720px; height: {ALTURA}px; }}
{chr(10).join(extra)}
.zoom {{ fill: none; stroke: var(--cons); stroke-width: 1; stroke-dasharray: 4 3; }}
.det {{ fill: #F7FAF8; stroke: var(--cons); stroke-width: 1.1; stroke-dasharray: 5 3; }}
.det-t {{ font: 600 10px var(--mono); letter-spacing: .06em; fill: var(--cons); }}
</style></head><body>
<svg viewBox="0 0 720 {ALTURA}" role="img" aria-label="Figura 1: arquitetura da bancada em cinco etapas, da OpenF1 ao PostgreSQL, e, embaixo, o detalhe da etapa de processamento com os três modos de entrega numa faixa: Direct, Channels e Pipelines.">
<!-- Arquitetura -->
{svg_a}
<!-- Zoom da etapa 4 para o detalhe -->
<path class="zoom" d="M318 200 L10 {DY - 22}"/>
<path class="zoom" d="M596 200 L710 {DY - 22}"/>
<rect class="det" x="4" y="{DY - 22}" width="712" height="{ALTURA - DY + 18}" rx="8"/>
<text class="det-t" x="14" y="{DY - 8}">ETAPA 4 EM DETALHE · UMA FAIXA</text>
<g transform="translate(8 {DY}) scale({ESCALA})">
{svg_m}
</g>
</svg>
</body></html>
"""
(aqui / 'bancada.html').write_text(html, encoding='utf-8', newline='\n')
print('bancada.html', ALTURA)
