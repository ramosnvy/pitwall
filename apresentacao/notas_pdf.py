"""Gera um PDF A4 com uma página por slide: miniatura do slide em cima e as
notas do apresentador embaixo. Uso: python notas_pdf.py deck.pdf notas.py saida.pdf"""
import html
import importlib.util
import re
import sys

import pymupdf

deck_pdf, notas_py, saida = sys.argv[1:4]
spec = importlib.util.spec_from_file_location("notas", notas_py)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
NOTAS = mod.NOTAS

deck = pymupdf.open(deck_pdf)
assert deck.page_count == len(NOTAS), (deck.page_count, len(NOTAS))
rotulos = [str(i) for i in range(1, 19)] + ["A1", "A2"]

CSS = """
* { font-family: sans-serif; }
p { margin: 0 0 5pt 0; font-size: 11.5pt; line-height: 1.35; color: #262B31; }
p.tempo { font-size: 15pt; font-weight: bold; color: #8E2929; margin-bottom: 6pt; }
p.opc { color: #7A828A; font-style: italic; }
span.dica { color: #2A78D6; font-weight: bold; font-size: 10.5pt; }
span.opc-tag { color: #9AA1A8; font-size: 9.5pt; font-weight: bold; font-style: normal; }
"""


def formatar(texto):
    partes = []
    for linha in texto.split("\n"):
        if not linha.strip():
            continue
        if re.fullmatch(r"\[\d:\d\d\]", linha.strip()):
            partes.append(f'<p class="tempo">Tempo: {linha.strip()[1:-1]}</p>')
            continue
        classe = ""
        if linha.startswith("[se der tempo]"):
            classe = ' class="opc"'
            linha = linha[len("[se der tempo]"):].strip()
            corpo = '<span class="opc-tag">SE DER TEMPO · </span>' + html.escape(linha)
        else:
            corpo = html.escape(linha)
            corpo = re.sub(r"\[([^\]]+)\]", r'<span class="dica">[\1]</span>', corpo)
        partes.append(f"<p{classe}>{corpo}</p>")
    return "".join(partes)


out = pymupdf.open()
W, H = pymupdf.paper_size("a4")
M = 36
for k in range(deck.page_count):
    pg = out.new_page(width=W, height=H)
    # cabecalho
    pg.insert_text((M, M - 8), f"Notas do apresentador · slide {rotulos[k]}", fontsize=9, color=(0.45, 0.48, 0.52))
    # miniatura do slide
    src = deck[k].rect
    w_img = (W - 2 * M) * 0.62
    h_img = w_img * src.height / src.width
    caixa_img = pymupdf.Rect(M, M, M + w_img, M + h_img)
    pg.show_pdf_page(caixa_img, deck, k)
    pg.draw_rect(caixa_img, color=(0.83, 0.85, 0.86), width=0.6)
    # notas
    area = pymupdf.Rect(M, M + h_img + 14, W - M, H - M)
    pg.insert_htmlbox(area, formatar(NOTAS[k]), css=CSS, scale_low=0.6)
    pg.insert_text((W - M - 40, H - 18), f"{k + 1}/{deck.page_count}", fontsize=8, color=(0.6, 0.6, 0.6))

out.save(saida, garbage=3, deflate=True)
print("salvo:", saida, "paginas:", out.page_count)
