"""Monta a apresentacao da banca do TCC2 sobre o modelo da UTFPR.

Mantem a capa e o slide final do modelo, apaga os slides de instrucao e cria
os slides de conteudo sobre o layout "Somente titulo", com o rodape, os logos
e o titulo em vermelho do proprio modelo. Cada slide tem notas do orador com o
tempo sugerido (20 min no total)."""
import copy
from pathlib import Path

from lxml import etree
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml.ns import qn
from pptx.util import Inches, Pt

AQUI = Path(__file__).parent
MODELO = AQUI / "modelo.pptx"
SAIDA = AQUI / "Apresentacao_TCC2_Pedro_Augusto.pptx"

VERMELHO = RGBColor(0x8E, 0x29, 0x29)    # titulo do modelo
GRAFITE = RGBColor(0x26, 0x2B, 0x31)     # texto
CINZA = RGBColor(0x5E, 0x68, 0x72)       # texto secundario
BORDA = RGBColor(0xD5, 0xD8, 0xDC)
BRANCO = RGBColor(0xFF, 0xFF, 0xFF)
RABBIT = RGBColor(0xEB, 0x68, 0x34)      # mesmas cores dos graficos
KAFKA = RGBColor(0x2A, 0x78, 0xD6)
ROSADO = RGBColor(0xF6, 0xE9, 0xE9)      # fundo de destaque, vermelho bem claro

TITULO_TCC = ("Avaliação Experimental de Arquiteturas Compostas de Processamento "
              "de Eventos em Alta Frequência Utilizando .NET")

prs = Presentation(MODELO)
LAYOUT_TITULO = prs.slide_layouts[4]   # "Somente titulo"

# --- estrutura: guarda o numero de slide do modelo, apaga os slides de instrucao ---
modelo_slides = list(prs.slides)
sldnum_xml = None
for sh in modelo_slides[2].shapes:
    if sh.is_placeholder and sh.placeholder_format.idx == 12:
        sldnum_xml = copy.deepcopy(sh._element)
assert sldnum_xml is not None

lista = prs.slides._sldIdLst
ids = list(lista)
for sld_id in ids[1:13]:               # mantem o 1 (capa) e o 14 (encerramento)
    prs.part.drop_rel(sld_id.rId)
    lista.remove(sld_id)
capa, encerramento = prs.slides[0], prs.slides[1]
# os slides novos sao numerados slide3, slide4...: tira o encerramento do caminho
from pptx.opc.packuri import PackURI
encerramento.part.partname = PackURI("/ppt/slides/slide99.xml")


# --- ajudantes ----------------------------------------------------------------

def caixa(slide, x, y, w, h, paragrafos, tam=16, cor=GRAFITE, negrito=False,
          alinh=PP_ALIGN.LEFT, ancora=MSO_ANCHOR.TOP, espaco=6, nome=None):
    """paragrafos: lista de str ou de listas de (texto, {opcoes}) por paragrafo."""
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    if nome:
        tb.name = nome
    tf = tb.text_frame
    tf.word_wrap = True
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    tf.vertical_anchor = ancora
    for k, par in enumerate(paragrafos):
        p = tf.paragraphs[0] if k == 0 else tf.add_paragraph()
        p.alignment = alinh
        if k:
            p.space_before = Pt(espaco)
        runs = [(par, {})] if isinstance(par, str) else par
        for texto, op in runs:
            r = p.add_run()
            r.text = texto
            f = r.font
            f.size = Pt(op.get("tam", tam))
            f.bold = op.get("negrito", negrito)
            f.italic = op.get("italico", False)
            f.color.rgb = op.get("cor", cor)
    return tb


def bloco(slide, x, y, w, h, fundo=BRANCO, borda=BORDA, nome=None, raio=0.06):
    s = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    s.adjustments[0] = raio
    s.fill.solid()
    s.fill.fore_color.rgb = fundo
    if borda is None:
        s.line.fill.background()
    else:
        s.line.color.rgb = borda
        s.line.width = Pt(0.75)
    s.shadow.inherit = False
    if nome:
        s.name = nome
    return s


def circulo(slide, x, y, d, texto, fundo=VERMELHO, tam=16):
    c = slide.shapes.add_shape(MSO_SHAPE.OVAL, Inches(x), Inches(y), Inches(d), Inches(d))
    c.fill.solid()
    c.fill.fore_color.rgb = fundo
    c.line.fill.background()
    c.shadow.inherit = False
    tf = c.text_frame
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    r = p.add_run()
    r.text = texto
    r.font.size = Pt(tam)
    r.font.bold = True
    r.font.color.rgb = BRANCO
    return c


def seta(slide, x, y, w, h=0.32, cor=CINZA):
    s = slide.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW, Inches(x), Inches(y), Inches(w), Inches(h))
    s.fill.solid()
    s.fill.fore_color.rgb = cor
    s.line.fill.background()
    s.shadow.inherit = False
    return s


def imagem(slide, arquivo, x, y, w=None, h=None, alt=""):
    pic = slide.shapes.add_picture(str(AQUI / arquivo), Inches(x), Inches(y),
                                   Inches(w) if w else None, Inches(h) if h else None)
    pic._element.nvPicPr.cNvPr.set("descr", alt)
    return pic


def novo_slide(titulo, notas):
    s = prs.slides.add_slide(LAYOUT_TITULO)
    s.shapes.title.text = titulo
    # numero do slide, em branco sobre a faixa escura do rodape
    caixa(s, 6.17, 6.98, 1.0, 0.35, [str(len(prs.slides) - 1)], tam=14, cor=BRANCO,
          alinh=PP_ALIGN.CENTER, ancora=MSO_ANCHOR.MIDDLE, nome="Numero do slide")
    s.notes_slide.notes_text_frame.text = notas
    return s


def tabela(slide, x, y, w, larguras, linhas, tam=13, alt_linha=0.42, cab_cor=VERMELHO,
           cores_linha=None):
    t = slide.shapes.add_table(len(linhas), len(larguras), Inches(x), Inches(y), Inches(w),
                               Inches(alt_linha * len(linhas))).table
    for j, lw in enumerate(larguras):
        t.columns[j].width = Inches(lw)
    for i, linha in enumerate(linhas):
        t.rows[i].height = Inches(alt_linha)
        for j, valor in enumerate(linha):
            cel = t.cell(i, j)
            cel.margin_left = cel.margin_right = Inches(0.08)
            cel.margin_top = cel.margin_bottom = Inches(0.04)
            cel.vertical_anchor = MSO_ANCHOR.MIDDLE
            cel.fill.solid()
            if i == 0:
                cel.fill.fore_color.rgb = cab_cor
            else:
                cel.fill.fore_color.rgb = BRANCO if i % 2 else RGBColor(0xF3, 0xF4, 0xF6)
            tf = cel.text_frame
            tf.word_wrap = True
            p = tf.paragraphs[0]
            p.text = ""
            r = p.add_run()
            r.text = valor
            r.font.size = Pt(tam)
            r.font.bold = i == 0 or (j == 0 and cores_linha is not None)
            r.font.color.rgb = BRANCO if i == 0 else GRAFITE
            if i > 0 and j == 0 and cores_linha:
                r.font.color.rgb = cores_linha[i]
    return t


# --- 1. capa (do modelo) --------------------------------------------------------

def preenche_capa(s, notas):
    for sh in s.shapes:
        if not sh.has_text_frame:
            continue
        if sh.is_placeholder and sh.placeholder_format.idx == 0:
            tf = sh.text_frame
            tf.paragraphs[0].runs[0].text = TITULO_TCC
            for r in tf.paragraphs[0].runs[1:]:
                r.text = ""
            tf.paragraphs[0].runs[0].font.size = Pt(34)
        elif sh.is_placeholder and sh.placeholder_format.idx == 1:
            novos = ["Trabalho de Conclusão de Curso 2",
                     "Discente: Pedro Augusto Ramos de Sousa",
                     "Orientador: Prof. Dr. Evandro K."]
            for p, texto in zip(sh.text_frame.paragraphs, novos):
                p.runs[0].text = texto
                for r in p.runs[1:]:
                    r.text = ""
    s.notes_slide.notes_text_frame.text = notas


preenche_capa(capa, "[0:30] Bom dia/boa tarde. Sou o Pedro, e vou apresentar meu TCC: uma avaliação "
                    "experimental de arquiteturas de processamento de eventos em .NET, combinando "
                    "dois brokers de mensageria com mecanismos de concorrência internos.")

# --- 2. roteiro -------------------------------------------------------------------
s = novo_slide("Roteiro", "[0:20] Este é o roteiro: contexto e objetivos, os conceitos e trabalhos "
               "relacionados, a bancada e a metodologia, os resultados e a conclusão.")
etapas = ["Contexto e objetivos", "Fundamentação e trabalhos relacionados", "Bancada experimental",
          "Metodologia", "Resultados", "Conclusão e trabalhos futuros"]
for k, etapa in enumerate(etapas):
    col, lin = k % 3, k // 3
    x, y = 0.6 + col * 4.1, 2.15 + lin * 2.1
    bloco(s, x, y, 3.8, 1.7, nome=f"Etapa {k + 1}")
    circulo(s, x + 0.3, y + 0.5, 0.7, str(k + 1), tam=20)
    caixa(s, x + 1.2, y + 0.25, 2.4, 1.2, [etapa], tam=18, negrito=True, ancora=MSO_ANCHOR.MIDDLE)

# --- 3. contexto e problema ---------------------------------------------------------
s = novo_slide("Contexto e problema",
               "[1:30] Sistemas de telemetria, IoT e monitoramento recebem milhares de eventos por "
               "segundo. Em .NET, a arquitetura costuma combinar duas camadas: um broker de mensageria, "
               "RabbitMQ ou Kafka, e, dentro do consumidor, um mecanismo de concorrência: Channels ou "
               "Pipelines, que a Microsoft recomenda para produtor-consumidor. O problema é que as "
               "comparações de broker costumam ser isoladas e fora do .NET, e ninguém mede se o "
               "mecanismo interno ajuda ou só custa recursos. Daí as duas perguntas do trabalho.")
caixa(s, 0.6, 1.85, 12.1, 0.8,
      ["Telemetria, IoT e monitoramento processam milhares de eventos por segundo. Em .NET, a "
       "arquitetura costuma combinar duas camadas:"], tam=18, cor=GRAFITE)
camadas = [("Gerador de eventos", CINZA), ("Broker\nRabbitMQ ou Kafka", VERMELHO),
           ("Consumidor .NET\ncallback", CINZA), ("Mecanismo interno\nDirect, Channels\nou Pipelines", VERMELHO),
           ("Processamento", CINZA)]
x = 0.6
for k, (rot, cor) in enumerate(camadas):
    b = bloco(s, x, 2.9, 2.0, 1.35, fundo=BRANCO, borda=cor, nome=rot.split("\n")[0])
    b.line.width = Pt(2 if cor == VERMELHO else 0.75)
    caixa(s, x + 0.1, 2.9, 1.8, 1.35, [rot.replace("\n", " ")], tam=15, negrito=cor == VERMELHO,
          cor=VERMELHO if cor == VERMELHO else GRAFITE, alinh=PP_ALIGN.CENTER, ancora=MSO_ANCHOR.MIDDLE)
    if k < len(camadas) - 1:
        seta(s, x + 2.05, 3.42, 0.45)
    x += 2.52
bloco(s, 0.6, 4.65, 12.1, 1.85, fundo=ROSADO, borda=None, nome="Perguntas")
caixa(s, 0.95, 4.85, 11.4, 1.5,
      [[("Pergunta 1. ", {"negrito": True, "cor": VERMELHO}),
        ("Acrescentar Channels ou Pipelines ao consumidor melhora, piora ou não muda o desempenho?", {})],
       [("Pergunta 2. ", {"negrito": True, "cor": VERMELHO}),
        ("Qual broker atende melhor cada faixa de carga, e quanto disso vem da configuração?", {})]],
      tam=19, espaco=12)

# --- 4. objetivos -----------------------------------------------------------------
s = novo_slide("Objetivos",
               "[1:00] O objetivo geral é avaliar experimentalmente essas arquiteturas compostas. "
               "Para isso: implementei o consumidor nos três modos, sendo o Direct o grupo de controle; "
               "construí um gerador reproduzível em malha aberta com dados reais da Fórmula 1; "
               "configurei os brokers de forma equivalente, em dois cenários; e medi latência, "
               "throughput, recursos e o ponto de saturação.")
bloco(s, 0.6, 1.85, 12.1, 1.15, fundo=ROSADO, borda=None, nome="Objetivo geral")
caixa(s, 0.95, 1.95, 11.4, 0.95,
      [[("Objetivo geral: ", {"negrito": True, "cor": VERMELHO}),
        ("avaliar experimentalmente o desempenho de arquiteturas compostas de processamento de "
         "eventos em alta frequência em .NET, combinando mensageria e concorrência interna.", {})]],
      tam=18, ancora=MSO_ANCHOR.MIDDLE)
especificos = [
    ("Implementar", "consumidores para RabbitMQ e Kafka em três modos: Direct (grupo de controle), Channels e Pipelines"),
    ("Construir", "um gerador de carga reproduzível, em malha aberta, com dados reais da OpenF1"),
    ("Configurar", "os brokers de forma equivalente: valores de fábrica e recomendação de cada fornecedor"),
    ("Medir", "latência, throughput, CPU e memória, e o ponto de saturação de cada combinação"),
]
for k, (verbo, resto) in enumerate(especificos):
    col, lin = k % 2, k // 2
    x, y = 0.6 + col * 6.15, 3.3 + lin * 1.65
    bloco(s, x, y, 5.95, 1.45, nome=verbo)
    circulo(s, x + 0.25, y + 0.4, 0.65, str(k + 1), tam=18)
    caixa(s, x + 1.1, y + 0.15, 4.65, 1.15, [[(verbo + " ", {"negrito": True, "cor": VERMELHO}), (resto, {})]],
          tam=15, ancora=MSO_ANCHOR.MIDDLE)

# --- 5. fundamentacao -----------------------------------------------------------
s = novo_slide("Conceitos principais",
               "[2:00] Rapidamente, os quatro conceitos. O Kafka é baseado em log e trabalha em lotes; "
               "o RabbitMQ trata cada mensagem individualmente e, persistente, grava em disco antes de "
               "confirmar. Channels é uma fila entre threads de objetos; Pipelines é um fluxo de bytes "
               "com buffers reaproveitados. Os dois tiram o processamento da thread que recebe. Para "
               "medir, uso percentis, porque o que limita esses sistemas é a cauda, e um gerador em "
               "malha aberta, que não desacelera quando o sistema satura e não esconde atrasos.")
cartoes = [
    ("Kafka", KAFKA, "Log particionado. Agrupa mensagens em lotes e grava em sequência; ordem garantida por partição."),
    ("RabbitMQ", RABBIT, "Broker AMQP com filas. Trata cada mensagem individualmente; persistente, grava em disco antes de confirmar."),
    ("Channels", VERMELHO, "Fila thread-safe de objetos entre a thread que recebe e uma thread de trabalho."),
    ("Pipelines", VERMELHO, "Fluxo de bytes com buffers reaproveitados; o leitor interpreta os registros sem copiá-los."),
]
for k, (nome, cor, texto) in enumerate(cartoes):
    col, lin = k % 2, k // 2
    x, y = 0.6 + col * 4.05, 1.9 + lin * 2.25
    b = bloco(s, x, y, 3.85, 2.05, nome=nome)
    caixa(s, x + 0.3, y + 0.2, 3.3, 0.45, [nome], tam=20, negrito=True, cor=cor)
    caixa(s, x + 0.3, y + 0.72, 3.3, 1.25, [texto], tam=14)
bloco(s, 8.85, 1.9, 3.85, 4.3, fundo=ROSADO, borda=None, nome="Como medir")
caixa(s, 9.15, 2.1, 3.3, 3.95,
      [[("Como medir", {"negrito": True, "cor": VERMELHO, "tam": 20})],
       [("P50 e P99: ", {"negrito": True}), ("o P99 é o valor abaixo do qual ficam 99% dos eventos; a cauda é o que limita.", {})],
       [("Malha aberta: ", {"negrito": True}), ("o gerador envia no instante previsto, sem esperar o sistema, e não esconde atrasos (Tene, 2015).", {})],
       [("Ponto de saturação: ", {"negrito": True}), ("a maior carga sustentada.", {})]],
      tam=14, espaco=10)

# --- 6. trabalhos relacionados -----------------------------------------------------
s = novo_slide("Trabalhos relacionados",
               "[1:30] Estes são os cinco trabalhos mais próximos. Eles comparam os brokers, às vezes em "
               ".NET, ou propõem a metodologia de comparação justa que eu segui. A lacuna é que nenhum "
               "combina os dois brokers com Channels e Pipelines, e nenhum mede o mecanismo interno "
               "contra um grupo de controle sem ele, que é o papel do modo Direct neste trabalho.")
tabela(s, 0.6, 1.85, 12.1, [3.4, 5.0, 3.7], [
    ["Trabalho", "O que avalia", "O que fica de fora"],
    ["Kamiński et al. (2025)", "RabbitMQ × Kafka em Spring Boot e ASP.NET", "Mecanismo interno"],
    ["Umam et al. (2026)", "Escalabilidade e estresse dos dois brokers", ".NET e mecanismo interno"],
    ["Dobbelaere e Esmaili (2017)", "Semântica de entrega, throughput e latência", ".NET e mecanismo interno"],
    ["Fu, Zhang e Yu (2021)", "Metodologia de comparação justa entre filas", "Base da metodologia aqui"],
    ["Ortiz et al. (2023)", "Motor de eventos acoplado ao broker, em IoT", "Grupo de controle"],
], tam=15, alt_linha=0.5)
bloco(s, 0.6, 5.2, 12.1, 1.3, fundo=ROSADO, borda=None, nome="Lacuna")
caixa(s, 0.95, 5.3, 11.4, 1.1,
      [[("Lacuna: ", {"negrito": True, "cor": VERMELHO}),
        ("nenhum combina RabbitMQ e Kafka com Channels e Pipelines numa avaliação unificada, nem mede o "
         "mecanismo interno contra um grupo de controle sem ele.", {})]],
      tam=18, ancora=MSO_ANCHOR.MIDDLE)

# --- 7. bancada -----------------------------------------------------------------
s = novo_slide("Bancada experimental",
               "[2:00] Esta é a bancada, da esquerda para a direita. A telemetria da OpenF1 é baixada uma "
               "vez e reamostrada a 100 Hz, com a frota multiplicada para chegar a centenas de milhares "
               "de eventos por segundo. O gerador publica em malha aberta num broker, em quatro faixas, "
               "e cada carro fica sempre na mesma faixa, para preservar a ordem. No consumidor, o quadro "
               "de baixo mostra os três modos: no Direct, o callback processa ele mesmo; no Channels e "
               "no Pipelines, entrega a um leitor numa thread de trabalho e volta a receber. Os três "
               "chamam o mesmo código de processamento, então a diferença vem só da forma de entrega.")
imagem(s, "fig_bancada.png", 0.6, 1.75, h=4.85, alt="Arquitetura da bancada e os três modos de processamento")
itens = [("Dados", "OpenF1, Bahrein 2024, reamostrado a 100 Hz, frota multiplicada"),
         ("Gerador", "malha aberta, até 400 mil eventos/s"),
         ("Broker", "4 faixas; cada carro sempre na mesma faixa"),
         ("Consumidor", "mesmo processamento nos 3 modos: janelas de 1 s e detecção de frenagens"),
         ("Isolamento", "Docker com núcleos de CPU exclusivos por componente")]
y = 1.8
for k, (rot, texto) in enumerate(itens):
    circulo(s, 7.75, y + 0.05, 0.5, str(k + 1), tam=14)
    caixa(s, 8.45, y, 4.25, 0.9, [[(rot + ": ", {"negrito": True, "cor": VERMELHO}), (texto, {})]], tam=15)
    y += 0.98

# --- 8. metodologia -------------------------------------------------------------
s = novo_slide("Metodologia",
               "[2:00] São dois cenários de configuração. O padrão usa os valores de fábrica, ajustados só "
               "onde impediam a equivalência; o ajustado aplica a recomendação de cada fornecedor. O "
               "protocolo cruza dois brokers, três modos e várias cargas, com 10 repetições de 90 "
               "segundos. As regras de análise foram registradas antes da coleta: validade, descarte de "
               "discrepantes, ponto de saturação e o tamanho mínimo de diferença considerado relevante.")
for k, (nome, texto) in enumerate([
        ("Cenário padrão", "valores de fábrica, alterados só onde impediam a equivalência entre os brokers"),
        ("Cenário ajustado", "recomendação de cada fornecedor: Kafka com linger.ms 1 (e 20, para vazão); "
                             "RabbitMQ com mensagem transitória e prefetch 300")]):
    y = 1.85 + k * 1.55
    bloco(s, 0.6, y, 5.6, 1.4, nome=nome)
    caixa(s, 0.9, y + 0.15, 5.0, 1.15, [[(nome, {"negrito": True, "cor": VERMELHO, "tam": 18})], texto],
          tam=14, espaco=4)
numeros = [("6", "combinações\n2 brokers × 3 modos"), ("10", "repetições por\ncombinação e carga"),
           ("400 mil", "eventos/s, a maior\ncarga testada"), ("90 s", "de medição\npor rodada")]
for k, (num, rot) in enumerate(numeros):
    col, lin = k % 2, k // 2
    x, y = 6.5 + col * 3.1, 1.85 + lin * 1.55
    bloco(s, x, y, 2.95, 1.4, nome=f"Numero {num}")
    caixa(s, x + 0.2, y + 0.1, 2.55, 0.65, [num], tam=30, negrito=True, cor=VERMELHO, alinh=PP_ALIGN.CENTER)
    caixa(s, x + 0.2, y + 0.75, 2.55, 0.6, [rot.replace("\n", " ")], tam=13, cor=CINZA, alinh=PP_ALIGN.CENTER)
bloco(s, 0.6, 5.05, 12.1, 1.5, fundo=ROSADO, borda=None, nome="Regras")
caixa(s, 0.95, 5.15, 11.4, 0.4, ["Regras registradas antes da coleta"], tam=16, negrito=True, cor=VERMELHO)
for k, (rot, texto) in enumerate([("Rodada válida", "atraso de envio até 50 ms e o mesmo resultado nas 6 combinações"),
                                  ("Ponto de saturação", "maior carga com P99 abaixo de 50 ms"),
                                  ("Diferença relevante", "pelo menos 20% no P99")]):
    caixa(s, 0.95 + k * 3.85, 5.62, 3.6, 0.85, [[(rot, {"negrito": True})], texto], tam=14, espaco=2)

# --- 9. validade ------------------------------------------------------------------
s = novo_slide("Os dados são confiáveis",
               "[0:45] Antes dos resultados: das 390 rodadas do cenário padrão, nenhuma divergiu no "
               "resultado do processamento, ou seja, nenhum evento se perdeu, duplicou ou saiu de ordem. "
               "As 12 inválidas foram todas do Kafka em carga muito alta, por atraso do gerador, ligado "
               "a pausas de disco da máquina virtual. A regra de Tukey tirou cerca de 8% como discrepantes.")
grandes = [("390", "rodadas no cenário padrão", VERMELHO), ("0", "divergências de resultado entre as combinações", VERMELHO),
           ("12", "inválidas, todas do Kafka a 200 mil eventos/s ou mais", CINZA), ("8%", "descartadas como discrepantes (Tukey)", CINZA)]
for k, (num, rot, cor) in enumerate(grandes):
    x = 0.6 + k * 3.07
    bloco(s, x, 2.2, 2.85, 3.3, nome=f"Validade {num}")
    caixa(s, x + 0.2, 2.5, 2.45, 1.3, [num], tam=60, negrito=True, cor=cor, alinh=PP_ALIGN.CENTER,
          ancora=MSO_ANCHOR.MIDDLE)
    caixa(s, x + 0.25, 3.95, 2.35, 1.4, [rot], tam=16, alinh=PP_ALIGN.CENTER)
caixa(s, 0.6, 5.85, 12.1, 0.6, ["Nenhum evento perdido, duplicado ou fora de ordem em nenhuma combinação."],
      tam=17, cor=GRAFITE, negrito=True)


# --- 10 a 12. resultados ------------------------------------------------------------
def resultado(titulo, figura, alt, w_fig, h_fig, mensagens, notas):
    s = novo_slide(titulo, notas)
    imagem(s, figura, 0.6, 1.8, w=w_fig, h=h_fig, alt=alt)
    x = 0.6 + w_fig + 0.3
    w = 12.7 - x
    y = 1.85
    for rot, texto in mensagens:
        caixa(s, x, y, w, 1.5, [[(rot, {"negrito": True, "cor": VERMELHO})], texto], tam=15, espaco=2)
        y += 1.6
    return s


resultado("Latência e throughput", "fig_latencia_throughput.png",
          "Latência P50 e P99 e throughput das seis combinações", 8.7, 3.8,
          [("Os brokers se cruzam", "RabbitMQ mais rápido em carga baixa (P99 de 2,3 ms contra 5,8 ms a 10 mil eventos/s); Kafka estável até 400 mil."),
           ("Os modos se sobrepõem", "Direct, Channels e Pipelines têm as mesmas linhas: maior diferença no P99 de 11%."),
           ("Saturação", "RabbitMQ: 60 mil eventos/s. Kafka: 400 mil ou mais, nos três modos.")],
          "[2:00] Este é o resultado principal. Laranja é RabbitMQ, azul é Kafka, e o traço indica o "
          "modo. Entre os brokers, a latência muda de sinal: o RabbitMQ é mais rápido em carga baixa, "
          "mas a cauda cresce e ele satura em 60 mil; o Kafka tem um piso de uns 3 ms, que é a espera "
          "para formar lotes, mas fica estável até 400 mil. Entre os modos, as linhas ficam uma em cima "
          "da outra. O throughput é igual em todos porque o gerador é de malha aberta: o que distingue "
          "é até onde cada um sustenta, e o modo não muda isso.")

resultado("Uso de recursos", "fig_recursos.png", "CPU e memória do consumidor e do broker", 7.3, 4.75,
          [("CPU do consumidor", "É a única medida em que os modos se separam: Channels e Pipelines custam de 1,9 a 3,3 vezes a CPU do Direct no Kafka."),
           ("CPU do broker", "Explica a saturação: o RabbitMQ chega a 3,5 dos 4 núcleos; o Kafka fica em 0,5."),
           ("Memória", "Não depende do modo; o broker Kafka usa de 1,4 a 1,7 GB, contra 0,2 a 0,3 GB.")],
          "[1:30] Nos recursos, a CPU do consumidor é a única medida em que os modos se separam, e "
          "para pior: no Kafka, Channels e Pipelines custam até 3,3 vezes a CPU do Direct, porque passar "
          "cada evento para outra thread custa mais que processá-lo. A CPU do broker explica a "
          "saturação: o RabbitMQ esgota os 4 núcleos, o Kafka quase não usa. A memória não depende do "
          "modo; o Kafka troca CPU por memória.")

resultado("Cenário ajustado", "fig_ajustado.png", "Modo Direct no cenário padrão e no ajustado", 8.7, 3.8,
          [("RabbitMQ", "Sem gravação em disco, P99 de 43% a 76% menor, e saturação de 60 para 80 mil eventos/s."),
           ("Kafka", "linger.ms 1: P50 de 3 para 1 ms, mas P99 maior em carga alta e o dobro de CPU no broker."),
           ("Mecanismos", "Continuaram sem diferença de latência: a configuração muda os brokers, não os modos.")],
          "[1:30] No cenário ajustado, a configuração mudou muito os brokers. O RabbitMQ sem gravação "
          "em disco reduziu o P99 em até 76% e passou a sustentar 80 mil. No Kafka, reduzir a espera "
          "de lote derrubou a mediana para 1 ms, mas aumentou a cauda e dobrou a CPU do broker. Ou "
          "seja, parte da diferença entre os brokers é configuração. Os mecanismos continuaram sem "
          "diferença.")

# --- 13. sintese ------------------------------------------------------------------
s = novo_slide("Onde cada combinação ganha",
               "[1:30] Juntando tudo: entre os brokers, o RabbitMQ ganha em carga baixa e o Kafka em carga "
               "alta. Dentro de cada broker, o Direct é a melhor escolha neste cenário. Channels e "
               "Pipelines só chegaram perto de um ganho no RabbitMQ a 40 mil, sem relevância, e no Kafka "
               "nunca ajudaram e custaram caro.")
tabela(s, 0.6, 1.85, 12.1, [1.6, 1.6, 3.0, 3.3, 2.6], [
    ["Broker", "Modo", "Onde se sai melhor", "O que ganha", "O que custa"],
    ["RabbitMQ", "Direct", "Carga baixa", "Menor latência: P99 de 2,3 ms", "Satura em 60 mil"],
    ["RabbitMQ", "Channels", "40 mil eventos/s", "P99 6% menor, sem relevância", "CPU +4% a 7%"],
    ["RabbitMQ", "Pipelines", "40 mil eventos/s", "P99 6% menor, sem relevância", "CPU +6% a 9%"],
    ["Kafka", "Direct", "Carga alta, até 400 mil", "P99 estável, broker leve", "P50 de 3 ms; memória"],
    ["Kafka", "Channels", "Nenhuma carga", "Nada", "CPU de 1,9 a 3,0×"],
    ["Kafka", "Pipelines", "Nenhuma carga", "Nada", "CPU de 2,0 a 3,3×"],
], tam=15, alt_linha=0.56,
    cores_linha=[None, RABBIT, RABBIT, RABBIT, KAFKA, KAFKA, KAFKA])
caixa(s, 0.6, 6.0, 12.1, 0.5, ["O broker decide o desempenho; o mecanismo interno decide só o custo."],
      tam=17, negrito=True, cor=VERMELHO)

# --- 14. conclusao --------------------------------------------------------------
s = novo_slide("Conclusão",
               "[1:30] Respondendo às perguntas. Primeira: nesta carga, Channels e Pipelines não fizeram "
               "diferença no desempenho e pioraram o custo. O motivo é que o processamento leva 0,12 "
               "microssegundo por evento e quem limita é o broker: não há trabalho acumulado para "
               "paralelizar. Segunda: o RabbitMQ atende carga baixa com latência mínima, o Kafka atende "
               "carga alta, e a configuração muda bastante esse quadro. E um estudo preliminar indica que, "
               "quando o processamento espera algo, como republicar o resultado no broker, o mecanismo "
               "passa a fazer diferença.")
bloco(s, 0.6, 1.85, 12.1, 1.25, fundo=ROSADO, borda=None, nome="Veredito")
caixa(s, 0.95, 1.95, 11.4, 1.05,
      [[("Channels e Pipelines não fizeram diferença no desempenho e pioraram o custo. ", {"negrito": True, "cor": VERMELHO}),
        ("O processamento custa 0,12 µs por evento: quem limita é o broker.", {})]],
      tam=18, ancora=MSO_ANCHOR.MIDDLE)
colunas = [("Brokers", "RabbitMQ para carga baixa com latência mínima; Kafka para carga alta, com menos CPU."),
           ("Configuração", "Um parâmetro muda o resultado em até 76%: a comparação exige configurações auditadas."),
           ("Método", "Efeitos do ambiente foram maiores que as diferenças: núcleos exclusivos e regras prévias.")]
for k, (rot, texto) in enumerate(colunas):
    x = 0.6 + k * 4.1
    bloco(s, x, 3.35, 3.9, 1.65, nome=rot)
    caixa(s, x + 0.25, 3.5, 3.4, 1.4, [[(rot, {"negrito": True, "cor": VERMELHO, "tam": 18})], texto], tam=14, espaco=4)
bloco(s, 0.6, 5.25, 12.1, 1.3, nome="Estudo preliminar")
caixa(s, 0.95, 5.35, 3.3, 1.1, [[("64 mil", {"negrito": True, "cor": KAFKA, "tam": 30})], "Direct, eventos/s"],
      tam=13, alinh=PP_ALIGN.CENTER, espaco=0)
caixa(s, 4.0, 5.35, 3.3, 1.1, [[("200 mil", {"negrito": True, "cor": KAFKA, "tam": 30})], "Channels e Pipelines"],
      tam=13, alinh=PP_ALIGN.CENTER, espaco=0)
caixa(s, 7.6, 5.35, 4.85, 1.1,
      [[("Estudo preliminar: ", {"negrito": True}), ("com o consumidor esperando a confirmação de uma "
        "republicação no Kafka, o mecanismo passa a fazer diferença.", {})]],
      tam=14, ancora=MSO_ANCHOR.MIDDLE)

# --- 15. trabalhos futuros --------------------------------------------------------
s = novo_slide("Limitações e trabalhos futuros",
               "[1:00] As principais limitações: tudo roda numa máquina, com um nó por broker, e a carga "
               "tem processamento leve, com mensagens pequenas. Daí os trabalhos futuros: aprofundar o "
               "estudo preliminar com processamento mais pesado e mais leitores por faixa, testar vários "
               "nós com replicação, e avaliar outros tipos de fila e o uso em lote.")
for k, (rot, itens_lista) in enumerate([
        ("Limitações", ["Uma máquina, um nó por broker, sem replicação",
                        "Processamento leve, 0,12 µs por evento",
                        "Mensagens de 46 bytes, um evento por vez"]),
        ("Trabalhos futuros", ["Processamento mais pesado e mais leitores por faixa",
                               "Vários nós, com replicação",
                               "Filas de quórum e streams do RabbitMQ",
                               "Channels e Pipelines em lote"])]):
    x = 0.6 + k * 6.15
    bloco(s, x, 1.9, 5.95, 4.6, fundo=ROSADO if k else BRANCO, borda=None if k else BORDA, nome=rot)
    caixa(s, x + 0.35, 2.1, 5.3, 0.5, [rot], tam=20, negrito=True, cor=VERMELHO)
    for j, item in enumerate(itens_lista):
        circulo(s, x + 0.35, 2.85 + j * 0.88, 0.42, str(j + 1), tam=13)
        caixa(s, x + 0.95, 2.82 + j * 0.88, 4.75, 0.8, [item], tam=16, ancora=MSO_ANCHOR.TOP)

# --- 16. referencias --------------------------------------------------------------
s = novo_slide("Referências", "[0:10] As referências citadas na apresentação.")
refs = [
    "DOBBELAERE, P.; ESMAILI, K. S. Kafka versus RabbitMQ: a comparative study of two industry reference "
    "publish/subscribe implementations. In: ACM DEBS, 11., 2017. p. 227–238.",
    "FU, G.; ZHANG, Y.; YU, G. A fair comparison of message queuing systems. IEEE Access, v. 9, p. 421–432, 2021.",
    "KAMIŃSKI, F.; KŁONICA, R.; PAŃCZYK, B. Comparative performance analysis of RabbitMQ and Kafka message "
    "queue systems in Spring Boot and ASP.NET environments. Journal of Computer Sciences Institute, v. 37, p. 457–462, 2025.",
    "MICROSOFT. System.Threading.Channels e System.IO.Pipelines. Documentação do .NET, 2023.",
    "OPENF1 CONTRIBUTORS. OpenF1: free and open-source F1 data API, 2024. Disponível em: https://openf1.org.",
    "ORTIZ, G. et al. Evaluating the integration of Esper complex event processing engine and message brokers. "
    "PeerJ Computer Science, v. 9, e1437, 2023.",
    "TENE, G. How NOT to measure latency. Palestra, 2015. Disponível em: https://www.infoq.com/presentations/latency-response-time/.",
    "UMAM, A. M.; RIAS, M.; ASMANI, A. A comparative study evaluation of Kafka and RabbitMQ: performance, "
    "scalability and stress test in distributed messaging systems. JUTI, v. 24, n. 1, 2026.",
]
caixa(s, 0.6, 1.8, 12.1, 4.9, refs, tam=14, espaco=8)

# --- encerramento: ultimo slide do modelo, depois das referencias ----------------
preenche_capa(encerramento, "Obrigado. Fico à disposição para as perguntas da banca.")
for sh in encerramento.shapes:
    if sh.is_placeholder and sh.placeholder_format.idx == 1:
        p0 = sh.text_frame.paragraphs[0]
        p0.runs[0].text = "Obrigado! Perguntas?"
ids = list(lista)
lista.remove(ids[1])
lista.append(ids[1])
encerramento.part.partname = PackURI("/ppt/slides/slide2.xml")

prs.save(SAIDA)
print("salvo:", SAIDA, "slides:", len(prs.slides))
