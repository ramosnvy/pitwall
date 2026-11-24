"""Monta a apresentacao da banca do TCC2 sobre o modelo da UTFPR.

Mantem a capa e o slide final do modelo, apaga os slides de instrucao e cria
os slides de conteudo sobre o layout "Somente titulo", com o rodape, os logos
e o titulo em vermelho do proprio modelo. As notas do apresentador ficam em
notas.py e substituem, no fim, as escritas junto de cada slide."""
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


def novo_slide(titulo, notas, num=None):
    s = prs.slides.add_slide(LAYOUT_TITULO)
    s.shapes.title.text = titulo
    # numero do slide, em branco sobre a faixa escura do rodape
    # (o encerramento fica na lista durante a montagem, por isso o -1)
    caixa(s, 6.17, 6.98, 1.0, 0.35, [num or str(len(prs.slides) - 1)], tam=14, cor=BRANCO,
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
                    "dois brokers de mensageria com mecanismos de concorrência internos. "
                    "(Tempo total previsto: cerca de 18 min e 30 s, com folga para os 20.)")

# --- 2. contexto e problema ---------------------------------------------------------
s = novo_slide("Contexto e problema",
               "[1:15] Sistemas de telemetria, IoT e monitoramento recebem milhares de eventos por "
               "segundo. Em .NET, a arquitetura costuma combinar duas camadas: um broker de mensageria, "
               "RabbitMQ ou Kafka, e, dentro do consumidor, um mecanismo de concorrência: Channels ou "
               "Pipelines, que a Microsoft recomenda para produtor-consumidor. O problema é que as "
               "comparações de broker costumam ser isoladas e fora do .NET, e ninguém mede se o "
               "mecanismo interno ajuda ou só custa recursos. E uma comparação de brokers só é justa com "
               "ambiente e carga controlados; sem isso, mede-se a máquina, e não a arquitetura. Daí as "
               "duas perguntas do trabalho.")
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

# --- 3. objetivos -----------------------------------------------------------------
s = novo_slide("Objetivos",
               "[0:45] O objetivo geral é avaliar experimentalmente essas arquiteturas compostas. "
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

# --- 4. fundamentacao -----------------------------------------------------------
s = novo_slide("Conceitos principais",
               "[1:15] Rapidamente, os quatro conceitos. O Kafka é baseado em log e trabalha em lotes; "
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

# --- 5. trabalhos relacionados -----------------------------------------------------
s = novo_slide("Trabalhos relacionados",
               "[0:50] Estes são os cinco trabalhos mais próximos. Eles comparam os brokers, às vezes em "
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

# --- 6. bancada -----------------------------------------------------------------
s = novo_slide("Bancada experimental",
               "[1:30] Esta é a bancada, da esquerda para a direita. A telemetria da OpenF1 é baixada uma "
               "vez e reamostrada a 100 Hz, com a frota multiplicada para chegar a centenas de milhares "
               "de eventos por segundo sem comprimir o tempo. O gerador publica em malha aberta num broker, "
               "em quatro faixas, e cada carro fica sempre na mesma faixa, pela mesma função nos dois "
               "brokers. Há um único leitor por faixa porque dois leitores poderiam processar fora de ordem "
               "os eventos de um carro, e a detecção de frenagens depende dessa ordem; o paralelismo vem das "
               "quatro faixas. No consumidor, o quadro de baixo mostra os três modos: no Direct, o callback "
               "processa ele mesmo; no Channels e no Pipelines, entrega a um leitor numa thread de trabalho e "
               "volta a receber. Os três chamam o mesmo código de processamento, e cada rodada gera um "
               "digest do resultado: se as seis combinações não produzirem o mesmo digest, houve perda, "
               "duplicação ou quebra de ordem.")
imagem(s, "fig_bancada.png", 0.6, 1.75, h=4.85, alt="Arquitetura da bancada e os três modos de processamento")
itens = [("Dados", "OpenF1, Bahrein 2024, reamostrado a 100 Hz, frota multiplicada"),
         ("Gerador", "malha aberta, até 400 mil eventos/s"),
         ("Broker", "4 faixas pela mesma função (CRC32); um leitor por faixa, para manter a ordem de cada carro"),
         ("Consumidor", "mesmo processamento nos 3 modos: janelas de 1 s e detecção de frenagens"),
         ("Conferência", "digest do resultado: tem de ser igual nas 6 combinações")]
y = 1.8
for k, (rot, texto) in enumerate(itens):
    circulo(s, 7.75, y + 0.05, 0.5, str(k + 1), tam=14)
    caixa(s, 8.45, y, 4.25, 0.9, [[(rot + ": ", {"negrito": True, "cor": VERMELHO}), (texto, {})]], tam=15)
    y += 0.98

# --- 7. ambiente e medicao -------------------------------------------------------
s = novo_slide("Ambiente e medição",
               "[0:45] Tudo roda numa máquina, em Docker, e cada componente tem núcleos de CPU exclusivos: "
               "o gerador não disputa CPU com o broker nem com o consumidor. Durante a medição, só o broker "
               "medido fica ligado. A latência vai do envio ao fim do processamento: o gerador grava na "
               "mensagem o instante da publicação, e o consumidor compara com o mesmo relógio monotônico; "
               "como tudo está na mesma máquina, não é preciso sincronizar relógios. CPU e memória vêm do "
               "Prometheus com cAdvisor, por container. Os resultados vão para o PostgreSQL em lote, e a "
               "latência termina antes da gravação, então o banco não entra na medida.")
bloco(s, 0.6, 1.8, 5.95, 4.15, nome="Ambiente")
caixa(s, 0.9, 1.95, 5.4, 0.45, ["Ambiente"], tam=19, negrito=True, cor=VERMELHO)
caixa(s, 0.9, 2.45, 5.4, 0.8, ["Intel Core i5-12400F (6 núcleos, 12 threads), 16 GB; Windows 11, "
                                "Docker sobre WSL2"], tam=14)
caixa(s, 0.9, 3.3, 5.4, 0.35, ["Núcleos exclusivos por componente:"], tam=14, negrito=True)
grupos = [("Gerador", 4, CINZA), ("Broker", 4, VERMELHO), ("Consumidor", 3, KAFKA),
          ("Banco e métricas", 1, RGBColor(0x9A, 0xA1, 0xA8))]
nucleo = 0
xq = 0.9
for nome_g, qtd, cor in grupos:
    for _ in range(qtd):
        q = bloco(s, xq, 3.75, 0.4, 0.4, fundo=cor, borda=None, raio=0.15, nome=f"Núcleo {nucleo}")
        caixa(s, xq, 3.75, 0.4, 0.4, [str(nucleo)], tam=11, cor=BRANCO, negrito=True,
              alinh=PP_ALIGN.CENTER, ancora=MSO_ANCHOR.MIDDLE)
        xq += 0.45
        nucleo += 1
legenda = [[(f"■ {n}  ", {"cor": c, "negrito": True})] for n, _, c in grupos]
caixa(s, 0.9, 4.3, 5.4, 0.4, [[r for par in legenda for r in par]], tam=12)
caixa(s, 0.9, 4.85, 5.4, 1.0, ["Só o broker medido fica ligado; o computador não é usado durante as medições."],
      tam=14, cor=CINZA)
bloco(s, 6.75, 1.8, 5.95, 4.15, fundo=ROSADO, borda=None, nome="Medição")
caixa(s, 7.05, 1.95, 5.4, 3.9,
      [[("Como cada métrica foi medida", {"negrito": True, "cor": VERMELHO, "tam": 19})],
       [("Latência: ", {"negrito": True}), ("do envio ao fim do processamento, pelo mesmo relógio "
                                             "monotônico; mesma máquina, sem sincronizar relógios; HdrHistogram.", {})],
       [("CPU e memória: ", {"negrito": True}), ("Prometheus e cAdvisor, por container.", {})],
       [("Alocação e coletas de lixo: ", {"negrito": True}), ("contadores do runtime do .NET.", {})],
       [("Resultados: ", {"negrito": True}), ("PostgreSQL em lote (COPY binário); a latência termina "
                                               "antes da gravação, e o banco não é gargalo.", {})]],
      tam=14, espaco=9)
caixa(s, 0.6, 6.15, 12.1, 0.5,
      [[("Versões: ", {"negrito": True, "cor": VERMELHO}),
        (".NET 10 · Kafka 4.1.0 · RabbitMQ 4.3.6 (filas clássicas) · Confluent.Kafka 2.15.1 · "
         "RabbitMQ.Client 7.2.2 · PostgreSQL 17 · HdrHistogram 2.5.0", {})]], tam=13)

# --- 8. metodologia -------------------------------------------------------------
s = novo_slide("Metodologia",
               "[1:15] São dois cenários de configuração. O padrão usa os valores de fábrica, conferidos no "
               "código-fonte de cada versão, alterados só onde impediam a equivalência. Uma diferença de "
               "garantia fica de propósito, porque é o padrão de cada um: o RabbitMQ, com mensagem "
               "persistente, grava em disco antes de confirmar, e o Kafka confirma com a mensagem no cache, "
               "como a própria documentação dele recomenda. Isso favorece o Kafka no cenário padrão. O "
               "cenário ajustado aplica a recomendação de cada fornecedor e, com a mensagem transitória, "
               "iguala as garantias. O protocolo cruza dois brokers, três modos e várias cargas, com 10 "
               "repetições de 90 segundos em ordem sorteada. As regras foram registradas antes da coleta.")
for k, (nome, texto) in enumerate([
        ("Cenário padrão", "valores de fábrica, alterados só pela equivalência. O RabbitMQ grava em disco "
                           "antes de confirmar e o Kafka não: favorece o Kafka"),
        ("Cenário ajustado", "recomendação de cada fornecedor: Kafka com linger.ms 1 (e 20, para vazão); "
                             "RabbitMQ transitório e prefetch 300: mesma garantia nos dois")]):
    y = 1.85 + k * 1.55
    bloco(s, 0.6, y, 5.6, 1.4, nome=nome)
    caixa(s, 0.9, y + 0.12, 5.0, 1.2, [[(nome, {"negrito": True, "cor": VERMELHO, "tam": 17})], texto],
          tam=13, espaco=3)
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
for k, (rot, texto) in enumerate([("Rodada válida", "atraso de envio até 50 ms e o mesmo digest nas 6 combinações"),
                                  ("Ponto de saturação", "maior carga com P99 abaixo de 50 ms"),
                                  ("Diferença relevante", "20% no P99 ou 10% no throughput; Kruskal-Wallis a 5%")]):
    caixa(s, 0.95 + k * 3.85, 5.62, 3.6, 0.85, [[(rot, {"negrito": True})], texto], tam=14, espaco=2)

# --- 9. comparacao justa --------------------------------------------------------
s = novo_slide("Como garanti uma comparação justa",
               "[1:30] Cada decisão do desenho tem um motivo e uma base. Primeiro, a mesma carga e o mesmo "
               "trabalho: os mesmos dados e o mesmo código de processamento nos três modos, a mesma divisão "
               "em faixas nos dois brokers, e a fila interna do Channels e do Pipelines com o mesmo volume; "
               "o digest prova que todos fizeram o mesmo trabalho. Segundo, a mesma garantia: at-least-once "
               "nos dois, com acks=all no Kafka equivalente às confirmações em lote do RabbitMQ, e valores "
               "de fábrica conferidos na fonte, para não ajustar nenhum a favor. A compressão foi desligada "
               "porque as réplicas da frota são quase idênticas e isso favoreceria o Kafka. Terceiro, as "
               "mesmas condições: núcleos exclusivos, só o broker medido ligado, ordem sorteada e malha "
               "aberta. Quarto, a análise: regras fixadas antes da coleta, como recomendam Jain e Fu, e o "
               "modo Direct como grupo de controle, que é o que permite dizer se o mecanismo ajudou. Cada "
               "decisão vem da documentação do fornecedor, da literatura ou de uma medição da calibração; "
               "o slide de apoio no fim tem a lista completa.")
cartoes = [
    ("Mesma carga e mesmo trabalho", [
        "Mesmos dados e mesmo código de processamento nos 3 modos; o digest confere o resultado",
        "4 faixas pela mesma função nos dois brokers",
        "Channels e Pipe com o mesmo volume: 10 mil eventos = 460 kB"]),
    ("Mesma garantia de entrega", [
        "At-least-once nos dois: acks=all no Kafka ↔ confirmações do RabbitMQ",
        "Valores de fábrica conferidos na fonte; mudados só pela equivalência",
        "Compressão desligada: a carga repetida favoreceria o Kafka"]),
    ("Mesmas condições de execução", [
        "Núcleos exclusivos por componente; só o broker medido ligado",
        "Ordem das rodadas sorteada; 10 s de aquecimento descartados",
        "Gerador em malha aberta: não alivia quando o sistema atrasa"]),
    ("Análise sem viés", [
        "Regras e limiar de relevância fixados antes da coleta",
        "Direct como grupo de controle: isola o efeito do mecanismo",
        "10 repetições, regra de Tukey e mediana"]),
]
for k, (titulo, itens_c) in enumerate(cartoes):
    col, lin = k % 2, k // 2
    x, y = 0.6 + col * 6.15, 1.8 + lin * 2.15
    bloco(s, x, y, 5.95, 2.0, nome=titulo)
    caixa(s, x + 0.3, y + 0.12, 5.4, 0.4, [titulo], tam=17, negrito=True, cor=VERMELHO)
    caixa(s, x + 0.3, y + 0.55, 5.4, 1.4, ["• " + t for t in itens_c], tam=13, espaco=4)
bloco(s, 0.6, 6.1, 12.1, 0.62, fundo=ROSADO, borda=None, nome="Base das decisoes")
caixa(s, 0.9, 6.1, 11.5, 0.62,
      [[("Base de cada decisão: ", {"negrito": True, "cor": VERMELHO}),
        ("documentação do fornecedor, literatura (Jain, 1991; Fu et al., 2021; Tene, 2015) "
         "ou uma medição da calibração.", {})]], tam=14, ancora=MSO_ANCHOR.MIDDLE)

# --- 10. calibracao -------------------------------------------------------------
s = novo_slide("Calibração antes da coleta",
               "[1:00] Antes da coleta oficial, fiz uma calibração, e ela mostrou que o ambiente pesava mais "
               "que as arquiteturas. Com cota de CPU, o RabbitMQ era estrangulado: núcleos exclusivos "
               "reduziram o P99 dele de 40% a 64%. Com gerador e consumidor fora dos containers, o Kafka "
               "tinha latência bimodal em 43% das rodadas de carga baixa. A gravação periódica de disco da "
               "máquina virtual tornava 126 vezes mais provável uma latência acima de 50 ms, e por isso o "
               "atraso de envio virou critério de validade e há dez repetições. E uma auditoria contra os "
               "valores de fábrica achou três defeitos de configuração e três assimetrias. Tudo foi "
               "corrigido ou registrado antes da coleta, e esses dados não entram na análise.")
achados = [("40–64%", "menos P99 no RabbitMQ com núcleos exclusivos, em vez de cota de CPU",
            "núcleos exclusivos"),
           ("43%", "das rodadas do Kafka com latência bimodal, com clientes fora dos containers",
            "tudo em containers"),
           ("126×", "mais chance de P99 acima de 50 ms logo após uma pausa de disco da VM",
            "atraso de envio como critério; 10 repetições"),
           ("3 + 3", "defeitos de configuração e assimetrias entre os brokers, achados na auditoria",
            "corrigidos antes da coleta")]
for k, (num, rot, decisao) in enumerate(achados):
    x = 0.6 + k * 3.07
    bloco(s, x, 1.85, 2.85, 3.9, nome=f"Calibracao {num}")
    caixa(s, x + 0.15, 2.0, 2.55, 0.95, [num], tam=40, negrito=True, cor=VERMELHO, alinh=PP_ALIGN.CENTER,
          ancora=MSO_ANCHOR.MIDDLE)
    caixa(s, x + 0.2, 3.05, 2.45, 1.6, [rot], tam=14, alinh=PP_ALIGN.CENTER)
    caixa(s, x + 0.2, 4.7, 2.45, 0.95, [[("→ ", {"negrito": True, "cor": VERMELHO}),
                                         (decisao, {"negrito": True, "cor": VERMELHO})]],
          tam=13, alinh=PP_ALIGN.CENTER)
caixa(s, 0.6, 5.95, 12.1, 0.75,
      ["Também: linger.ms 0 descartado (P50 de 400 ms já a 10 mil eventos/s). Tudo corrigido ou registrado "
       "antes da coleta; esses dados não entram na análise."], tam=14, cor=GRAFITE)

# --- 11. validade -----------------------------------------------------------------
s = novo_slide("Validade das rodadas",
               "[0:40] Antes dos resultados: das 390 rodadas do cenário padrão, nenhuma divergiu no "
               "digest, ou seja, nenhum evento se perdeu, duplicou ou saiu de ordem. As 12 inválidas foram "
               "todas do Kafka em carga muito alta, por atraso do gerador, ligado às pausas de disco da "
               "máquina virtual. A regra de Tukey tirou cerca de 8% como discrepantes. E, como os pontos a "
               "100 Hz são interpolados, repeti o Direct com os dados originais, a 3,7 Hz: as conclusões "
               "não mudam.")
grandes = [("390", "rodadas no cenário padrão", VERMELHO), ("0", "divergências de digest entre as combinações", VERMELHO),
           ("12", "inválidas, todas do Kafka a 200 mil eventos/s ou mais", CINZA), ("8%", "descartadas como discrepantes (Tukey)", CINZA)]
for k, (num, rot, cor) in enumerate(grandes):
    x = 0.6 + k * 3.07
    bloco(s, x, 2.0, 2.85, 3.3, nome=f"Validade {num}")
    caixa(s, x + 0.2, 2.3, 2.45, 1.3, [num], tam=60, negrito=True, cor=cor, alinh=PP_ALIGN.CENTER,
          ancora=MSO_ANCHOR.MIDDLE)
    caixa(s, x + 0.25, 3.75, 2.35, 1.4, [rot], tam=16, alinh=PP_ALIGN.CENTER)
caixa(s, 0.6, 5.6, 12.1, 1.0,
      [[("Nenhum evento perdido, duplicado ou fora de ordem em nenhuma combinação.", {"negrito": True})],
       "Bateria a 3,7 Hz, com os dados originais sem interpolação: as conclusões não mudam."],
      tam=16, cor=GRAFITE, espaco=6)


# --- 12 a 14. resultados ------------------------------------------------------------
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
           ("Saturação", "RabbitMQ: 60 mil eventos/s. Kafka: acima de 400 mil, a maior carga testada, nos três modos.")],
          "[1:45] Este é o resultado principal. Laranja é RabbitMQ, azul é Kafka, e o traço indica o "
          "modo. Entre os brokers, a latência muda de sinal: o RabbitMQ é mais rápido em carga baixa, "
          "mas a cauda cresce e ele satura em 60 mil; o Kafka tem um piso de uns 3 ms, que é a espera "
          "para formar lotes, mas fica estável até 400 mil. Entre os modos, as linhas ficam uma em cima "
          "da outra. O throughput é igual em todos porque o gerador é de malha aberta: o que distingue "
          "é até onde cada um sustenta, e o modo não muda isso. O ponto de saturação do Kafka não foi "
          "localizado: sei só que fica acima de 400 mil, a maior carga que o gerador sustentou.")

resultado("Uso de recursos", "fig_recursos.png", "CPU e memória do consumidor e do broker", 7.3, 4.75,
          [("CPU do consumidor", "Só aqui os modos se separam: no Kafka, Channels e Pipelines custam 1,9 a 3,3 vezes a CPU do Direct, 2,4 a 3,7 µs a mais por evento, contra 0,12 µs do processamento."),
           ("CPU do broker", "Explica a saturação: o RabbitMQ chega a 3,5 dos 4 núcleos; o Kafka fica em 0,5."),
           ("Memória e alocação", "Não dependem do modo: cerca de 950 bytes por evento no Kafka nos três, porque quem aloca é a biblioteca cliente.")],
          "[1:15] Nos recursos, a CPU do consumidor é a única medida em que os modos se separam, e "
          "para pior: no Kafka, Channels e Pipelines custam até 3,3 vezes a CPU do Direct, porque passar "
          "cada evento para outra thread custa mais que processá-lo: são 2,4 a 3,7 microssegundos a mais por evento, "
          "mais ou menos o custo de acordar uma thread, contra 0,12 microssegundo do processamento. A CPU do broker explica a "
          "saturação: o RabbitMQ esgota os 4 núcleos, o Kafka quase não usa. A memória não depende do "
          "modo; no broker, o Kafka usa de 1,4 a 1,7 GB contra 0,2 a 0,3 GB do RabbitMQ, ou seja, troca "
          "CPU por memória. Um ponto que parece estranho: o Pipelines foi feito para não criar um objeto "
          "por evento, mas a alocação é igual nos três modos, cerca de 950 bytes por evento no Kafka, "
          "20 vezes o tamanho da mensagem. É porque quem aloca é a biblioteca cliente do broker, antes "
          "de o evento chegar ao mecanismo.")

resultado("Cenário ajustado", "fig_ajustado.png", "Modo Direct no cenário padrão e no ajustado", 8.7, 3.8,
          [("RabbitMQ", "Mensagem transitória, mesma garantia do Kafka: P99 de 43% a 76% menor; satura em 80 mil."),
           ("Kafka", "linger.ms 1: P50 de 3 para 1 ms, mas P99 maior em carga alta e CPU dobrada. "
                     "linger.ms 20: P99 ≈ 21 ms, 30% menos CPU."),
           ("Mecanismos", "Continuaram sem diferença de latência: a configuração muda os brokers, não os modos.")],
          "[1:15] No cenário ajustado, a configuração mudou muito os brokers. O RabbitMQ com mensagem "
          "transitória, que é a mesma garantia que o Kafka já tinha no padrão, reduziu o P99 em até 76% "
          "e passou a sustentar 80 mil. No Kafka, reduzir a espera de lote para 1 ms derrubou a mediana "
          "para 1 ms, mas aumentou a cauda e dobrou a CPU do broker; o perfil para vazão, com 20 ms, faz "
          "o contrário: P99 perto de 21 ms e 30% menos CPU. Ou seja, parte da diferença entre os brokers "
          "é configuração, e cada parâmetro troca latência por CPU. Os mecanismos continuaram sem "
          "diferença.")

# --- 15. sintese ------------------------------------------------------------------
s = novo_slide("Onde cada combinação ganha",
               "[1:00] Juntando tudo: entre os brokers, o RabbitMQ ganha em carga baixa e o Kafka em carga "
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

# --- 16. conclusao, limitacoes e trabalhos futuros ---------------------------------
s = novo_slide("Conclusão",
               "[1:45] Respondendo às perguntas. Primeira: nesta carga, Channels e Pipelines não fizeram "
               "diferença no desempenho e pioraram o custo. O motivo é que o processamento leva 0,12 "
               "microssegundo por evento e quem limita é o broker: não há trabalho acumulado para "
               "paralelizar. Segunda: o RabbitMQ atende carga baixa com latência mínima, o Kafka atende "
               "carga alta, e a configuração muda bastante esse quadro. Isso confirma Umam e Dobbelaere, "
               "e confirma Kamiński, que viu o Kafka melhor em .NET, mas só em carga alta; e, como Fu "
               "defende, o ambiente pesou mais que a arquitetura. Um estudo preliminar indica que, quando o "
               "processamento espera algo, como a confirmação de uma republicação no Kafka, o mecanismo "
               "passa a fazer diferença: o Direct parou em 64 mil, contra 200 mil dos outros. As "
               "limitações são uma máquina com um nó por broker e uma carga leve, e os trabalhos futuros "
               "partem delas, começando por aprofundar esse estudo preliminar.")
bloco(s, 0.6, 1.8, 12.1, 0.95, fundo=ROSADO, borda=None, nome="Veredito")
caixa(s, 0.95, 1.85, 11.4, 0.85,
      [[("Channels e Pipelines não fizeram diferença no desempenho e pioraram o custo. ", {"negrito": True, "cor": VERMELHO}),
        ("O processamento custa 0,12 µs por evento: quem limita é o broker.", {})]],
      tam=17, ancora=MSO_ANCHOR.MIDDLE)
colunas = [("Brokers", "RabbitMQ para carga baixa com latência mínima; Kafka para carga alta, com menos CPU."),
           ("Configuração", "Um parâmetro muda o resultado em até 76%: a comparação exige configurações auditadas."),
           ("Literatura", "Confirma Umam e Dobbelaere; Kamiński só em carga alta; como em Fu, o ambiente pesou mais.")]
for k, (rot, texto) in enumerate(colunas):
    x = 0.6 + k * 4.1
    bloco(s, x, 2.95, 3.9, 1.6, nome=rot)
    caixa(s, x + 0.25, 3.07, 3.4, 1.4, [[(rot, {"negrito": True, "cor": VERMELHO, "tam": 17})], texto], tam=13, espaco=3)
# estudo preliminar, a esquerda
bloco(s, 0.6, 4.75, 5.95, 1.95, nome="Estudo preliminar")
caixa(s, 0.85, 4.85, 5.5, 0.4, ["Estudo preliminar (republicação no Kafka)"], tam=15, negrito=True, cor=VERMELHO)
caixa(s, 0.85, 5.3, 2.6, 0.9, [[("64 mil", {"negrito": True, "cor": KAFKA, "tam": 28})], "Direct, eventos/s"],
      tam=12, alinh=PP_ALIGN.CENTER, espaco=0)
caixa(s, 3.6, 5.3, 2.7, 0.9, [[("200 mil", {"negrito": True, "cor": KAFKA, "tam": 28})], "Channels e Pipelines"],
      tam=12, alinh=PP_ALIGN.CENTER, espaco=0)
caixa(s, 0.85, 6.22, 5.5, 0.45, ["Quando o processamento espera, o mecanismo faz diferença."], tam=12, cor=CINZA)
# limitacoes e trabalhos futuros, a direita
bloco(s, 6.75, 4.75, 5.95, 1.95, fundo=ROSADO, borda=None, nome="Limitacoes e trabalhos futuros")
caixa(s, 7.0, 4.85, 5.5, 0.4, ["Limitação → trabalho futuro"], tam=15, negrito=True, cor=VERMELHO)
caixa(s, 7.0, 5.3, 5.5, 1.35,
      [[("Carga leve, 46 bytes → ", {"negrito": True}), ("aprofundar o estudo: processamento mais pesado, mais leitores", {})],
       [("Uma máquina, um nó → ", {"negrito": True}), ("vários nós, com replicação", {})],
       [("Filas clássicas, um a um → ", {"negrito": True}), ("quórum, streams e lotes", {})]],
      tam=12, espaco=4)

# --- 17. referencias --------------------------------------------------------------
s = novo_slide("Referências", "[0:10] As referências citadas na apresentação.")
refs = [
    "ARASU, A. et al. Linear Road: a stream data management benchmark. In: VLDB, 30., 2004. p. 480–491.",
    "DOBBELAERE, P.; ESMAILI, K. S. Kafka versus RabbitMQ: a comparative study of two industry reference "
    "publish/subscribe implementations. In: ACM DEBS, 11., 2017. p. 227–238.",
    "FU, G.; ZHANG, Y.; YU, G. A fair comparison of message queuing systems. IEEE Access, v. 9, p. 421–432, 2021.",
    "JAIN, R. The art of computer systems performance analysis. New York: Wiley, 1991.",
    "KAMIŃSKI, F.; KŁONICA, R.; PAŃCZYK, B. Comparative performance analysis of RabbitMQ and Kafka message "
    "queue systems in Spring Boot and ASP.NET environments. Journal of Computer Sciences Institute, v. 37, p. 457–462, 2025.",
    "KRUSKAL, W. H.; WALLIS, W. A. Use of ranks in one-criterion variance analysis. JASA, v. 47, n. 260, p. 583–621, 1952.",
    "MICROSOFT. System.Threading.Channels e System.IO.Pipelines. Documentação do .NET, 2023.",
    "MUTSCHLER, C.; ZIEKOW, H.; JERZAK, Z. The DEBS 2013 Grand Challenge. In: ACM DEBS, 7., 2013. p. 289–294.",
    "OPENF1 CONTRIBUTORS. OpenF1: free and open-source F1 data API, 2024. Disponível em: https://openf1.org.",
    "ORTIZ, G. et al. Evaluating the integration of Esper complex event processing engine and message brokers. "
    "PeerJ Computer Science, v. 9, e1437, 2023.",
    "TENE, G. How NOT to measure latency. Palestra, 2015. Disponível em: https://www.infoq.com/presentations/latency-response-time/.",
    "TUKEY, J. W. Exploratory data analysis. Reading: Addison-Wesley, 1977.",
    "UMAM, A. M.; RIAS, M.; ASMANI, A. A comparative study evaluation of Kafka and RabbitMQ: performance, "
    "scalability and stress test in distributed messaging systems. JUTI, v. 24, n. 1, 2026.",
]
caixa(s, 0.6, 1.75, 12.1, 5.0, refs, tam=11, espaco=4)


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

# --- apoio, depois do encerramento: para as perguntas da banca --------------------
s = novo_slide("Apoio: cada decisão, o porquê e a base",
               "Slide de apoio, só para perguntas. Para cada decisão: o que foi feito, por que, e em que se "
               "baseia. Se perguntarem 'por que não X', a resposta costuma estar na coluna do meio.",
               num="A1")
tabela(s, 0.6, 1.7, 12.1, [3.7, 4.9, 3.5], [
    ["Decisão", "Por quê", "Base"],
    ["Modo Direct como grupo de controle", "Separa o efeito do broker do efeito do mecanismo", "Desenho experimental (Jain, 1991)"],
    ["Gerador em malha aberta", "Não alivia a carga quando o sistema atrasa; não esconde a cauda", "Tene (2015)"],
    ["OpenF1 a 100 Hz, frota multiplicada", "Carga real e reproduzível, sem comprimir o tempo", "Bateria a 3,7 Hz: mesmas conclusões"],
    ["Janelas de 1 s e detecção de frenagens", "Operações de referência de processamento de fluxos", "Arasu (2004); Mutschler (2013)"],
    ["4 faixas por CRC32, um leitor por faixa", "Mesma divisão nos dois brokers; ordem por carro", "Função de fábrica do cliente Kafka"],
    ["Channel de 10 mil eventos; Pipe de 460 kB", "O mesmo volume nos dois mecanismos", "10 mil × 46 bytes"],
    ["Valores de fábrica, mudados só pela equivalência", "Nenhum broker ajustado a favor", "Código-fonte de cada versão; Fu (2021)"],
    ["At-least-once: acks=all ↔ publisher confirms", "Mesma garantia de entrega", "Documentação dos brokers"],
    ["Compressão desligada", "Réplicas quase idênticas favoreceriam o Kafka", "Conteúdo da carga"],
    ["Núcleos exclusivos, tudo em containers", "Interferência maior que o efeito medido", "Calibração: 40–64%, 43%"],
    ["10 repetições, ordem sorteada, Tukey, mediana", "Uma rodada isolada não sustenta conclusão", "Calibração: 126×; Tukey (1977)"],
    ["Limiar: 20% no P99, 10% no throughput", "Com dispersão pequena, o irrelevante fica significativo", "Fixado antes; Kruskal-Wallis (1952)"],
], tam=11, alt_linha=0.38)

s = novo_slide("Apoio: configuração dos brokers",
               "Slide de apoio, só para perguntas. Os parâmetros fora do valor de fábrica e o motivo de cada "
               "um. A diferença de durabilidade do cenário padrão é deliberada: é o padrão de cada broker, e "
               "o Kafka recomenda deixar a durabilidade para a replicação. Na calibração, a gravação em disco "
               "custou 48% no P99 do RabbitMQ a 40 mil, e o cenário ajustado iguala as garantias.",
               num="A2")
tabela(s, 0.6, 1.7, 12.1, [2.0, 6.1, 4.0], [
    ["Componente", "Parâmetro e valor", "Motivo"],
    ["Ambos", "4 faixas, carro por CRC32; compressão desligada", "Ordem por carro; não favorecer o Kafka"],
    ["Ambos", "Até 100 mil mensagens sem confirmação no gerador", "Limite equivalente (fábrica do Kafka)"],
    ["Kafka", "acks=all; offset guardado só após a entrega; 1 nó", "At-least-once; máquina única"],
    ["RabbitMQ", "Publisher confirms em lotes de 12.500, dois em voo por faixa; mensagem persistente", "Equivalente ao acks=all"],
    ["RabbitMQ", "Uma conexão por faixa; confirmação do consumidor em lotes de 100", "Recomendação do fornecedor"],
    ["RabbitMQ", "Memória considerada de 3 GB", "O padrão usa a memória da VM"],
    ["Ajustado: Kafka", "linger.ms 5 → 1 (latência) e 20 (vazão); 0 descartado", "Recomendação; 0 deu P50 de 400 ms"],
    ["Ajustado: RabbitMQ", "Mensagem transitória; prefetch 300", "Recomendação; iguala a garantia"],
], tam=12, alt_linha=0.5)
caixa(s, 0.6, 6.3, 12.1, 0.45,
      [[("Assimetria deliberada no padrão: ", {"negrito": True, "cor": VERMELHO}),
        ("o RabbitMQ grava em disco antes de confirmar (custou 48% no P99 a 40 mil); o Kafka, não.", {})]],
      tam=13)

# notas do apresentador definitivas, escritas para leitura na hora (notas.py)
from notas import NOTAS
assert len(NOTAS) == len(prs.slides), (len(NOTAS), len(prs.slides))
for slide, texto in zip(prs.slides, NOTAS):
    slide.notes_slide.notes_text_frame.text = texto

prs.save(SAIDA)
print("salvo:", SAIDA, "slides:", len(prs.slides))
