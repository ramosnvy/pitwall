"""Converte um .pptx em PDF com o LibreOffice instalado e cada pagina em JPG
(PyMuPDF). Uso: python render.py deck.pptx prefixo [paginas, ex. 1,3,5]"""
import subprocess, sys, tempfile
from pathlib import Path
import fitz

SOFFICE = r"C:\Program Files\LibreOffice\program\soffice.exe"
deck = Path(sys.argv[1]).resolve()
prefixo = sys.argv[2]
so = {int(p) for p in sys.argv[3].split(',')} if len(sys.argv) > 3 else None

perfil = Path(tempfile.mkdtemp(prefix="lo-")).as_uri()
subprocess.run([SOFFICE, f"-env:UserInstallation={perfil}", "--headless", "--convert-to", "pdf",
                "--outdir", str(deck.parent), str(deck)], check=True, capture_output=True, timeout=240)
pdf = deck.with_suffix(".pdf")
doc = fitz.open(pdf)
for k in sorted(set(range(1, doc.page_count + 1)) if so is None else so):
    pix = doc[k - 1].get_pixmap(dpi=110)
    out = deck.parent / f"{prefixo}-{k:02d}.jpg"
    pix.save(str(out))
    print(out)
print("paginas:", doc.page_count)
