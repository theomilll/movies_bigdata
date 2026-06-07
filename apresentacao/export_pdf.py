"""
Exporta a apresentação para PDF (1 página por slide, 1920x1080, 16:9).
Captura cada slide com o navegador (gráficos e imagens já renderizados) e junta
tudo num PDF. Estático: as animações viram seu estado final (normal num PDF).

Uso: python apresentacao/export_pdf.py [n_slides]   (padrão 24)
"""

import sys
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent
HTML = (HERE / "apresentacao.html").resolve()
N = int(sys.argv[1]) if len(sys.argv) > 1 else 24
SCALE = 2  # nitidez (1920x1080 -> 3840x2160 por página)

frames_dir = HERE / "_pdf_frames"
frames_dir.mkdir(exist_ok=True)
frames = []

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1920, "height": 1080}, device_scale_factor=SCALE)
    page.goto(HTML.as_uri())
    # esconde a dica de navegação interativa (sem sentido num PDF estático)
    page.add_style_tag(content=".hint{display:none!important}")
    page.wait_for_timeout(2600)  # fontes + gráficos + animação do 1º slide
    for i in range(N):
        f = frames_dir / f"f{i+1:02d}.png"
        page.screenshot(path=str(f))
        frames.append(f)
        page.keyboard.press("ArrowRight")
        page.wait_for_timeout(2000)  # deixa as animações de entrada terminarem
    browser.close()

imgs = [Image.open(f).convert("RGB") for f in frames]
out = HERE / "apresentacao.pdf"
imgs[0].save(out, save_all=True, append_images=imgs[1:], resolution=200.0)

for f in frames:
    f.unlink()
frames_dir.rmdir()

print(f"PDF gerado: {out}  ({out.stat().st_size // 1024} KB, {len(imgs)} páginas)")
