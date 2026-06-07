"""Captura screenshots de slides HTML para verificação visual.
Uso: python apresentacao/screenshot.py <arquivo.html> <saida_dir> [--slides N] [--w 1280] [--h 720]
Sem --slides: captura só o estado atual. Com --slides N: navega N slides (ArrowRight) e captura cada um.
"""
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright

html = Path(sys.argv[1]).resolve()
outdir = Path(sys.argv[2]).resolve()
outdir.mkdir(parents=True, exist_ok=True)
nslides = 0
w, h = 1280, 720
args = sys.argv[3:]
for i, a in enumerate(args):
    if a == "--slides":
        nslides = int(args[i + 1])
    if a == "--w":
        w = int(args[i + 1])
    if a == "--h":
        h = int(args[i + 1])

stem = html.stem
with sync_playwright() as p:
    b = p.chromium.launch()
    pg = b.new_page(viewport={"width": w, "height": h}, device_scale_factor=2)
    pg.goto(html.as_uri())
    pg.wait_for_timeout(1400)
    if nslides <= 0:
        out = outdir / f"{stem}.png"
        pg.screenshot(path=str(out))
        print("saved", out)
    else:
        for i in range(nslides):
            out = outdir / f"{stem}-{i+1:02d}.png"
            pg.screenshot(path=str(out))
            print("saved", out)
            pg.keyboard.press("ArrowRight")
            pg.wait_for_timeout(700)
    b.close()
