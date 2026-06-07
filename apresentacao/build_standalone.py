"""
Gera uma versão AUTOSSUFICIENTE da apresentação: um único arquivo HTML com a
biblioteca de gráficos (plotly) e as imagens (base64) EMBUTIDAS. Pode ser enviado
sozinho (WhatsApp, e-mail, Drive) e abre em qualquer navegador, offline.

Também empacota a pasta completa em apresentacao.zip como alternativa.

Uso: python apresentacao/build_standalone.py   (rode build_deck.py antes)
"""

import base64
import re
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
html = (HERE / "apresentacao.html").read_text(encoding="utf-8")

# 1) Embute o plotly.min.js (escapa </script> para não fechar a tag por engano)
plotly = (HERE / "plotly.min.js").read_text(encoding="utf-8").replace("</script", "<\\/script")
html = html.replace('<script src="plotly.min.js"></script>', "<script>\n" + plotly + "\n</script>")

# 2) Embute as imagens referenciadas como assets/... em base64
def _inline_img(match):
    rel = match.group(1)
    path = HERE / rel
    if not path.is_file():
        return match.group(0)
    b64 = base64.b64encode(path.read_bytes()).decode()
    mime = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
    return f'src="data:{mime};base64,{b64}"'

html = re.sub(r'src="(assets/[^"]+)"', _inline_img, html)

standalone = HERE / "apresentacao_standalone.html"
standalone.write_text(html, encoding="utf-8")
print(f"1 arquivo único: {standalone}  ({len(html)//1024} KB)")

# 3) Alternativa: zip com a pasta completa (HTML + assets + plotly)
zip_path = HERE / "apresentacao.zip"
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
    z.write(HERE / "apresentacao.html", "apresentacao/apresentacao.html")
    z.write(HERE / "plotly.min.js", "apresentacao/plotly.min.js")
    for img in (HERE / "assets").glob("*.png"):
        z.write(img, f"apresentacao/assets/{img.name}")
print(f"zip da pasta:   {zip_path}  ({zip_path.stat().st_size//1024} KB)")
