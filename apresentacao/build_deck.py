"""Injeta apresentacao/dados_slides.json no template e gera apresentacao.html."""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
template = (HERE / "deck_template.html").read_text(encoding="utf-8")
dados = json.loads((HERE / "dados_slides.json").read_text(encoding="utf-8"))

# JSON compacto e seguro para embutir em <script>
blob = json.dumps(dados, ensure_ascii=False).replace("</", "<\\/")
out = template.replace("__DADOS_JSON__", blob)

(HERE / "apresentacao.html").write_text(out, encoding="utf-8")
print(f"Gerado: {HERE/'apresentacao.html'}  ({len(out)//1024} KB)")
