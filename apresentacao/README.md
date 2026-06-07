# Apresentação AV2 — movies_bigdata

Apresentação HTML (16:9, roda 100% no navegador, offline) para a entrega AV2 de
Fundamentos de Big Data. Tema 14: **prever a bilheteria e a chance de sucesso de
qualquer filme ANTES da estreia**. O modelo (HistGradientBoosting) usa só
atributos pré-lançamento (orçamento, gênero, diretor, duração, franquia,
keywords) — sinais pós-estreia (votos, popularidade, notas) são excluídos por
vazamento, então ele prevê até filmes que ainda nem saíram. Lançamentos recentes
ilustram onde ele acerta (mainstream, ex.: Dia D do Spielberg) e onde erra (os
virais de baixo orçamento — Backrooms, Obsessão).

## Como abrir

Basta abrir `apresentacao.html` no navegador (duplo clique ou):

```bash
xdg-open apresentacao/apresentacao.html     # Linux
```

> Mantenha `apresentacao.html`, `plotly.min.js` na mesma pasta — os gráficos
> interativos usam o Plotly local (sem internet).

## Como apresentar

- **Avançar / voltar:** ← → · espaço · PageUp/PageDown · roda do mouse · swipe
- **Início / fim:** Home / End
- **Editar texto ao vivo:** tecla `E` (ou passe o mouse no canto superior esquerdo),
  clique no texto, edite; aperte `E` de novo para salvar no navegador.
- **Gráficos interativos:** passe o mouse sobre as barras/pontos para ver os valores.

São 25 slides desenhados para ~20 minutos de fala (densidade "palestra").

## Como os números são gerados (sem digitação manual)

Todo número vem do pipeline e do modelo oficial do grupo, não é digitado à mão:

```bash
# da raiz do projeto, com o ambiente virtual ativo
python codigo/pipeline_filmes.py           # Bronze→Gold (dados/processed)
python codigo/modelo_preditivo.py          # treina o modelo + salva métricas/joblib
python apresentacao/gerar_dados_modelo.py  # atualiza o bloco do modelo em dados_slides.json
python apresentacao/build_deck.py          # injeta os dados em apresentacao.html
```

`deck_template.html` é o template (com o marcador `__DADOS_JSON__`);
`gerar_dados_modelo.py` lê `dados/processed/modelos/metricas_modelo.json` e a lista de
lançamentos (`filmes_lancamentos.json`) para preencher métricas e o back-test pré-estreia;
`build_deck.py` injeta `dados_slides.json` e gera `apresentacao.html`.

## Dashboard interativo (demo ao vivo opcional)

```bash
streamlit run dashboard/streamlit_app.py
```

Aba **Panorama** (filtros por gênero/década, gráficos, ranking de ROI) + aba
**Previsão** (formulário "teste seu próprio filme").

## Arquivos

| Arquivo | Papel |
|---|---|
| `apresentacao.html` | **Apresentação final** (abrir esta) |
| `deck_template.html` | Template editável dos slides |
| `dados_slides.json` | Números consolidados (pipeline + modelo) |
| `gerar_dados_modelo.py` | Atualiza o bloco do modelo a partir dos artefatos do repo |
| `filmes_lancamentos.json` | Lançamentos recentes (metadata + bilheteria pesquisada) |
| `build_deck.py` | Injeta os dados no template |
| `build_standalone.py` | Gera 1 arquivo único (plotly + imagens embutidos) |
| `export_pdf.py` | Exporta a apresentação para PDF |
| `screenshot.py` | Captura PNGs dos slides (verificação) |
| `plotly.min.js` | Biblioteca de gráficos (local/offline) |
