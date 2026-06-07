# Apresentação AV2 — movies_bigdata

Apresentação HTML (16:9, roda 100% no navegador, offline) para a entrega AV2 de
Fundamentos de Big Data. Tema 14: **prever o sucesso de qualquer filme antes da
estreia**, com back-test multigênero em lançamentos recentes (animação, sci-fi,
musical, ação, drama e terror — incluindo os escolhidos pelo grupo: Backrooms,
Obsessão e Dia D).

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

São 24 slides desenhados para ~20 minutos de fala (densidade "palestra").

## Como os números são gerados (sem digitação manual)

Todo número vem do pipeline e do modelo, não é digitado à mão:

```bash
# da raiz do projeto, com o ambiente virtual ativo
python codigo/modelo_predicao.py        # treina o modelo e salva métricas
python codigo/predicao_lancamentos.py   # roda a demo de terror (predições)
python codigo/dados_apresentacao.py     # consolida tudo em apresentacao/dados_slides.json
python apresentacao/build_deck.py       # injeta os dados em apresentacao.html
```

`deck_template.html` é o template (com o marcador `__DADOS_JSON__`);
`build_deck.py` injeta `dados_slides.json` e gera `apresentacao.html`.

## Dashboard interativo (demo ao vivo opcional)

```bash
streamlit run dashboard/streamlit_app.py
```

Aba **Panorama** (filtros por gênero/década) + aba **Predição de lançamentos**
(back-test de terror + formulário "teste seu próprio filme").

## Arquivos

| Arquivo | Papel |
|---|---|
| `apresentacao.html` | **Apresentação final** (abrir esta) |
| `deck_template.html` | Template editável dos slides |
| `dados_slides.json` | Números consolidados (pipeline + modelo) |
| `build_deck.py` | Injeta os dados no template |
| `screenshot.py` | Captura PNGs dos slides (verificação) |
| `plotly.min.js` | Biblioteca de gráficos (local/offline) |
