# Apresentacao AV2 - movies_bigdata

Tempo alvo: 20 minutos.

## 1. Abertura - 1 min

- Tema: engenharia e analise de dados de filmes a partir do The Movies Dataset.
- Problema: transformar arquivos brutos e heterogeneos em uma base gold capaz de responder perguntas sobre sucesso comercial, avaliacao e tendencias temporais.
- Entrega principal: repositorio publico no GitHub funcionando como relatorio.

## 2. Motivacao - 2 min

- Filmes combinam variaveis financeiras, criativas, temporais e avaliativas.
- O dataset permite praticar ingestao, limpeza, enriquecimento, agregacao, visualizacao e storytelling.
- A analise ajuda a comparar receita, genero, diretor, decada, ratings e ROI.

## 3. Fontes de dados - 2 min

- The Movies Dataset no Kaggle.
- Arquivos usados: `movies_metadata.csv`, `credits.csv`, `keywords.csv`, `links.csv`, `ratings_small.csv`.
- `links.csv` conecta IDs MovieLens a IDs TMDB para agregar notas de usuarios.
- `credits.csv` permite extrair o diretor a partir do campo `crew`.

## 4. Pipeline em funcionamento - 4 min

- Bronze: leitura e validacao dos CSVs obrigatorios.
- Silver: limpeza de IDs, tipos numericos, filmes adultos, duracao invalida, orcamento/receita zero.
- Enriquecimento: diretor, palavras-chave, nota media de usuarios.
- Gold: dataset consolidado com `profit`, `roi`, `release_year`, `decade` e `has_collection`.
- Saidas: Parquet, CSV principal local, 11 tabelas analiticas e 4 graficos PNG.

Comando para demonstrar:

```bash
python codigo/pipeline_filmes.py
```

## 5. Resultados principais - 5 min

- Dataset gold validado: 43.608 filmes e 25 colunas.
- Receita observada total: US$ 509,22 bi; orcamento observado total: US$ 191,73 bi.
- Action lidera receita por genero com US$ 122,04 bi; Action, Adventure, Comedy e Drama somam cerca de 69,6% da receita observada.
- Steven Spielberg lidera receita acumulada de diretores com US$ 9,26 bi em 30 filmes; James Cameron aparece com US$ 5,90 bi em 8 filmes.
- Decadas recentes concentram maior volume: 10.685 filmes dos anos 2000 e 12.150 dos anos 2010.
- Orcamento e receita apresentam correlacao forte (0,7301), mas as correlacoes com ROI ficam proximas de zero.
- Avatar lidera receita absoluta; Alice in Wonderland (1951) lidera ROI entre os filtros usados.
- Ratings de usuarios ampliam a leitura de qualidade, mas cobrem apenas 20,6% da base apos o cruzamento MovieLens/TMDB.

## 6. Dashboard - 3 min

Comando para demonstrar:

```bash
streamlit run dashboard/streamlit_app.py
```

Mostrar:

- KPIs de filmes filtrados, receita total e ROI mediano.
- Filtros por genero e decada.
- Grafico de receita por genero.
- Grafico de orcamento vs receita.
- Tendencia por decada.
- Heatmap de correlacoes.
- Tabela de filmes com maior ROI.

## 7. Limitacoes - 2 min

- Muitos filmes nao possuem orcamento ou receita preenchidos.
- Valores zero foram tratados como ausentes, o que reduz a amostra financeira.
- `ratings_small.csv` nao cobre todos os filmes do TMDB.
- O join entre MovieLens e TMDB depende de `links.csv` e pode perder filmes.
- Popularidade e contagem de votos carregam vieses de visibilidade.

## 8. Fechamento - 1 min

- O projeto entrega uma pipeline reprodutivel, com validacao, transformacao, armazenamento final e visualizacao.
- O README funciona como relatorio principal em formato de repositorio GitHub.
- Trabalhos futuros: dashboard mais interativo, validacao automatica de qualidade de dados, series temporais e novas fontes externas.
