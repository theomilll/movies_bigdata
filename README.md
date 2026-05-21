# movies_bigdata

Relatorio final AV2 - Fundamentos de Big Data

Entrega: 08/06/2026

Repositorio publico: https://github.com/theomilll/movies_bigdata

## Introducao

Este projeto desenvolve uma pipeline de engenharia e analise de dados para filmes a partir do **The Movies Dataset**. A base original combina metadados do TMDB, creditos de producao, palavras-chave, links para MovieLens e avaliacoes de usuarios. O objetivo pratico e transformar arquivos CSV brutos em um dataset gold reprodutivel, com tabelas analiticas, visualizacoes e um dashboard para apoiar a apresentacao final.

O problema analisado e: **quais fatores ajudam a explicar sucesso comercial e desempenho analitico de filmes quando combinamos genero, diretor, decada, orcamento, receita, ratings e popularidade?**

## Motivacao

Filmes sao um bom estudo de caso para Big Data em escala educacional porque misturam variaveis estruturadas, campos semiestruturados e indicadores de negocio. A base exige limpeza, normalizacao, parsing de listas/dicionarios serializados, joins entre identificadores diferentes e geracao de artefatos finais para consumo analitico.

A escolha tambem permite discutir uma limitacao real: nem todo filme tem orcamento, receita ou rating de usuarios preenchido. Portanto, a analise precisa ser transparente sobre cobertura, vieses de popularidade e limites do cruzamento entre TMDB e MovieLens.

## Objetivo

Construir uma solucao baseada em dados que implemente e documente uma pipeline completa:

- coletar e organizar fontes de dados brutas;
- validar arquivos e colunas obrigatorias;
- limpar e enriquecer a base de filmes;
- gerar um dataset gold em CSV local e Parquet versionado;
- produzir 11 tabelas analiticas e 4 visualizacoes;
- disponibilizar resultados em README, notebook e dashboard;
- preparar material final para apresentacao de ate 20 minutos.

## Metodologia / Pipeline

### Fontes

Fonte principal: **The Movies Dataset**, disponivel no Kaggle:

https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset

Arquivos obrigatorios esperados em `dados/raw/`:

| Arquivo | Papel na pipeline |
|---|---|
| `movies_metadata.csv` | Base principal com titulo, genero, datas, orcamento, receita, popularidade e votos do TMDB |
| `credits.csv` | Campo `crew`, usado para extrair o diretor |
| `keywords.csv` | Palavras-chave associadas a cada filme |
| `links.csv` | Ponte entre IDs MovieLens e IDs TMDB |
| `ratings_small.csv` | Avaliacoes de usuarios do MovieLens Small |

`credits.csv` e obrigatorio, mas nao e versionado por tamanho. A abordagem canonica e baixar o arquivo do Kaggle e coloca-lo em `dados/raw/credits.csv`. A pipeline falha explicitamente se ele estiver ausente; nao ha fallback silencioso.

### Ingestao

A funcao publica `load_raw_datasets()` em `codigo/pipeline_filmes.py` localiza `dados/raw/`, verifica todos os arquivos obrigatorios e valida colunas minimas. A execucao real da AV2 leu:

| Dataset | Linhas | Colunas |
|---|---:|---:|
| movies | 45.466 | 24 |
| credits | 45.476 | 3 |
| keywords | 46.419 | 2 |
| links | 45.843 | 3 |
| ratings | 100.004 | 4 |

### Transformacao

A transformacao e implementada em funcoes pequenas e reutilizaveis:

- `clean_movies()`: remove IDs invalidos e duplicados, converte colunas numericas, remove filmes adultos, remove duracoes invalidas e trata orcamento/receita zero como ausentes.
- `build_directors()`: extrai o diretor a partir do campo `crew`.
- `build_keywords()`: transforma keywords serializadas em lista e palavra-chave principal.
- `build_movie_ratings()`: cruza `ratings_small.csv` com `links.csv` para trazer media e contagem de notas de usuarios ao ID TMDB.
- `build_gold_dataset()`: consolida tudo e deriva `profit`, `roi`, `release_year`, `decade` e `has_collection`.
- `build_aggregations()`: gera as 11 tabelas analiticas finais.

Resultado validado: a base gold possui **43.608 filmes** e **25 colunas**.

### Carregamento

Os artefatos sao gravados em `dados/processed/`:

- `filmes_processados.csv`: dataset gold completo em CSV, gerado localmente e ignorado no Git por tamanho.
- `filmes_processados_parquet/part-00000.parquet`: dataset gold versionado em Parquet.
- 11 CSVs analiticos pequenos, versionados para consulta direta.
- 4 graficos PNG em `dados/processed/visualizacoes/`.

### Destino

Os resultados ficam disponiveis em quatro superficies:

- README final, que funciona como relatorio principal do GitHub.
- Notebook canonico: `notebooks/pipeline_filmes.ipynb`.
- Dashboard Streamlit: `dashboard/streamlit_app.py`.
- Material de apresentacao em `documentacao/apresentacao_av2.md` e `documentacao/roteiro_apresentacao_av2.md`.

## Tecnologias e arquitetura

| Tecnologia | Uso |
|---|---|
| Python 3.11 | Linguagem principal |
| pandas | Leitura, limpeza, joins e agregacoes |
| matplotlib + seaborn | Graficos PNG finais |
| pyarrow + Parquet | Armazenamento colunar do dataset gold |
| Jupyter Notebook | Exploracao e apresentacao tecnica |
| Streamlit + Plotly | Dashboard interativo de demonstracao |
| pytest | Smoke test automatizado da pipeline |
| GitHub | Repositorio publico e entrega principal |

A arquitetura segue um modelo medallion simplificado:

- Bronze: CSVs brutos em `dados/raw/`.
- Silver: limpeza, normalizacao, parsing e enriquecimento.
- Gold: dataset final, agregacoes, Parquet, CSVs analiticos e graficos.

O ponto de entrada estavel e `codigo/pipeline_filmes.py`. O notebook e o dashboard consomem os artefatos ou funcoes da pipeline, sem reimplementar a logica principal.

## Resultados e visualizacoes

Artefatos finais verificados:

- Dataset gold: 43.608 linhas e 25 colunas.
- Dados financeiros completos: 5.363 filmes com orcamento e receita.
- Filmes com receita preenchida: 7.371, ou 16,9% da base.
- Filmes com orcamento preenchido: 8.808, ou 20,2% da base.
- Filmes com rating de usuario cruzado via MovieLens: 9.005, ou 20,6% da base.
- Receita total observada: US$ 509,22 bi.
- Orcamento total observado: US$ 191,73 bi.
- ROI mediano entre filmes com dados financeiros: 106,19%.

Graficos versionados:

- `dados/processed/visualizacoes/receita_por_genero.png`
- `dados/processed/visualizacoes/filmes_por_decada.png`
- `dados/processed/visualizacoes/orcamento_vs_receita.png`
- `dados/processed/visualizacoes/correlacoes_sucesso.png`

### Principais achados

1. **A receita e concentrada por genero.** Action lidera com US$ 122,04 bi, seguida de Adventure, Comedy e Drama. Os quatro primeiros generos concentram aproximadamente 69,6% da receita observada por genero.

2. **Genero com maior receita total nao e necessariamente o de maior receita media.** Action tem a maior receita total, mas Animation e Adventure possuem receita media por filme maior entre os primeiros grupos, refletindo menor volume e maior concentracao em lancamentos de grande escala.

3. **Diretores acumulam receita por volume e franquias.** Steven Spielberg lidera o ranking com US$ 9,26 bi em 30 filmes, enquanto James Cameron aparece com US$ 5,90 bi em apenas 8 filmes. Isso mostra que receita acumulada mistura escala de carreira, franquias e bilheterias excepcionais.

4. **Decadas recentes dominam a amostra.** A base contem 10.685 filmes dos anos 2000 e 12.150 dos anos 2010. Isso indica maior cobertura digital recente e exige cuidado ao comparar periodos antigos e recentes.

5. **Orcamento explica parte relevante da receita, mas nao explica ROI.** A correlacao entre budget e revenue foi 0,7301, e entre vote_count e revenue foi 0,7816. Para ROI, as correlacoes ficaram proximas de zero, indicando que retorno percentual depende de outra dinamica.

6. **Top receita e top ROI contam historias diferentes.** Avatar lidera receita com US$ 2,79 bi, mas o ranking de ROI e liderado por Alice in Wonderland (1951), com 18.966,67%, porque o investimento registrado e muito menor.

7. **Ratings de usuarios ajudam, mas a cobertura e limitada.** Apenas 20,6% da base recebeu media de usuarios depois do join MovieLens/TMDB. Alem disso, `user_rating_avg` teve correlacao quase nula com revenue (0,0011) na amostra com dados disponiveis.

8. **Popularidade e votos medem visibilidade, nao qualidade pura.** `vote_count` e `popularity` se correlacionam mais com receita que as notas medias, sugerindo que alcance e exposicao influenciam fortemente os indicadores comerciais.

## Dashboard

O dashboard Streamlit le os artefatos processados e oferece:

- KPIs de filmes filtrados, filmes com receita, receita total e ROI mediano;
- filtros por genero e decada;
- grafico de receita por genero;
- grafico de orcamento vs receita;
- tendencia por decada;
- heatmap de correlacoes;
- tabela dos filmes com maior ROI.

Execucao:

```bash
streamlit run dashboard/streamlit_app.py
```

Se os artefatos processados nao existirem, o dashboard falha com uma mensagem clara pedindo a execucao de `python codigo/pipeline_filmes.py`.

## Conclusoes

O projeto entrega uma pipeline completa e reprodutivel, alinhada aos requisitos de AV2: fontes, ingestao, transformacao, carregamento, destino, visualizacoes e apresentacao. A analise mostra que sucesso comercial em filmes e fortemente associado a escala de lancamento, visibilidade e franquias, enquanto ROI exige leitura separada porque favorece filmes com menor orcamento relativo.

Tambem fica claro que a base nao deve ser interpretada como retrato perfeito da industria. A baixa cobertura de orcamento, receita e ratings limita inferencias estatisticas fortes. Por isso, os resultados sao tratados como exploratorios e orientados a storytelling, nao como modelo preditivo final.

## Dificuldades encontradas

- `credits.csv` e grande e nao deve ser commitado, mas e necessario para extrair diretores.
- Campos como `genres`, `crew` e `keywords` chegam como strings com listas/dicionarios, exigindo parsing seguro.
- Muitos registros usam zero para orcamento ou receita, o que precisou ser tratado como ausencia de dado.
- O cruzamento MovieLens/TMDB depende de IDs em `links.csv`, reduzindo a cobertura de ratings.
- O ambiente local inicialmente nao tinha pandas, matplotlib, seaborn ou pyarrow instalados.
- Havia um notebook duplicado na raiz e um `small_test.py` sem valor real de validacao.

## Trabalhos futuros

- Trocar o dataset pequeno de ratings por uma base MovieLens maior, documentando custo e impacto de processamento.
- Adicionar validacoes de qualidade de dados com limites minimos de cobertura por coluna.
- Criar series temporais mais detalhadas por ano, pais e idioma original.
- Adicionar testes para contratos de saida dos CSVs analiticos.
- Publicar o dashboard em ambiente hospedado, caso a disciplina exija acesso externo.
- Avaliar Power BI, Tableau ou DuckDB para exploracao mais interativa em producao.

## Como executar

### Execucao local

1. Criar e ativar ambiente:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

No Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

3. Baixar os dados:

- Acesse https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset.
- Baixe o ZIP do dataset.
- Extraia os CSVs obrigatorios para `dados/raw/`.
- Confirme que `dados/raw/credits.csv` existe.

4. Executar a pipeline:

```bash
python codigo/pipeline_filmes.py
```

5. Rodar testes:

```bash
pytest -q
```

6. Abrir o dashboard:

```bash
streamlit run dashboard/streamlit_app.py
```

7. Abrir o notebook:

```bash
jupyter notebook notebooks/pipeline_filmes.ipynb
```

### Execucao no Google Colab

1. Clone o repositorio ou envie a pasta do projeto ao Colab.
2. Rode:

```python
!pip install -r requirements.txt
```

3. Faca upload dos CSVs obrigatorios para `dados/raw/`, incluindo `credits.csv`.
4. Execute:

```python
!python codigo/pipeline_filmes.py
```

5. Abra `notebooks/pipeline_filmes.ipynb` para explorar as etapas e saidas.

## Estrutura do repositorio

```text
movies_bigdata/
|-- codigo/
|   `-- pipeline_filmes.py
|-- dashboard/
|   `-- streamlit_app.py
|-- dados/
|   |-- raw/
|   |   |-- README.md
|   |   |-- keywords.csv
|   |   |-- links.csv
|   |   |-- links_small.csv
|   |   |-- movies_metadata.csv
|   |   `-- ratings_small.csv
|   `-- processed/
|       |-- README.md
|       |-- filmes_processados_parquet/
|       |-- visualizacoes/
|       `-- *.csv
|-- documentacao/
|   |-- arquitetura_av1.pdf
|   |-- apresentacao_av2.md
|   `-- roteiro_apresentacao_av2.md
|-- notebooks/
|   `-- pipeline_filmes.ipynb
|-- tests/
|   `-- test_pipeline_filmes.py
|-- PROJETO_FUND_BIG_DATA.pdf
|-- pipeline_diagrama.svg
|-- requirements.txt
`-- README.md
```

## Equipe e divisao de tarefas

Equipe registrada no documento de arquitetura AV1:

| Integrante | Responsabilidades |
|---|---|
| Theomilll / Theo Moura | Criacao do repositorio, estrutura inicial, pipeline, reproducibilidade e consolidacao final |
| Joao Pedro Araujo Nobrega | Documentacao, README e organizacao textual do relatorio |
| menex100 | Upload e organizacao de dados brutos e artefatos processados |
| Joao Batista | Notebook, execucao passo a passo e validacao em Google Colab |
| Mathews Ivo Tavares | Ajustes no notebook, testes de execucao e melhorias de compreensao |

Observacao para a apresentacao: a proposta da disciplina menciona equipes de exatamente 3 participantes. Antes da entrega oficial, a equipe deve confirmar quais nomes devem permanecer como membros formais e quais aparecem apenas como colaboradores do repositorio.

## Verificacao AV2

Comandos executados na preparacao final:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pytest -q
.venv/bin/python codigo/pipeline_filmes.py
```

Resultados verificados:

- Testes: 2 passed.
- Pipeline: 43.608 filmes no dataset gold.
- Saidas: Parquet, CSV principal local, 11 CSVs analiticos e 4 PNGs.
- Dashboard: implementado em `dashboard/streamlit_app.py`.
- Apresentacao: `documentacao/apresentacao_av2.md` e `documentacao/roteiro_apresentacao_av2.md`.
