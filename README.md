# movies_bigdata

A partir de arquivos CSV brutos, com consolidacao de um dataset final, gerando tabelas analiticas e exportacao de visualizacao, surgiu o projeto de engenharia e analise de dados para processamento de filmes. 

O repositorio esta organizado em torno de uma pipeline em Python que:

- le os arquivos brutos em `dados/raw`
- limpa e padroniza os dados principais de filmes
- enriquece o dataset com diretores, palavras-chave e notas de usuarios
- calcula indicadores de sucesso comercial e analitico
- salva artefatos em CSV, Parquet e PNG

## Fonte dos dados

Os arquivos brutos utilizados neste projeto sao provenientes do **The Movies Dataset**, disponivel publicamente no Kaggle, originado do TMDB (The Movie Database), combinado com o dataset **MovieLens Small** do GroupLens:

- Kaggle: https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset
- MovieLens: https://grouplens.org/datasets/movielens/latest/

## Objetivo

Construir uma base consolidada de filmes para analise exploratoria, permitindo responder perguntas como:

- quais generos concentram mais receita
- quais diretores acumulam melhor desempenho
- como a producao e a avaliacao media mudam por decada
- quais fatores se correlacionam com receita e ROI
- quais filmes se destacam por receita total ou retorno sobre investimento

## Estrutura do projeto

```text
movies_bigdata/
|-- codigo/
|   `-- pipeline_filmes.py
|-- dados/
|   |-- raw/
|   |   |-- keywords.csv
|   |   |-- links.csv
|   |   |-- links_small.csv
|   |   |-- movies_metadata.csv
|   |   `-- ratings_small.csv
|   `-- processed/
|       |-- filmes_processados_parquet/
|       `-- visualizacoes/
|-- notebooks/
|   `-- pipeline_filmes.ipynb
|-- .gitignore
`-- README.md
```

## Componentes principais

### `codigo/pipeline_filmes.py`

Arquivo principal do projeto. Ele concentra toda a logica da pipeline:

- validacao dos arquivos e colunas obrigatorias
- limpeza de `movies_metadata.csv`
- extracao do diretor a partir de `credits.csv`
- extracao e consolidacao de palavras-chave
- cruzamento entre `links.csv` e `ratings_small.csv`
- criacao do dataset gold
- geracao de tabelas agregadas
- exportacao dos resultados
- criacao dos graficos finais

### `notebooks/pipeline_filmes.ipynb`

Notebook de apoio para executar a pipeline a partir da raiz do projeto e visualizar:

- resumo das fontes lidas
- resumo por etapa do processamento
- amostra do dataset final
- previas das tabelas analiticas
- exibicao dos graficos gerados

O notebook nao reimplementa a logica; ele importa e executa `main()` do arquivo em `codigo/`.

## Dados de entrada

A pipeline espera encontrar os arquivos abaixo em `dados/raw`:

| Arquivo | Obrigatorio | Uso na pipeline |
|---|---|---|
| `movies_metadata.csv` | Sim | Base principal de filmes |
| `credits.csv` | Sim | Extracao de diretor a partir do campo `crew` |
| `keywords.csv` | Sim | Extracao de palavras-chave por filme |
| `links.csv` | Sim | Relacao entre `movieId` e `tmdbId` |
| `ratings_small.csv` | Sim | Media e contagem de avaliacoes de usuarios |
| `links_small.csv` | Nao | Presente no repositorio, mas nao usado pela pipeline principal |

## Estado atual do repositorio

O repositorio versiona apenas parte dos insumos e parte das saidas.

Pontos importantes:

- `credits.csv` e obrigatorio no codigo, mas nao esta presente em `dados/raw` no estado atual do repositorio
- os CSVs gerados em `dados/processed` estao ignorados no `.gitignore`, entao nao aparecem versionados
- os artefatos atualmente presentes no repositorio sao:
  - `dados/processed/filmes_processados_parquet/part-00000.parquet`
  - `dados/processed/filmes_processados_parquet/_SUCCESS`
  - `dados/processed/visualizacoes/correlacoes_sucesso.png`
  - `dados/processed/visualizacoes/filmes_por_decada.png`
  - `dados/processed/visualizacoes/orcamento_vs_receita.png`
  - `dados/processed/visualizacoes/receita_por_genero.png`

Isso significa que, para reexecutar a pipeline do zero, sera necessario adicionar `dados/raw/credits.csv` e instalar as dependencias Python.

## Requisitos

Sugestao de ambiente:

- Python 3.11 ou superior
- `pandas`
- `matplotlib`
- `seaborn`
- `pyarrow`
- `jupyter` ou `notebook` para abrir o notebook

Exemplo de instalacao no PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install pandas matplotlib seaborn pyarrow notebook
```

## Como executar

### 1. Preparar os dados

Garanta que todos os arquivos obrigatorios estejam em `dados/raw`, principalmente `credits.csv`.

### 2. Executar a pipeline

A partir da raiz do projeto:

```powershell
python .\codigo\pipeline_filmes.py
```

### 3. Executar via notebook

Se preferir explorar o fluxo de forma interativa:

```powershell
jupyter notebook .\notebooks\pipeline_filmes.ipynb
```

O notebook localiza automaticamente a raiz do projeto, adiciona `codigo/` ao `sys.path` e chama `main(data_root=PROJECT_ROOT)`.

## Etapas da pipeline

### 1. Ingestao e validacao

- localiza a pasta `dados/raw`
- verifica a existencia dos arquivos obrigatorios
- valida colunas minimas esperadas em cada dataset

### 2. Limpeza dos filmes

- remove IDs invalidos
- remove IDs duplicados
- converte colunas numericas
- elimina filmes marcados como adultos
- remove registros sem titulo valido
- remove registros sem duracao valida
- trata `budget` e `revenue` iguais a zero como ausentes

### 3. Enriquecimento

- extrai diretor do campo `crew`
- transforma o campo de palavras-chave em lista
- deriva a palavra-chave principal
- cruza `links.csv` com `ratings_small.csv`
- calcula media e contagem de notas por filme

### 4. Dataset final

O dataset consolidado inclui, entre outros, os campos:

- identificacao: `id`, `title`
- classificacao: `primary_genre`, `genres_list`
- contexto criativo: `director`, `primary_keyword`, `keywords_list`
- indicadores financeiros: `budget`, `revenue`, `profit`, `roi`
- indicadores de avaliacao: `vote_average`, `vote_count`, `user_rating_avg`, `user_rating_count`
- contexto temporal: `release_date`, `release_year`, `decade`
- atributos complementares: `runtime`, `original_language`, `has_collection`, `overview`, `status`

### 5. Agregacoes analiticas

O script gera tabelas como:

- `receita_por_genero`
- `top_diretores`
- `filmes_por_decada`
- `top_palavras_chave`
- `correlacoes_sucesso`
- `top_filmes_receita`
- `top_filmes_roi`
- `resumo_sucesso_por_genero`
- `resumo_sucesso_por_diretor`
- `resumo_sucesso_por_decada`
- `resumo_sucesso_por_palavra_chave`

### 6. Exportacao

Sao produzidos os seguintes artefatos em `dados/processed`:

- `filmes_processados.csv`
- `filmes_processados_parquet/part-00000.parquet`
- `receita_por_genero.csv`
- `top_diretores.csv`
- `filmes_por_decada.csv`
- `top_palavras_chave.csv`
- `correlacoes_sucesso.csv`
- `top_filmes_receita.csv`
- `top_filmes_roi.csv`
- `resumo_sucesso_por_genero.csv`
- `resumo_sucesso_por_diretor.csv`
- `resumo_sucesso_por_decada.csv`
- `resumo_sucesso_por_palavra_chave.csv`

Tambem sao gerados os graficos:

- `visualizacoes/receita_por_genero.png`
- `visualizacoes/filmes_por_decada.png`
- `visualizacoes/orcamento_vs_receita.png`
- `visualizacoes/correlacoes_sucesso.png`

## Observacoes tecnicas

- a exportacao em Parquet depende explicitamente de `pyarrow`
- o codigo emite avisos quando encontra conteudo malformado em campos serializados como listas/dicionarios
- `ratings_small.csv` e usado no lugar de um arquivo de ratings completo
- `links_small.csv` existe no repositorio, mas nao participa da execucao principal
- o script foi escrito para ser executado da raiz do projeto ou com `data_root` informado

## Possiveis melhorias

- adicionar um `requirements.txt` ou `pyproject.toml`
- versionar um arquivo de exemplo para `credits.csv` ou documentar sua origem esperada
- criar testes para validacao das etapas de limpeza e agregacao
- salvar tambem um resumo executivo dos principais achados em Markdown ou HTML

## Resumo

Este projeto entrega uma pipeline de dados de filmes com foco em limpeza, enriquecimento e analise. O coracao da implementacao esta em `codigo/pipeline_filmes.py`, o notebook serve como camada de exploracao, e os resultados finais sao exportados em formatos adequados para consumo analitico e visual.
