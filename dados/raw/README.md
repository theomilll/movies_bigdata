# Dados brutos

Os arquivos brutos vêm do **The Movies Dataset** no Kaggle:

https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset

Para reproduzir a pipeline completa, baixe o ZIP do dataset, extraia os CSVs e deixe estes arquivos em `dados/raw/`:

- `movies_metadata.csv`
- `credits.csv`
- `keywords.csv`
- `links.csv`
- `ratings_small.csv`

`links_small.csv` pode permanecer na pasta como referência do MovieLens Small, mas a pipeline principal usa `links.csv`.

`credits.csv` é obrigatório porque a análise de diretores depende do campo `crew`. O arquivo é mantido fora do Git por tamanho e por política de dados do projeto; a execução deve falhar caso ele esteja ausente.
