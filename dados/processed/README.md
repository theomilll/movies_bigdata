# Dados processados

Esta pasta armazena os artefatos gerados por `codigo/pipeline_filmes.py`.

Versionados no repositório:

- `filmes_processados_parquet/part-00000.parquet`: dataset gold em formato colunar.
- `visualizacoes/*.png`: quatro gráficos finais usados no relatório.
- Tabelas analíticas CSV pequenas, como `receita_por_genero.csv`, `top_diretores.csv` e `correlacoes_sucesso.csv`.

Gerado localmente e não versionado:

- `filmes_processados.csv`: versão CSV completa do dataset gold. O Parquet é mantido como representação canônica versionada por ser mais compacto.

Para regenerar tudo, instale as dependências, coloque os CSVs obrigatórios em `dados/raw/` e execute:

```bash
python codigo/pipeline_filmes.py
```
