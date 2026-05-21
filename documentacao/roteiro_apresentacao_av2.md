# Roteiro e checklist da apresentacao AV2

## Antes da aula

- Confirmar que o repositorio GitHub esta publico.
- Confirmar nomes oficiais da equipe e colaboradores no GitHub.
- Executar `git status --short` e deixar limpo.
- Ativar o ambiente Python:

```bash
source .venv/bin/activate
```

- Conferir que `dados/raw/credits.csv` existe localmente.
- Rodar a pipeline pelo menos uma vez:

```bash
python codigo/pipeline_filmes.py
```

- Abrir o dashboard:

```bash
streamlit run dashboard/streamlit_app.py
```

## Demonstracao ao vivo

1. Mostrar o README como relatorio principal.
2. Abrir `codigo/pipeline_filmes.py` e apontar a funcao `main()`.
3. Executar ou mostrar a execucao recente da pipeline.
4. Mostrar os artefatos em `dados/processed/`.
5. Abrir o dashboard e aplicar filtros por genero e decada.
6. Explicar 3 achados principais:
   - concentracao de receita por genero;
   - diferenca entre receita absoluta e ROI;
   - limitacao de cobertura dos ratings.
7. Fechar com limitacoes e trabalhos futuros.

## Plano B se a internet ou o Streamlit falhar

- Usar os PNGs em `dados/processed/visualizacoes/`.
- Usar `notebooks/pipeline_filmes.ipynb` com as saidas salvas.
- Apresentar as tabelas CSV em `dados/processed/`.
