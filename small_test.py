tables = {
    'receita_por_genero': df1,
    'top_diretores': df2,
    'filmes_por_decada': df3,
    'top_palavras_chave': df4,
    'correlacoes_sucesso': df5,
    'top_filmes_receita': df6,
    'top_filmes_roi': df7,
}


name = 'receita_por_genero'

name = 'top_diretores'

table_previews = [ 'receita_por_genero', 'top_diretores', 'filmes_por_decada', 'top_palavras_chave', 'correlacoes_sucesso', 'top_filmes_receita', 'top_filmes_roi', ] for name in table_previews: print(f'=== {name} ===') display(tables[name].head(10))
