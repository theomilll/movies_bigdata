from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "dados" / "processed"
PARQUET_FILE = PROCESSED_DIR / "filmes_processados_parquet" / "part-00000.parquet"
REQUIRED_TABLES = {
    "receita_por_genero": PROCESSED_DIR / "receita_por_genero.csv",
    "top_diretores": PROCESSED_DIR / "top_diretores.csv",
    "filmes_por_decada": PROCESSED_DIR / "filmes_por_decada.csv",
    "correlacoes_sucesso": PROCESSED_DIR / "correlacoes_sucesso.csv",
    "top_filmes_roi": PROCESSED_DIR / "top_filmes_roi.csv",
}


def require_outputs():
    missing = [str(PARQUET_FILE)] if not PARQUET_FILE.is_file() else []
    missing.extend(str(path) for path in REQUIRED_TABLES.values() if not path.is_file())
    if missing:
        raise FileNotFoundError(
            "Artefatos processados ausentes. Execute 'python codigo/pipeline_filmes.py' "
            "a partir da raiz do projeto antes de abrir o dashboard. Faltando: "
            + ", ".join(missing)
        )


@st.cache_data
def load_outputs():
    require_outputs()
    gold = pd.read_parquet(PARQUET_FILE)
    tables = {name: pd.read_csv(path) for name, path in REQUIRED_TABLES.items()}
    return gold, tables


st.set_page_config(page_title="movies_bigdata AV2", layout="wide")
st.title("movies_bigdata - Dashboard AV2")

try:
    gold_df, tables = load_outputs()
except FileNotFoundError as exc:
    st.error(str(exc))
    st.stop()

gold_df["decade_numeric"] = pd.to_numeric(gold_df["decade"], errors="coerce")
genres = sorted(gold_df["primary_genre"].dropna().unique())
decades = sorted(gold_df["decade_numeric"].dropna().astype(int).unique())

with st.sidebar:
    st.header("Filtros")
    selected_genres = st.multiselect("Gêneros", genres)
    selected_decades = st.slider(
        "Décadas",
        min_value=int(min(decades)),
        max_value=int(max(decades)),
        value=(int(min(decades)), int(max(decades))),
        step=10,
    )

filtered = gold_df.copy()
if selected_genres:
    filtered = filtered[filtered["primary_genre"].isin(selected_genres)]
filtered = filtered[
    filtered["decade_numeric"].notna()
    & (filtered["decade_numeric"] >= selected_decades[0])
    & (filtered["decade_numeric"] <= selected_decades[1])
]

financial = filtered[filtered["revenue"].notna() & filtered["budget"].notna()].copy()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Filmes filtrados", f"{len(filtered):,}".replace(",", "."))
col2.metric("Com receita", f"{int(filtered['revenue'].notna().sum()):,}".replace(",", "."))
col3.metric("Receita (US$ bi)", f"{filtered['revenue'].sum(skipna=True) / 1e9:.1f}")
col4.metric("ROI mediano", f"{financial['roi'].median(skipna=True):.1f}%")

left, right = st.columns((1.1, 0.9))

with left:
    genre_chart = tables["receita_por_genero"].head(12)
    st.plotly_chart(
        px.bar(
            genre_chart.sort_values("receita_total_bi"),
            x="receita_total_bi",
            y="primary_genre",
            orientation="h",
            labels={"receita_total_bi": "Receita total (US$ bi)", "primary_genre": "Gênero"},
            title="Receita total por gênero",
        ),
        use_container_width=True,
    )

    scatter_df = financial[
        (financial["budget"] > 1_000_000) & (financial["revenue"] > 100_000)
    ].copy()
    scatter_df["budget_mi"] = scatter_df["budget"] / 1e6
    scatter_df["revenue_mi"] = scatter_df["revenue"] / 1e6
    st.plotly_chart(
        px.scatter(
            scatter_df,
            x="budget_mi",
            y="revenue_mi",
            color="vote_average",
            hover_data=["title", "release_year", "primary_genre", "director"],
            labels={
                "budget_mi": "Orçamento (US$ mi)",
                "revenue_mi": "Receita (US$ mi)",
                "vote_average": "Nota TMDB",
            },
            title="Orçamento vs receita",
        ),
        use_container_width=True,
    )

with right:
    decade_chart = tables["filmes_por_decada"].copy()
    st.plotly_chart(
        px.line(
            decade_chart,
            x="decade",
            y=["qtd_filmes", "nota_usuarios_media"],
            markers=True,
            title="Volume de filmes e nota média por década",
        ),
        use_container_width=True,
    )

    corr = tables["correlacoes_sucesso"].pivot(
        index="feature", columns="target", values="correlation"
    )
    st.plotly_chart(
        px.imshow(
            corr,
            text_auto=True,
            zmin=-1,
            zmax=1,
            color_continuous_scale="RdBu_r",
            title="Correlações com receita e ROI",
        ),
        use_container_width=True,
    )

st.subheader("Filmes com maior ROI")
st.dataframe(
    tables["top_filmes_roi"][
        ["title", "release_year", "primary_genre", "director", "revenue", "budget", "roi"]
    ].head(15),
    use_container_width=True,
    hide_index=True,
)
