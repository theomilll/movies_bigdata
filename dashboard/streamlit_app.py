from pathlib import Path
import sys

import joblib
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "codigo"))
import modelo_preditivo as modelo

PROCESSED_DIR = PROJECT_ROOT / "dados" / "processed"
PARQUET_FILE = PROCESSED_DIR / "filmes_processados_parquet" / "part-00000.parquet"
MODELS_DIR = PROCESSED_DIR / "modelos"
REVENUE_MODEL_FILE = MODELS_DIR / "modelo_receita.joblib"
HIT_MODEL_FILE = MODELS_DIR / "modelo_hit.joblib"
REQUIRED_TABLES = {
    "receita_por_genero": PROCESSED_DIR / "receita_por_genero.csv",
    "top_diretores": PROCESSED_DIR / "top_diretores.csv",
    "filmes_por_decada": PROCESSED_DIR / "filmes_por_decada.csv",
    "correlacoes_sucesso": PROCESSED_DIR / "correlacoes_sucesso.csv",
    "top_filmes_roi": PROCESSED_DIR / "top_filmes_roi.csv",
}


def require_outputs():
    missing = [
        str(path)
        for path in (PARQUET_FILE, REVENUE_MODEL_FILE, HIT_MODEL_FILE)
        if not path.is_file()
    ]
    missing.extend(str(path) for path in REQUIRED_TABLES.values() if not path.is_file())
    if missing:
        raise FileNotFoundError(
            "Artefatos ausentes. Execute 'python codigo/pipeline_filmes.py' e "
            "'python codigo/modelo_preditivo.py' a partir da raiz do projeto antes de abrir o "
            "dashboard. Faltando: " + ", ".join(missing)
        )


@st.cache_data
def load_outputs():
    require_outputs()
    gold = pd.read_parquet(PARQUET_FILE)
    tables = {name: pd.read_csv(path) for name, path in REQUIRED_TABLES.items()}
    return gold, tables


@st.cache_resource
def load_models():
    # Modelos .joblib gerados localmente por codigo/modelo_preditivo.py (fonte confiavel).
    return joblib.load(REVENUE_MODEL_FILE), joblib.load(HIT_MODEL_FILE)


st.set_page_config(page_title="movies_bigdata AV2", layout="wide")
st.title("movies_bigdata - Dashboard AV2")

try:
    gold_df, tables = load_outputs()
    revenue_model, hit_model = load_models()
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

tab_panorama, tab_previsao = st.tabs(["Panorama", "Previsão"])

with tab_panorama:
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

with tab_previsao:
    st.subheader("Prever bilheteria e chance de sucesso")
    st.caption(
        "vote_count, popularidade e notas só existem após o lançamento; os campos de engajamento "
        "vêm preenchidos com a mediana da base. O modelo estima a bilheteria com buzz pós-lançamento, "
        "não é uma previsão pura pré-lançamento."
    )

    genre_options = sorted(gold_df["primary_genre"].dropna().unique())
    lang_options = sorted(gold_df["original_language"].dropna().unique())
    default_genre = genre_options.index("Action") if "Action" in genre_options else 0
    default_lang = lang_options.index("en") if "en" in lang_options else 0

    median_runtime = int(gold_df["runtime"].median())
    median_keywords = int(gold_df["keyword_count"].median())
    median_vote_count = int(gold_df["vote_count"].median())
    median_popularity = float(round(gold_df["popularity"].median(), 2))
    median_vote_average = float(round(gold_df["vote_average"].median(), 1))
    median_user_avg = float(round(gold_df["user_rating_avg"].median(), 2))
    median_user_count = float(gold_df["user_rating_count"].median())

    c1, c2, c3 = st.columns(3)
    with c1:
        in_budget = st.number_input("Orçamento (US$)", min_value=0, value=50_000_000, step=1_000_000)
        in_runtime = st.number_input("Duração (min)", min_value=0, value=median_runtime)
        in_genre = st.selectbox("Gênero principal", genre_options, index=default_genre)
    with c2:
        in_language = st.selectbox("Idioma original", lang_options, index=default_lang)
        in_year = st.number_input("Ano de lançamento", min_value=1900, max_value=2030, value=2020)
        in_keywords = st.number_input("Qtd. de palavras-chave", min_value=0, value=median_keywords)
    with c3:
        in_collection = st.checkbox("Faz parte de coleção/franquia")
        in_vote_count = st.number_input("Votos esperados (TMDB)", min_value=0, value=median_vote_count)
        in_popularity = st.number_input("Popularidade esperada", min_value=0.0, value=median_popularity)

    if st.button("Prever"):
        row = pd.DataFrame(
            [
                {
                    "budget": float(in_budget),
                    "runtime": float(in_runtime),
                    "vote_average": median_vote_average,
                    "vote_count": float(in_vote_count),
                    "popularity": float(in_popularity),
                    "user_rating_avg": median_user_avg,
                    "user_rating_count": median_user_count,
                    "keyword_count": float(in_keywords),
                    "release_year": float(in_year),
                    "has_collection": int(in_collection),
                    "primary_genre": in_genre,
                    "original_language": in_language,
                }
            ]
        )[modelo.FEATURE_COLUMNS]

        predicted_revenue = float(np.expm1(revenue_model.predict(row)[0]))
        hit_probability = float(hit_model.predict_proba(row)[0, 1])

        m1, m2 = st.columns(2)
        m1.metric("Receita prevista", f"US$ {predicted_revenue / 1e6:,.1f} mi")
        m2.metric("Chance de hit (≥2x orçamento)", f"{hit_probability:.0%}")
