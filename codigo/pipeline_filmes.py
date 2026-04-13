import ast
import shutil
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)

REQUIRED_RAW_FILES = {
    "movies": "movies_metadata.csv",
    "credits": "credits.csv",
    "keywords": "keywords.csv",
    "links": "links.csv",
    "ratings": "ratings_small.csv",
}

REQUIRED_COLUMNS = {
    "movies": {
        "id",
        "title",
        "adult",
        "budget",
        "revenue",
        "runtime",
        "genres",
        "belongs_to_collection",
        "vote_average",
        "vote_count",
        "popularity",
        "original_language",
        "release_date",
        "overview",
        "status",
    },
    "credits": {"id", "crew"},
    "keywords": {"id", "keywords"},
    "links": {"movieId", "tmdbId"},
    "ratings": {"userId", "movieId", "rating", "timestamp"},
}

NUMERIC_MOVIE_COLUMNS = ["budget", "revenue", "runtime", "vote_average", "vote_count", "popularity"]
TOP_FILM_LIMIT = 20
TOP_FILM_MIN_VOTE_COUNT = 100
TOP_FILM_MIN_BUDGET = 1_000_000
MIN_DIRECTOR_FILMS = 3
MIN_KEYWORD_FILMS = 10
MIN_GENRE_FILMS = 20
MIN_DECADE_FILMS = 20

def resolve_data_root(data_root=None):
    if data_root is not None:
        candidate_dirs = [Path(data_root).expanduser().resolve()]
    else:
        cwd = Path.cwd().resolve()
        candidate_dirs = [
            cwd,
            cwd / "data",
            cwd.parent,
            Path("/content"),
            Path("/content/data"),
        ]

    for base_dir in candidate_dirs:
        raw_dir = base_dir / "dados" / "raw"
        if raw_dir.is_dir():
            processed_dir = base_dir / "dados" / "processed"
            processed_dir.mkdir(parents=True, exist_ok=True)
            return base_dir, raw_dir, processed_dir

    raise FileNotFoundError(
        "Nao foi possivel localizar a pasta 'dados/raw'. "
        "Coloque os arquivos obrigatorios em dados/raw antes de executar o pipeline."
    )


def _validate_columns(df, dataset_name):
    missing_columns = sorted(REQUIRED_COLUMNS[dataset_name] - set(df.columns))
    if missing_columns:
        raise ValueError(
            f"O dataset '{dataset_name}' nao possui as colunas obrigatorias: {', '.join(missing_columns)}"
        )


def _parse_python_literal(raw_value):
    if raw_value is None or pd.isna(raw_value) or raw_value == "":
        return None, False

    if isinstance(raw_value, (list, dict)):
        return raw_value, False

    try:
        return ast.literal_eval(str(raw_value)), False
    except (SyntaxError, ValueError):
        return None, True


def parse_name_list(raw_value):
    parsed_value, malformed = _parse_python_literal(raw_value)
    if parsed_value is None:
        return [], malformed
    if not isinstance(parsed_value, list):
        return [], True

    names = []
    for item in parsed_value:
        if isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
    return names, malformed


def extract_director(raw_value):
    parsed_value, malformed = _parse_python_literal(raw_value)
    if parsed_value is None:
        return None, malformed
    if not isinstance(parsed_value, list):
        return None, True

    for member in parsed_value:
        if isinstance(member, dict) and member.get("job") == "Director" and member.get("name"):
            return str(member["name"]), malformed
    return None, malformed


def _apply_parser(series, parser, *, field_name, empty_value):
    parsed_values = []
    malformed_rows = 0

    for raw_value in series.tolist():
        parsed_value, malformed = parser(raw_value)
        parsed_values.append(parsed_value if parsed_value is not None else empty_value)
        malformed_rows += int(malformed)

    if malformed_rows:
        warnings.warn(
            f"{field_name}: {malformed_rows} linhas com conteudo malformado foram ignoradas.",
            RuntimeWarning,
        )

    return pd.Series(parsed_values, index=series.index)

def _round_columns(df, columns, digits=2):
    result = df.copy()
    for column in columns:
        if column in result.columns:
            result[column] = result[column].round(digits)
    return result


def _build_group_summary(df, *, group_column, min_count):
    base_df = df[df[group_column].notna()].copy()
    base_df = base_df[base_df["revenue"].notna()]

    summary = (
        base_df.groupby(group_column, dropna=False)
        .agg(
            qtd_filmes=("id", "count"),
            receita_total_bi=("revenue", lambda s: s.sum() / 1e9),
            receita_media_mi=("revenue", lambda s: s.mean() / 1e6),
            receita_mediana_mi=("revenue", lambda s: s.median() / 1e6),
            roi_medio=("roi", "mean"),
            nota_metadata_media=("vote_average", "mean"),
            nota_usuarios_media=("user_rating_avg", "mean"),
        )
        .reset_index()
    )
    summary = summary[summary["qtd_filmes"] >= min_count]
    summary = _round_columns(
        summary,
        [
            "receita_total_bi",
            "receita_media_mi",
            "receita_mediana_mi",
            "roi_medio",
            "nota_metadata_media",
            "nota_usuarios_media",
        ],
    )
    return summary.sort_values(["receita_total_bi", "qtd_filmes"], ascending=[False, False]).reset_index(drop=True)


def _ensure_parquet_dependency():
    try:
        import pyarrow
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "A exportacao para Parquet requer a dependencia 'pyarrow'. "
            "Instale com: pip install pyarrow"
        ) from exc


def load_raw_datasets(data_root=None):
    base_dir, raw_dir, processed_dir = resolve_data_root(data_root)

    missing_files = [
        filename for filename in REQUIRED_RAW_FILES.values() if not (raw_dir / filename).is_file()
    ]
    if missing_files:
        raise FileNotFoundError(
            "Arquivos obrigatorios ausentes em dados/raw: " + ", ".join(sorted(missing_files))
        )

    tables = {
        "movies": pd.read_csv(raw_dir / REQUIRED_RAW_FILES["movies"], low_memory=False),
        "credits": pd.read_csv(raw_dir / REQUIRED_RAW_FILES["credits"], low_memory=False),
        "keywords": pd.read_csv(raw_dir / REQUIRED_RAW_FILES["keywords"], low_memory=False),
        "links": pd.read_csv(raw_dir / REQUIRED_RAW_FILES["links"], low_memory=False),
        "ratings": pd.read_csv(raw_dir / REQUIRED_RAW_FILES["ratings"], low_memory=False),
    }

    for dataset_name, dataset in tables.items():
        _validate_columns(dataset, dataset_name)

    print("Ingestao concluida!")
    for dataset_name, dataset in tables.items():
        print(f"  {dataset_name:8s} {len(dataset):6d} linhas, {len(dataset.columns)} colunas")

    return {
        "base_dir": base_dir,
        "raw_dir": raw_dir,
        "processed_dir": processed_dir,
        "tables": tables,
    }


def clean_movies(movies_df):
    _validate_columns(movies_df, "movies")

    total_rows = len(movies_df)
    print(f"Total de linhas em movies_metadata: {total_rows}")

    df_clean = movies_df.copy()
    numeric_id_mask = df_clean["id"].astype(str).str.fullmatch(r"\d+")
    invalid_id_count = int((~numeric_id_mask).sum())
    df_clean = df_clean[numeric_id_mask].copy()
    df_clean["id"] = pd.to_numeric(df_clean["id"], errors="raise").astype(int)

    duplicate_id_count = int(df_clean["id"].duplicated().sum())
    df_clean = df_clean.drop_duplicates(subset=["id"], keep="first").copy()

    for column in NUMERIC_MOVIE_COLUMNS:
        df_clean[column] = pd.to_numeric(df_clean[column], errors="coerce")

    df_clean["adult"] = df_clean["adult"].astype(str).str.strip().str.lower().eq("true")
    df_clean["title"] = df_clean["title"].astype(str).str.strip()
    df_clean.loc[df_clean["title"].eq(""), "title"] = pd.NA
    df_clean.loc[df_clean["budget"].eq(0), "budget"] = pd.NA
    df_clean.loc[df_clean["revenue"].eq(0), "revenue"] = pd.NA

    df_clean = df_clean[~df_clean["adult"]]
    df_clean = df_clean[df_clean["title"].notna()]
    df_clean = df_clean[df_clean["runtime"].notna() & (df_clean["runtime"] > 0)]

    print(f"Linhas removidas por ID invalido: {invalid_id_count}")
    print(f"Linhas removidas por ID duplicado: {duplicate_id_count}")
    print(f"Linhas apos limpeza principal: {len(df_clean)}")
    return df_clean.reset_index(drop=True)


def build_directors(credits_df):
    _validate_columns(credits_df, "credits")

    df = credits_df.copy()
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df[df["id"].notna()].copy()
    df["id"] = df["id"].astype(int)
    df["director"] = _apply_parser(df["crew"], extract_director, field_name="credits.crew", empty_value=None)
    df = df[["id", "director"]].drop_duplicates(subset=["id"], keep="first")
    return df[df["director"].notna()].reset_index(drop=True)


def build_keywords(keywords_df):
    _validate_columns(keywords_df, "keywords")

    df = keywords_df.copy()
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df[df["id"].notna()].copy()
    df["id"] = df["id"].astype(int)
    df["keywords_list"] = _apply_parser(
        df["keywords"],
        parse_name_list,
        field_name="keywords.keywords",
        empty_value=[],
    )
    df["primary_keyword"] = df["keywords_list"].apply(lambda values: values[0] if values else None)
    df["keyword_count"] = df["keywords_list"].apply(len)
    return df[["id", "keywords_list", "primary_keyword", "keyword_count"]].drop_duplicates(
        subset=["id"], keep="first"
    )


def build_movie_ratings(links_df, ratings_df):
    _validate_columns(links_df, "links")
    _validate_columns(ratings_df, "ratings")

    df_links = links_df.copy()
    df_links["movieId"] = pd.to_numeric(df_links["movieId"], errors="coerce")
    df_links["tmdbId"] = pd.to_numeric(df_links["tmdbId"], errors="coerce")
    df_links = df_links[df_links["movieId"].notna() & df_links["tmdbId"].notna()].copy()
    df_links["movieId"] = df_links["movieId"].astype(int)
    df_links["tmdbId"] = df_links["tmdbId"].astype(int)
    df_links = df_links[["movieId", "tmdbId"]].drop_duplicates(subset=["movieId"], keep="first")

    df_ratings = ratings_df.copy()
    df_ratings["movieId"] = pd.to_numeric(df_ratings["movieId"], errors="coerce")
    df_ratings["userId"] = pd.to_numeric(df_ratings["userId"], errors="coerce")
    df_ratings["rating"] = pd.to_numeric(df_ratings["rating"], errors="coerce")
    df_ratings = df_ratings[df_ratings["movieId"].notna() & df_ratings["rating"].notna()].copy()
    df_ratings["movieId"] = df_ratings["movieId"].astype(int)

    merged = df_ratings.merge(df_links, on="movieId", how="inner")
    ratings_by_movie = (
        merged.groupby("tmdbId")
        .agg(user_rating_avg=("rating", "mean"), user_rating_count=("rating", "count"))
        .reset_index()
        .rename(columns={"tmdbId": "id"})
    )
    ratings_by_movie["user_rating_avg"] = ratings_by_movie["user_rating_avg"].round(2)
    ratings_by_movie["user_rating_count"] = ratings_by_movie["user_rating_count"].astype(int)
    return ratings_by_movie


def build_gold_dataset(movies_df, directors_df, keywords_df, movie_ratings_df):
    df = movies_df.copy()
    df["genres_list"] = _apply_parser(df["genres"], parse_name_list, field_name="movies.genres", empty_value=[])
    df["primary_genre"] = df["genres_list"].apply(lambda values: values[0] if values else None)

    df = df.merge(directors_df, on="id", how="left")
    df = df.merge(keywords_df, on="id", how="left")
    df = df.merge(movie_ratings_df, on="id", how="left")

    df["keywords_list"] = df["keywords_list"].apply(
        lambda value: value if isinstance(value, list) else []
    )
    df["keyword_count"] = df["keyword_count"].fillna(0).astype(int)
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df["release_year"] = df["release_date"].dt.year.astype("Int64")
    df["decade"] = ((df["release_year"] // 10) * 10).astype("Int64")
    df["profit"] = df["revenue"] - df["budget"]
    df["roi"] = pd.NA
    valid_roi_mask = df["budget"].notna() & (df["budget"] > 0) & df["revenue"].notna()
    df.loc[valid_roi_mask, "roi"] = (
        ((df.loc[valid_roi_mask, "revenue"] - df.loc[valid_roi_mask, "budget"]) / df.loc[valid_roi_mask, "budget"])
        * 100
    )
    df["roi"] = pd.to_numeric(df["roi"], errors="coerce").round(2)
    df["has_collection"] = (
        df["belongs_to_collection"].notna()
        & df["belongs_to_collection"].astype(str).str.strip().ne("")
    ).astype(int)
    df["release_date"] = df["release_date"].dt.strftime("%Y-%m-%d")

    final_columns = [
        "id",
        "title",
        "primary_genre",
        "genres_list",
        "director",
        "primary_keyword",
        "keywords_list",
        "keyword_count",
        "budget",
        "revenue",
        "profit",
        "roi",
        "vote_average",
        "vote_count",
        "user_rating_avg",
        "user_rating_count",
        "popularity",
        "runtime",
        "original_language",
        "release_date",
        "release_year",
        "decade",
        "has_collection",
        "overview",
        "status",
    ]
    return df[final_columns].reset_index(drop=True)


def build_aggregations(gold_df):
    tables = {}

    revenue_by_genre = (
        gold_df[gold_df["revenue"].notna() & gold_df["primary_genre"].notna()]
        .groupby("primary_genre")
        .agg(
            receita_total_bi=("revenue", lambda s: s.sum() / 1e9),
            receita_media_mi=("revenue", lambda s: s.mean() / 1e6),
            qtd_filmes=("id", "count"),
        )
        .reset_index()
        .sort_values("receita_total_bi", ascending=False)
    )
    tables["receita_por_genero"] = _round_columns(revenue_by_genre, ["receita_total_bi", "receita_media_mi"])

    top_directors = (
        gold_df[gold_df["revenue"].notna() & gold_df["director"].notna()]
        .groupby("director")
        .agg(
            receita_total_bi=("revenue", lambda s: s.sum() / 1e9),
            nota_metadata_media=("vote_average", "mean"),
            nota_usuarios_media=("user_rating_avg", "mean"),
            qtd_filmes=("id", "count"),
        )
        .reset_index()
    )
    top_directors = top_directors[top_directors["qtd_filmes"] >= MIN_DIRECTOR_FILMS]
    top_directors = top_directors.sort_values("receita_total_bi", ascending=False).reset_index(drop=True)
    tables["top_diretores"] = _round_columns(
        top_directors,
        ["receita_total_bi", "nota_metadata_media", "nota_usuarios_media"],
    )

    movies_by_decade = (
        gold_df[gold_df["decade"].notna()]
        .groupby("decade")
        .agg(
            qtd_filmes=("id", "count"),
            nota_metadata_media=("vote_average", "mean"),
            nota_usuarios_media=("user_rating_avg", "mean"),
            duracao_media=("runtime", "mean"),
        )
        .reset_index()
        .sort_values("decade")
    )
    tables["filmes_por_decada"] = _round_columns(
        movies_by_decade,
        ["nota_metadata_media", "nota_usuarios_media", "duracao_media"],
    )

    keywords_exploded = gold_df.explode("keywords_list")
    keywords_exploded = keywords_exploded[keywords_exploded["keywords_list"].notna()].copy()
    top_keywords = (
        keywords_exploded.groupby("keywords_list")
        .agg(
            qtd_filmes=("id", "count"),
            receita_media_mi=("revenue", lambda s: s.mean() / 1e6),
            nota_usuarios_media=("user_rating_avg", "mean"),
        )
        .reset_index()
        .rename(columns={"keywords_list": "keyword"})
    )
    top_keywords = top_keywords[top_keywords["qtd_filmes"] >= MIN_KEYWORD_FILMS]
    top_keywords = top_keywords.sort_values(
        ["qtd_filmes", "receita_media_mi"], ascending=[False, False]
    ).reset_index(drop=True)
    tables["top_palavras_chave"] = _round_columns(
        top_keywords,
        ["receita_media_mi", "nota_usuarios_media"],
    )

    correlation_rows = []
    features = ["budget", "popularity", "vote_average", "vote_count", "user_rating_avg", "runtime"]
    targets = ["revenue", "roi"]
    for target in targets:
        for feature in features:
            correlation_df = gold_df[[feature, target]].dropna()
            sample_size = len(correlation_df)
            correlation = correlation_df[feature].corr(correlation_df[target]) if sample_size >= 2 else pd.NA
            correlation_rows.append(
                {
                    "target": target,
                    "feature": feature,
                    "correlation": correlation,
                    "sample_size": sample_size,
                }
            )
    correlations = pd.DataFrame(correlation_rows)
    correlations["correlation"] = pd.to_numeric(correlations["correlation"], errors="coerce").round(4)
    tables["correlacoes_sucesso"] = correlations.sort_values(
        ["target", "correlation"], ascending=[True, False]
    ).reset_index(drop=True)

    top_by_revenue = gold_df[
        gold_df["revenue"].notna() & (gold_df["vote_count"].fillna(0) >= TOP_FILM_MIN_VOTE_COUNT)
    ].copy()
    top_by_revenue = top_by_revenue.sort_values("revenue", ascending=False).head(TOP_FILM_LIMIT)
    tables["top_filmes_receita"] = _round_columns(
        top_by_revenue[
            [
                "title",
                "release_year",
                "primary_genre",
                "director",
                "revenue",
                "budget",
                "profit",
                "roi",
                "vote_average",
                "user_rating_avg",
                "vote_count",
                "user_rating_count",
            ]
        ],
        ["revenue", "budget", "profit", "roi", "vote_average", "user_rating_avg"],
    ).reset_index(drop=True)

    top_by_roi = gold_df[
        gold_df["roi"].notna()
        & gold_df["revenue"].notna()
        & gold_df["budget"].notna()
        & (gold_df["budget"] >= TOP_FILM_MIN_BUDGET)
        & (gold_df["vote_count"].fillna(0) >= TOP_FILM_MIN_VOTE_COUNT)
    ].copy()
    top_by_roi = top_by_roi.sort_values("roi", ascending=False).head(TOP_FILM_LIMIT)
    tables["top_filmes_roi"] = _round_columns(
        top_by_roi[
            [
                "title",
                "release_year",
                "primary_genre",
                "director",
                "revenue",
                "budget",
                "profit",
                "roi",
                "vote_average",
                "user_rating_avg",
                "vote_count",
                "user_rating_count",
            ]
        ],
        ["revenue", "budget", "profit", "roi", "vote_average", "user_rating_avg"],
    ).reset_index(drop=True)

    tables["resumo_sucesso_por_genero"] = _build_group_summary(
        gold_df,
        group_column="primary_genre",
        min_count=MIN_GENRE_FILMS,
    )
    tables["resumo_sucesso_por_diretor"] = _build_group_summary(
        gold_df,
        group_column="director",
        min_count=MIN_DIRECTOR_FILMS,
    )
    tables["resumo_sucesso_por_decada"] = _build_group_summary(
        gold_df,
        group_column="decade",
        min_count=MIN_DECADE_FILMS,
    )

    keyword_summary_source = gold_df.explode("keywords_list")
    keyword_summary_source = keyword_summary_source.rename(columns={"keywords_list": "keyword"})
    tables["resumo_sucesso_por_palavra_chave"] = _build_group_summary(
        keyword_summary_source,
        group_column="keyword",
        min_count=MIN_KEYWORD_FILMS,
    )

    return tables


def save_outputs(processed_dir, gold_df, tables):
    _ensure_parquet_dependency()

    processed_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {}

    gold_csv_path = processed_dir / "filmes_processados.csv"
    gold_df.to_csv(gold_csv_path, index=False)
    output_paths["filmes_processados_csv"] = gold_csv_path

    parquet_dir = processed_dir / "filmes_processados_parquet"
    if parquet_dir.exists():
        shutil.rmtree(parquet_dir)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    gold_df.to_parquet(parquet_dir / "part-00000.parquet", index=False)
    (parquet_dir / "_SUCCESS").touch()
    output_paths["filmes_processados_parquet"] = parquet_dir

    for name, dataset in tables.items():
        output_path = processed_dir / f"{name}.csv"
        dataset.to_csv(output_path, index=False)
        output_paths[name] = output_path

    print(f"Dataset final salvo em CSV: {gold_csv_path}")
    print(f"Dataset final salvo em Parquet: {parquet_dir}")
    for name in tables:
        print(f"Tabela salva: {processed_dir / f'{name}.csv'}")

    return output_paths


def plot_results(processed_dir, gold_df, tables):
    charts_dir = processed_dir / "visualizacoes"
    charts_dir.mkdir(parents=True, exist_ok=True)
    plot_paths = {}

    revenue_by_genre = tables["receita_por_genero"]
    fig, ax = plt.subplots(figsize=(14, 7))
    bars = ax.barh(
        revenue_by_genre["primary_genre"][::-1],
        revenue_by_genre["receita_total_bi"][::-1],
        color=sns.color_palette("viridis", len(revenue_by_genre)),
    )
    ax.set_xlabel("Receita Total (Bilhoes USD)")
    ax.set_title("Receita Total por Genero")
    ax.bar_label(bars, fmt="%.1f B", padding=5)
    plt.tight_layout()
    output_path = charts_dir / "receita_por_genero.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    plot_paths["receita_por_genero"] = output_path

    movies_by_decade = tables["filmes_por_decada"]
    fig, ax1 = plt.subplots(figsize=(14, 7))
    ax1.bar(
        movies_by_decade["decade"].astype(str),
        movies_by_decade["qtd_filmes"],
        color="#2196F3",
        alpha=0.7,
    )
    ax1.set_xlabel("Decada")
    ax1.set_ylabel("Quantidade de Filmes", color="#2196F3")
    ax1.tick_params(axis="y", labelcolor="#2196F3")

    ax2 = ax1.twinx()
    ax2.plot(
        movies_by_decade["decade"].astype(str),
        movies_by_decade["nota_usuarios_media"],
        color="#FF5722",
        marker="o",
        linewidth=2,
    )
    ax2.set_ylabel("Nota Media dos Usuarios", color="#FF5722")
    ax2.tick_params(axis="y", labelcolor="#FF5722")
    ax1.set_title("Producao de Filmes e Nota Media dos Usuarios por Decada")
    plt.tight_layout()
    output_path = charts_dir / "filmes_por_decada.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    plot_paths["filmes_por_decada"] = output_path

    financial_df = gold_df[
        gold_df["budget"].notna()
        & gold_df["revenue"].notna()
        & (gold_df["budget"] > TOP_FILM_MIN_BUDGET)
        & (gold_df["revenue"] > 100_000)
    ][["budget", "revenue", "vote_average"]].copy()

    fig, ax = plt.subplots(figsize=(14, 8))
    scatter = ax.scatter(
        financial_df["budget"] / 1e6,
        financial_df["revenue"] / 1e6,
        c=financial_df["vote_average"],
        cmap="RdYlGn",
        alpha=0.5,
        s=20,
    )
    max_val = max(financial_df["budget"].max(), financial_df["revenue"].max()) / 1e6
    ax.plot([0, max_val], [0, max_val], "k--", alpha=0.3, label="Cost = Revenue")
    ax.set_xlabel("Orcamento (Milhoes USD)")
    ax.set_ylabel("Receita (Milhoes USD)")
    ax.set_title("Orcamento vs Receita (cor = nota do metadata)")
    plt.colorbar(scatter, label="Nota Media")
    ax.legend()
    plt.tight_layout()
    output_path = charts_dir / "orcamento_vs_receita.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    plot_paths["orcamento_vs_receita"] = output_path

    correlations = tables["correlacoes_sucesso"].pivot(index="feature", columns="target", values="correlation")
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(correlations, annot=True, cmap="coolwarm", center=0, vmin=-1, vmax=1, ax=ax)
    ax.set_title("Correlacoes com Receita e ROI")
    plt.tight_layout()
    output_path = charts_dir / "correlacoes_sucesso.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    plot_paths["correlacoes_sucesso"] = output_path

    print(f"Graficos salvos em: {charts_dir}")
    return plot_paths


def _build_source_summary(loaded):
    datasets = []
    for dataset_name, filename in REQUIRED_RAW_FILES.items():
        dataset = loaded["tables"][dataset_name]
        datasets.append(
            {
                "dataset": dataset_name,
                "arquivo": filename,
                "linhas": len(dataset),
                "colunas": len(dataset.columns),
            }
        )

    return {
        "base_dir": str(loaded["base_dir"]),
        "raw_dir": str(loaded["raw_dir"]),
        "processed_dir": str(loaded["processed_dir"]),
        "datasets": datasets,
    }


def _build_stage_summary(loaded, clean_movies_df, directors_df, keywords_df, movie_ratings_df, gold_df):
    return {
        "fontes": {
            "quantidade_arquivos": len(REQUIRED_RAW_FILES),
            "arquivos": list(REQUIRED_RAW_FILES.values()),
        },
        "ingestao": {
            "dataset_lidos": len(loaded["tables"]),
            "total_linhas_brutas": sum(len(dataset) for dataset in loaded["tables"].values()),
        },
        "transformacao": {
            "movies_linhas_brutas": len(loaded["tables"]["movies"]),
            "movies_linhas_apos_limpeza": len(clean_movies_df),
            "diretores_extraidos": len(directors_df),
            "filmes_com_palavras_chave": int((keywords_df["keyword_count"] > 0).sum()),
            "filmes_com_ratings": len(movie_ratings_df),
            "dataset_gold_linhas": len(gold_df),
            "dataset_gold_colunas": len(gold_df.columns),
        },
    }


def _build_output_summary(processed_dir, output_paths, plot_paths):
    return {
        "processed_dir": str(processed_dir),
        "arquivos_saida": {name: str(path) for name, path in output_paths.items()},
        "graficos": {name: str(path) for name, path in plot_paths.items()},
    }


def main(data_root=None):
    loaded = load_raw_datasets(data_root)
    print(f"Diretorio base: {loaded['base_dir']}")
    print(f"Dados brutos: {loaded['raw_dir']}")
    print(f"Dados processados: {loaded['processed_dir']}")

    clean_movies_df = clean_movies(loaded["tables"]["movies"])
    directors_df = build_directors(loaded["tables"]["credits"])
    keywords_df = build_keywords(loaded["tables"]["keywords"])
    movie_ratings_df = build_movie_ratings(loaded["tables"]["links"], loaded["tables"]["ratings"])

    gold_df = build_gold_dataset(clean_movies_df, directors_df, keywords_df, movie_ratings_df)
    print(f"Dataset final: {len(gold_df)} filmes")
    print(gold_df.head(5).to_string(index=False))

    tables = build_aggregations(gold_df)
    for name, dataset in tables.items():
        print(f"=== {name} ===")
        print(dataset.head(10).to_string(index=False))

    output_paths = save_outputs(loaded["processed_dir"], gold_df, tables)
    plot_paths = plot_results(loaded["processed_dir"], gold_df, tables)
    source_summary = _build_source_summary(loaded)
    stage_summary = _build_stage_summary(
        loaded,
        clean_movies_df,
        directors_df,
        keywords_df,
        movie_ratings_df,
        gold_df,
    )
    stage_summary["carregamento"] = {
        "csv_principal": str(output_paths["filmes_processados_csv"]),
        "parquet_principal": str(output_paths["filmes_processados_parquet"]),
        "tabelas_geradas": len(tables),
    }
    stage_summary["destino"] = {
        "dataset_gold": str(output_paths["filmes_processados_csv"]),
        "parquet_gold": str(output_paths["filmes_processados_parquet"]),
        "quantidade_graficos": len(plot_paths),
    }
    output_summary = _build_output_summary(loaded["processed_dir"], output_paths, plot_paths)

    return {
        "gold_df": gold_df,
        "tables": tables,
        "plot_paths": plot_paths,
        "processed_dir": loaded["processed_dir"],
        "source_summary": source_summary,
        "stage_summary": stage_summary,
        "output_summary": output_summary,
    }


if __name__ == "__main__":
    main()
