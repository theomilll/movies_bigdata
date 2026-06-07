import json
from pathlib import Path

DEPENDENCY_INSTALL_HINT = (
    "Dependências Python ausentes. A partir da raiz do projeto, execute: "
    "python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
)

try:
    import numpy as np
    import pandas as pd
except ModuleNotFoundError as exc:
    raise RuntimeError(f"{DEPENDENCY_INSTALL_HINT} (pacote faltando: pandas/numpy)") from exc

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:
    raise RuntimeError(f"{DEPENDENCY_INSTALL_HINT} (pacote faltando: matplotlib)") from exc

try:
    import joblib
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
    from sklearn.impute import SimpleImputer
    from sklearn.inspection import permutation_importance
    from sklearn.linear_model import LogisticRegression, Ridge
    from sklearn.metrics import (
        accuracy_score,
        f1_score,
        mean_absolute_error,
        mean_squared_error,
        precision_score,
        r2_score,
        recall_score,
        roc_auc_score,
    )
    from sklearn.model_selection import cross_val_score, train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler, TargetEncoder
except ModuleNotFoundError as exc:
    raise RuntimeError(f"{DEPENDENCY_INSTALL_HINT} (pacote faltando: scikit-learn/joblib)") from exc


# ---------------------------------------------------------------------------
# Modelo PRÉ-ESTREIA: estima bilheteria e chance de sucesso usando APENAS o que
# se sabe antes do lançamento. Tudo que só existe depois da estreia (votos,
# popularidade, notas) é tratado como VAZAMENTO e fica de fora — o objetivo do
# Tema 14 é prever ANTES da estreia, então o modelo precisa funcionar sem esses
# sinais (inclusive em filmes que ainda nem saíram).
# ---------------------------------------------------------------------------

# Numéricas conhecidas antes da estreia.
NUMERIC_FEATURES = [
    "log_budget",
    "runtime",
    "release_year",
    "has_collection",
    "keyword_count",
    "is_sequel",
    "based_on_novel",
]
# Categóricas de baixa cardinalidade (one-hot).
ONEHOT_FEATURES = ["primary_genre", "original_language"]
# Alta cardinalidade -> target encoding com cross-fitting interno (sem vazamento).
TARGET_FEATURES = ["director"]
FEATURE_COLUMNS = NUMERIC_FEATURES + ONEHOT_FEATURES + TARGET_FEATURES

# Colunas que NUNCA podem virar feature: o alvo em si e sinais pós-estreia.
EXCLUDED_LEAKAGE = [
    "revenue",
    "profit",
    "roi",
    "vote_count",
    "vote_average",
    "popularity",
    "user_rating_avg",
    "user_rating_count",
]

HIT_MULTIPLIER = 2.0
RANDOM_STATE = 42
TEMPORAL_SPLIT_YEAR = 2010

LEAKAGE_NOTE = (
    "Modelo pré-estreia: usa apenas atributos conhecidos ANTES do lançamento "
    "(orçamento, gênero, diretor, duração, franquia, idioma, keywords). Votos, "
    "popularidade e notas foram EXCLUÍDOS por só existirem após a estreia (vazamento)."
)


def load_gold(data_root=None):
    if data_root is not None:
        processed_dir = Path(data_root).expanduser().resolve() / "dados" / "processed"
    else:
        processed_dir = Path(__file__).resolve().parents[1] / "dados" / "processed"

    parquet_path = processed_dir / "filmes_processados_parquet" / "part-00000.parquet"
    if not parquet_path.is_file():
        raise FileNotFoundError(
            "Dataset gold ausente em "
            + str(parquet_path)
            + ". Execute 'python codigo/pipeline_filmes.py' antes de treinar o modelo."
        )
    return pd.read_parquet(parquet_path), processed_dir


def _keyword_flags(keywords_value):
    """Deriva (is_sequel, based_on_novel) a partir da lista de keywords do filme."""
    if isinstance(keywords_value, (list, tuple, np.ndarray)):
        kws = [str(k).lower() for k in keywords_value]
    elif isinstance(keywords_value, str):
        kws = [k.strip().lower() for k in keywords_value.split(",")]
    else:
        kws = []
    is_sequel = int(any(k in kws for k in ("sequel", "based on franchise")))
    based_on_novel = int("based on novel" in kws)
    return is_sequel, based_on_novel


def build_model_table(gold_df):
    df = gold_df.copy()
    df = df[
        df["budget"].notna()
        & (df["budget"] > 0)
        & df["revenue"].notna()
        & (df["revenue"] > 0)
    ].copy()

    y_revenue = np.log1p(df["revenue"].astype(float))
    y_hit = (df["revenue"] >= HIT_MULTIPLIER * df["budget"]).astype(int)

    if "keywords_list" in df.columns:
        flags = df["keywords_list"].apply(_keyword_flags)
    else:
        flags = pd.Series([(0, 0)] * len(df), index=df.index)

    X = pd.DataFrame(index=df.index)
    X["log_budget"] = np.log1p(df["budget"].astype(float))
    X["runtime"] = pd.to_numeric(df.get("runtime"), errors="coerce").astype(float)
    X["release_year"] = pd.to_numeric(df.get("release_year"), errors="coerce").astype(float)
    X["has_collection"] = pd.to_numeric(df.get("has_collection", 0), errors="coerce").fillna(0).astype(float)
    X["keyword_count"] = pd.to_numeric(df.get("keyword_count", 0), errors="coerce").fillna(0).astype(float)
    X["is_sequel"] = [f[0] for f in flags]
    X["based_on_novel"] = [f[1] for f in flags]
    X["primary_genre"] = df.get("primary_genre", "desconhecido").astype("object")
    X["original_language"] = df.get("original_language", "desconhecido").astype("object")
    X["director"] = df.get("director", pd.Series(index=df.index, dtype="object")).fillna("desconhecido").astype("object")

    return (
        X[FEATURE_COLUMNS].reset_index(drop=True),
        y_revenue.reset_index(drop=True),
        y_hit.reset_index(drop=True),
    )


def build_preprocessor(target_type):
    numeric = Pipeline(
        [("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]
    )
    categorical = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="constant", fill_value="desconhecido")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", min_frequency=20, sparse_output=False)),
        ]
    )
    director = TargetEncoder(target_type=target_type)
    return ColumnTransformer(
        [
            ("num", numeric, NUMERIC_FEATURES),
            ("cat", categorical, ONEHOT_FEATURES),
            ("dir", director, TARGET_FEATURES),
        ]
    )


# Regularização leve: prever sucesso pré-estreia satura cedo; árvores rasas e
# folhas grandes evitam decorar o treino (menos gap entre holdout e CV).
_HGB_PARAMS = dict(
    max_iter=300,
    learning_rate=0.06,
    max_leaf_nodes=15,
    min_samples_leaf=40,
    l2_regularization=1.0,
    random_state=RANDOM_STATE,
)


def build_revenue_model():
    return Pipeline(
        [("prep", build_preprocessor("continuous")), ("model", HistGradientBoostingRegressor(**_HGB_PARAMS))]
    )


def build_hit_model():
    return Pipeline(
        [("prep", build_preprocessor("binary")), ("model", HistGradientBoostingClassifier(**_HGB_PARAMS))]
    )


def _regression_metrics(y_true_log, y_pred_log):
    y_true = np.expm1(y_true_log)
    y_pred = np.expm1(y_pred_log)
    return {
        "r2_log": round(float(r2_score(y_true_log, y_pred_log)), 4),
        "mae_usd": round(float(mean_absolute_error(y_true, y_pred)), 2),
        "rmse_usd": round(float(np.sqrt(mean_squared_error(y_true, y_pred))), 2),
    }


def _classification_metrics(y_true, y_pred, y_proba):
    return {
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 4),
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
        "roc_auc": round(float(roc_auc_score(y_true, y_proba)), 4),
    }


def _temporal_eval(X, y_revenue, y_hit):
    years = X["release_year"]
    train_mask = years.notna() & (years < TEMPORAL_SPLIT_YEAR)
    test_mask = years.notna() & (years >= TEMPORAL_SPLIT_YEAR)
    if train_mask.sum() < 50 or test_mask.sum() < 50 or y_hit[train_mask].nunique() < 2:
        return None

    revenue_model = build_revenue_model().fit(X[train_mask], y_revenue[train_mask])
    hit_model = build_hit_model().fit(X[train_mask], y_hit[train_mask])
    return {
        "corte_ano": TEMPORAL_SPLIT_YEAR,
        "n_treino": int(train_mask.sum()),
        "n_teste": int(test_mask.sum()),
        "receita": _regression_metrics(y_revenue[test_mask], revenue_model.predict(X[test_mask])),
        "hit": _classification_metrics(
            y_hit[test_mask],
            hit_model.predict(X[test_mask]),
            hit_model.predict_proba(X[test_mask])[:, 1],
        ),
    }


def save_importance_plot(model, X_eval, y_eval_log, charts_dir):
    charts_dir.mkdir(parents=True, exist_ok=True)
    result = permutation_importance(
        model, X_eval, y_eval_log, n_repeats=10, random_state=RANDOM_STATE, scoring="r2"
    )
    importance = pd.Series(result.importances_mean, index=X_eval.columns).sort_values()

    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(importance.index, importance.values, color="#2196F3")
    ax.set_title("Importância das features (permutação) — modelo de receita pré-estreia")
    ax.set_xlabel("Queda média no R² ao embaralhar a feature")
    plt.tight_layout()
    output_path = charts_dir / "importancia_features.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def save_artifacts(processed_dir, revenue_model, hit_model, metrics, X_eval, y_eval_log):
    models_dir = processed_dir / "modelos"
    models_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(revenue_model, models_dir / "modelo_receita.joblib")
    joblib.dump(hit_model, models_dir / "modelo_hit.joblib")
    (models_dir / "metricas_modelo.json").write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    plot_path = save_importance_plot(revenue_model, X_eval, y_eval_log, processed_dir / "visualizacoes")
    return {
        "modelo_receita": models_dir / "modelo_receita.joblib",
        "modelo_hit": models_dir / "modelo_hit.joblib",
        "metricas": models_dir / "metricas_modelo.json",
        "importancia": plot_path,
    }


def main(data_root=None):
    assert not set(EXCLUDED_LEAKAGE) & set(FEATURE_COLUMNS), "Coluna de leakage entrou nas features."

    gold_df, processed_dir = load_gold(data_root)
    X, y_revenue, y_hit = build_model_table(gold_df)
    print(f"Filmes com orçamento e receita: {len(X)}")
    print(f"Taxa de hits (receita >= {HIT_MULTIPLIER:.0f}x orçamento): {y_hit.mean():.1%}")

    X_train, X_test, yr_train, yr_test, yh_train, yh_test = train_test_split(
        X, y_revenue, y_hit, test_size=0.2, random_state=RANDOM_STATE, stratify=y_hit
    )

    revenue_model = build_revenue_model().fit(X_train, yr_train)
    hit_model = build_hit_model().fit(X_train, yh_train)

    revenue_holdout = _regression_metrics(yr_test, revenue_model.predict(X_test))
    hit_holdout = _classification_metrics(
        yh_test, hit_model.predict(X_test), hit_model.predict_proba(X_test)[:, 1]
    )

    ridge = Pipeline([("prep", build_preprocessor("continuous")), ("model", Ridge())]).fit(X_train, yr_train)
    logreg = Pipeline(
        [("prep", build_preprocessor("binary")), ("model", LogisticRegression(max_iter=1000))]
    ).fit(X_train, yh_train)
    revenue_baseline = _regression_metrics(yr_test, ridge.predict(X_test))
    hit_baseline = _classification_metrics(
        yh_test, logreg.predict(X_test), logreg.predict_proba(X_test)[:, 1]
    )

    revenue_cv = cross_val_score(build_revenue_model(), X, y_revenue, cv=5, scoring="r2")
    hit_cv = cross_val_score(build_hit_model(), X, y_hit, cv=5, scoring="roc_auc")

    metrics = {
        "n_filmes": int(len(X)),
        "taxa_hits": round(float(y_hit.mean()), 4),
        "features": {
            "numericas": NUMERIC_FEATURES,
            "categoricas": ONEHOT_FEATURES,
            "target_encoded": TARGET_FEATURES,
            "excluidas_por_leakage": EXCLUDED_LEAKAGE,
        },
        "aviso_leakage": LEAKAGE_NOTE,
        "modelo_receita": {
            "holdout": revenue_holdout,
            "cv_r2_log": {"media": round(float(revenue_cv.mean()), 4), "desvio": round(float(revenue_cv.std()), 4)},
            "baseline_ridge_holdout": revenue_baseline,
        },
        "modelo_hit": {
            "holdout": hit_holdout,
            "cv_roc_auc": {"media": round(float(hit_cv.mean()), 4), "desvio": round(float(hit_cv.std()), 4)},
            "baseline_logreg_holdout": hit_baseline,
        },
        "split_temporal": _temporal_eval(X, y_revenue, y_hit),
    }

    paths = save_artifacts(processed_dir, revenue_model, hit_model, metrics, X_test, yr_test)

    print("\n=== Modelo de receita (HistGradientBoosting, pré-estreia) ===")
    print(f"  holdout : R2(log)={revenue_holdout['r2_log']}  MAE=US$ {revenue_holdout['mae_usd']:,.0f}")
    print(f"  CV 5x   : R2(log)={metrics['modelo_receita']['cv_r2_log']['media']}")
    print(f"  baseline: R2(log)={revenue_baseline['r2_log']} (Ridge)")
    print("\n=== Modelo de hit/flop (HistGradientBoosting, pré-estreia) ===")
    print(f"  holdout : acc={hit_holdout['accuracy']}  ROC-AUC={hit_holdout['roc_auc']}  F1={hit_holdout['f1']}")
    print(f"  CV 5x   : ROC-AUC={metrics['modelo_hit']['cv_roc_auc']['media']}")
    print(f"  baseline: ROC-AUC={hit_baseline['roc_auc']} (LogReg)")
    if metrics["split_temporal"]:
        st = metrics["split_temporal"]
        print(f"\n=== Split temporal (treino <{TEMPORAL_SPLIT_YEAR}, teste >={TEMPORAL_SPLIT_YEAR}) ===")
        print(f"  receita : R2(log)={st['receita']['r2_log']}  |  hit ROC-AUC={st['hit']['roc_auc']}")
    print("\nArtefatos salvos:")
    for name, path in paths.items():
        print(f"  {name}: {path}")

    return {"revenue_model": revenue_model, "hit_model": hit_model, "metrics": metrics, "paths": paths}


if __name__ == "__main__":
    main()
