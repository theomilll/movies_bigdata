from pathlib import Path
import sys

import joblib
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "codigo"))

import modelo_preditivo as modelo


def _gold_like(n=60):
    # Amostra sintetica apenas para smoke test; metricas nao sao significativas neste tamanho.
    rng = np.random.RandomState(0)
    budget = rng.randint(1_000_000, 100_000_000, n).astype(float)
    revenue = budget * rng.uniform(0.5, 4.0, n)
    return pd.DataFrame(
        {
            "budget": budget,
            "revenue": revenue,
            "profit": revenue - budget,
            "roi": (revenue - budget) / budget * 100,
            "runtime": rng.randint(80, 160, n).astype(float),
            "vote_average": rng.uniform(3, 9, n),
            "vote_count": rng.randint(10, 5000, n).astype(float),
            "popularity": rng.uniform(1, 50, n),
            "user_rating_avg": rng.uniform(2, 5, n),
            "user_rating_count": rng.randint(1, 500, n).astype(float),
            "keyword_count": rng.randint(0, 15, n),
            "release_year": rng.randint(1990, 2018, n),
            "has_collection": rng.randint(0, 2, n),
            "primary_genre": rng.choice(["Action", "Comedy", "Drama"], n),
            "original_language": rng.choice(["en", "fr", "es"], n),
        }
    )


def test_leakage_columns_excluded_from_features():
    assert not (set(modelo.EXCLUDED_LEAKAGE) & set(modelo.FEATURE_COLUMNS))


def test_build_table_train_and_save(tmp_path):
    gold = _gold_like()
    X, y_revenue, y_hit = modelo.build_model_table(gold)

    assert len(X) == len(gold)
    assert set(X.columns) == set(modelo.FEATURE_COLUMNS)
    assert not (set(modelo.EXCLUDED_LEAKAGE) & set(X.columns))
    assert y_hit.nunique() == 2

    revenue_model = modelo.build_revenue_model().fit(X, y_revenue)
    hit_model = modelo.build_hit_model().fit(X, y_hit)

    assert len(revenue_model.predict(X.head(5))) == 5
    assert hit_model.predict_proba(X.head(5)).shape == (5, 2)

    paths = modelo.save_artifacts(tmp_path, revenue_model, hit_model, {"teste": True}, X, y_revenue)
    for path in paths.values():
        assert Path(path).is_file()

    reloaded = joblib.load(paths["modelo_receita"])
    assert len(reloaded.predict(X.head(3))) == 3
