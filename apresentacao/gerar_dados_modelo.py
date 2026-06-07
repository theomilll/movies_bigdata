"""
Atualiza os blocos do MODELO em apresentacao/dados_slides.json a partir dos
artefatos oficiais do repositório (nada de número digitado à mão):

  - dados/processed/modelos/metricas_modelo.json   -> métricas (holdout, CV, baseline, split temporal)
  - codigo/modelo_preditivo.py                     -> re-treina em memória para importância + previsões
  - apresentacao/filmes_lancamentos.json           -> lançamentos recentes (metadata pública + bilheteria pesquisada)

O modelo é PRÉ-ESTREIA: usa só o que se sabe antes do lançamento, então prevê
qualquer filme (inclusive os que ainda nem saíram) sem precisar de sinais pós-estreia.

Uso: python apresentacao/gerar_dados_modelo.py   (precisa do gold em dados/processed/)
"""

import json
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.model_selection import train_test_split

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "codigo"))
import modelo_preditivo as M  # noqa: E402

DADOS_JSON = ROOT / "apresentacao" / "dados_slides.json"
METRICAS_JSON = ROOT / "dados" / "processed" / "modelos" / "metricas_modelo.json"
FILMES_JSON = ROOT / "apresentacao" / "filmes_lancamentos.json"


def construir_bloco_modelo(metricas, importancias):
    r = metricas["modelo_receita"]
    h = metricas["modelo_hit"]
    st = metricas["split_temporal"]
    feats = metricas["features"]
    return {
        "metricas": {
            "n_total_treinaveis": metricas["n_filmes"],
            "taxa_hit_base": metricas["taxa_hits"],
            "definicao_hit": "bilheteria >= 2x orçamento",
            "validacao": "holdout 80/20 + 5-fold CV + split temporal (treino <2010 / teste >=2010)",
            "aviso_leakage": metricas["aviso_leakage"],
            "features_usadas": feats["numericas"] + feats["categoricas"] + feats.get("target_encoded", []),
            "features_excluidas_por_vazamento": feats["excluidas_por_leakage"],
            # --- campos lidos diretamente pelo template (headline) = CV (estimativa robusta) ---
            "regressao_log_receita": {
                "r2": r["cv_r2_log"]["media"],
                "r2_std": r["cv_r2_log"]["desvio"],
                "mae_usd_milhoes": round(r["holdout"]["mae_usd"] / 1e6, 1),
            },
            "classificacao_hit": {
                "roc_auc": h["cv_roc_auc"]["media"],
                "roc_auc_std": h["cv_roc_auc"]["desvio"],
                "acuracia": h["holdout"]["accuracy"],
            },
            # --- detalhamento por método de validação (slides de rigor) ---
            "receita": {
                "holdout": {
                    "r2_log": r["holdout"]["r2_log"],
                    "mae_milhoes": round(r["holdout"]["mae_usd"] / 1e6, 1),
                    "rmse_milhoes": round(r["holdout"]["rmse_usd"] / 1e6, 1),
                },
                "cv": {"r2_log": r["cv_r2_log"]["media"], "r2_log_std": r["cv_r2_log"]["desvio"]},
                "baseline_ridge": {"r2_log": r["baseline_ridge_holdout"]["r2_log"]},
                "temporal": {
                    "r2_log": st["receita"]["r2_log"],
                    "mae_milhoes": round(st["receita"]["mae_usd"] / 1e6, 1),
                },
            },
            "hit": {
                "holdout": {
                    "acuracia": h["holdout"]["accuracy"],
                    "precisao": h["holdout"]["precision"],
                    "recall": h["holdout"]["recall"],
                    "f1": h["holdout"]["f1"],
                    "roc_auc": h["holdout"]["roc_auc"],
                },
                "cv": {"roc_auc": h["cv_roc_auc"]["media"], "roc_auc_std": h["cv_roc_auc"]["desvio"]},
                "baseline_logreg": {
                    "acuracia": h["baseline_logreg_holdout"]["accuracy"],
                    "roc_auc": h["baseline_logreg_holdout"]["roc_auc"],
                },
                "temporal": {"acuracia": st["hit"]["accuracy"], "roc_auc": st["hit"]["roc_auc"]},
            },
            "split_temporal": {
                "corte_ano": st["corte_ano"],
                "n_treino": st["n_treino"],
                "n_teste": st["n_teste"],
            },
        },
        "importancias": importancias,
    }


def _feature_row(f):
    kws = f.get("keywords") or []
    # mesma derivação do treino (M._keyword_flags), sem acoplar has_collection
    flag_seq, flag_novel = M._keyword_flags(kws)
    return {
        "log_budget": float(np.log1p(f["budget"])),
        "runtime": float(f.get("runtime") or 100),
        "release_year": float(f["release_year"]),
        "has_collection": int(f.get("has_collection", 0)),
        "keyword_count": float(len(kws)),
        "is_sequel": int(f.get("is_sequel", flag_seq)),
        "based_on_novel": int(f.get("based_on_novel", flag_novel)),
        "primary_genre": f["primary_genre"],
        "original_language": f.get("original_language", "en"),
        "director": (f.get("director") or "desconhecido"),
    }


def prever_lancamentos(rev_model, hit_model, filmes):
    linhas = []
    for f in filmes:
        X = pd.DataFrame([_feature_row(f)])[M.FEATURE_COLUMNS]
        receita = float(np.expm1(rev_model.predict(X)[0]))
        prob = float(hit_model.predict_proba(X)[0, 1])
        actual = f.get("actual_box_office_usd")
        linhas.append({
            "title": f["title"],
            "genero": f["primary_genre"],
            "diretor": (f.get("director") or "desconhecido"),
            "ano": f["release_year"],
            "budget_usd": float(f["budget"]),
            "receita_prevista_usd": receita,
            "roi_previsto_x": receita / f["budget"],
            "prob_hit": prob,
            "veredito": "PROVÁVEL SUCESSO" if prob >= 0.5 else "ALTO RISCO",
            "destaque": bool(f.get("destaque", False)),
            "actual_box_office_usd": float(actual) if actual else None,
            "roi_real_x": (float(actual) / f["budget"]) if actual else None,
            "nota": f.get("nota", ""),
            "fonte": f.get("fonte", ""),
        })
    return linhas


def main():
    gold, _ = M.load_gold()
    metricas = json.loads(METRICAS_JSON.read_text(encoding="utf-8"))
    filmes = json.loads(FILMES_JSON.read_text(encoding="utf-8"))
    dados = json.loads(DADOS_JSON.read_text(encoding="utf-8"))

    X, yr, yh = M.build_model_table(gold)
    # re-treina (determinístico) p/ importância (holdout) e back-test (dados completos)
    Xtr, Xte, ytr, yte, _, _ = train_test_split(X, yr, yh, test_size=0.2, random_state=M.RANDOM_STATE, stratify=yh)
    rev_holdout = M.build_revenue_model().fit(Xtr, ytr)
    imp = permutation_importance(rev_holdout, Xte, yte, n_repeats=10, random_state=M.RANDOM_STATE, scoring="r2")
    importancias = [{"feature": c, "importancia": float(v)}
                    for c, v in sorted(zip(X.columns, imp.importances_mean), key=lambda kv: -kv[1])]

    rev_full = M.build_revenue_model().fit(X, yr)
    hit_full = M.build_hit_model().fit(X, yh)
    predicoes = prever_lancamentos(rev_full, hit_full, filmes)

    com_real = [p for p in predicoes if p["actual_box_office_usd"]]
    acertos = sum(1 for p in com_real
                  if (p["prob_hit"] >= 0.5) == (p["actual_box_office_usd"] >= 2 * p["budget_usd"]))
    sub = [p["actual_box_office_usd"] / p["receita_prevista_usd"]
           for p in com_real if p["actual_box_office_usd"] > p["receita_prevista_usd"]]
    backtest_stats = {
        "n_filmes": len(com_real),
        "acertos_classificacao": acertos,
        "n_sucessos_reais": sum(1 for p in com_real if p["actual_box_office_usd"] >= 2 * p["budget_usd"]),
        "mediana_subestimacao_x": round(float(np.median(sub)), 1) if sub else None,
    }

    dados["modelo"] = construir_bloco_modelo(metricas, importancias)
    dados["predicoes"] = predicoes
    dados["backtest_stats"] = backtest_stats
    DADOS_JSON.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK -> {DADOS_JSON}")
    print(f"  modelo: CV R2(log)={metricas['modelo_receita']['cv_r2_log']['media']} | "
          f"CV AUC={metricas['modelo_hit']['cv_roc_auc']['media']} | holdout acc={metricas['modelo_hit']['holdout']['accuracy']}")
    print(f"  importância top: " + ", ".join(f"{i['feature']} {i['importancia']:.3f}" for i in importancias[:3]))
    print(f"  back-test: {acertos}/{len(com_real)} acertos · subestimação mediana {backtest_stats['mediana_subestimacao_x']}x")
    for p in predicoes:
        a = p["actual_box_office_usd"]
        print(f"    {p['title']:<18} previsto US$ {p['receita_prevista_usd']/1e6:7.1f} mi · prob {p['prob_hit']:.2f}"
              + (f" · real US$ {a/1e6:.1f} mi" if a else ""))


if __name__ == "__main__":
    main()
