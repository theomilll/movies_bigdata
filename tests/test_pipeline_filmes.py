from pathlib import Path
import sys

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "codigo"))

import pipeline_filmes as pipeline


def _movie_rows():
    return pd.DataFrame(
        [
            {
                "id": "1",
                "title": "Alpha",
                "adult": "False",
                "budget": "10000000",
                "revenue": "90000000",
                "runtime": "120",
                "genres": "[{'id': 28, 'name': 'Action'}]",
                "belongs_to_collection": "",
                "vote_average": "7.4",
                "vote_count": "800",
                "popularity": "33.1",
                "original_language": "en",
                "release_date": "1995-06-01",
                "overview": "A test film.",
                "status": "Released",
            },
            {
                "id": "2",
                "title": "Beta",
                "adult": "False",
                "budget": "20000000",
                "revenue": "150000000",
                "runtime": "105",
                "genres": "[{'id': 35, 'name': 'Comedy'}]",
                "belongs_to_collection": "{'id': 10, 'name': 'Series'}",
                "vote_average": "6.8",
                "vote_count": "450",
                "popularity": "27.4",
                "original_language": "en",
                "release_date": "2005-09-10",
                "overview": "Another test film.",
                "status": "Released",
            },
            {
                "id": "3",
                "title": "Gamma",
                "adult": "False",
                "budget": "5000000",
                "revenue": "45000000",
                "runtime": "98",
                "genres": "[{'id': 18, 'name': 'Drama'}]",
                "belongs_to_collection": "",
                "vote_average": "8.1",
                "vote_count": "1200",
                "popularity": "41.0",
                "original_language": "pt",
                "release_date": "2015-02-20",
                "overview": "A third test film.",
                "status": "Released",
            },
        ]
    )


def test_pipeline_smoke_builds_outputs(tmp_path):
    movies = pipeline.clean_movies(_movie_rows())
    directors = pipeline.build_directors(
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "crew": [
                    "[{'job': 'Director', 'name': 'A Director'}]",
                    "[{'job': 'Director', 'name': 'B Director'}]",
                    "[{'job': 'Director', 'name': 'A Director'}]",
                ],
            }
        )
    )
    keywords = pipeline.build_keywords(
        pd.DataFrame(
            {
                "id": [1, 2, 3],
                "keywords": [
                    "[{'id': 1, 'name': 'space'}]",
                    "[{'id': 2, 'name': 'family'}]",
                    "[{'id': 3, 'name': 'music'}]",
                ],
            }
        )
    )
    ratings = pipeline.build_movie_ratings(
        pd.DataFrame({"movieId": [10, 20, 30], "tmdbId": [1, 2, 3]}),
        pd.DataFrame(
            {
                "userId": [1, 2, 3, 4],
                "movieId": [10, 10, 20, 30],
                "rating": [4.0, 5.0, 3.5, 4.5],
                "timestamp": [1, 2, 3, 4],
            }
        ),
    )

    gold = pipeline.build_gold_dataset(movies, directors, keywords, ratings)
    tables = pipeline.build_aggregations(gold)
    output_paths = pipeline.save_outputs(tmp_path, gold, tables)
    plot_paths = pipeline.plot_results(tmp_path, gold, tables)

    assert len(gold) == 3
    assert gold.loc[gold["title"] == "Alpha", "roi"].iloc[0] == 800.0
    assert len(tables) == 11
    assert (tmp_path / "filmes_processados.csv").is_file()
    assert (output_paths["filmes_processados_parquet"] / "part-00000.parquet").is_file()
    assert len(plot_paths) == 4
    for path in plot_paths.values():
        assert path.is_file()


def test_missing_required_raw_file_message_names_credits(tmp_path):
    raw_dir = tmp_path / "dados" / "raw"
    raw_dir.mkdir(parents=True)

    with pytest.raises(FileNotFoundError) as exc_info:
        pipeline.load_raw_datasets(tmp_path)

    message = str(exc_info.value)
    assert "credits.csv" in message
    assert "The Movies Dataset" in message
