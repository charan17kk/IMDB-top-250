"""Run the full pipeline end-to-end:
1) Scrape Top 250
2) Enrich with per-movie details
3) Save JSON
4) Convert to CSV
5) Optionally upload to BigQuery when IMDB_UPLOAD=1

Usage: python run_all.py
"""
import os
from imdb_pipeline import scrape_imdb_top_250, enrich_movies_with_details, save_to_json, load_to_bigquery
from convert_to_csv import convert


def run_all(do_upload=False):
    print('Starting full pipeline...')
    movies = scrape_imdb_top_250()
    if not movies:
        print('No movies scraped. Exiting.')
        return
    print('Enriching movie details...')
    enrich_movies_with_details(movies, max_workers=10)
    save_to_json(movies)
    convert()
    if do_upload:
        print('Uploading to BigQuery...')
        load_to_bigquery(movies)
    else:
        print('Upload skipped. Set IMDB_UPLOAD=1 to enable upload.')


if __name__ == '__main__':
    do_upload = os.environ.get('IMDB_UPLOAD', '') == '1'
    run_all(do_upload)
