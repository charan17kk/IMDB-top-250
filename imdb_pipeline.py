import requests
from bs4 import BeautifulSoup
import pandas as pd
import uuid
import json
import os
from google.cloud import bigquery
import concurrent.futures
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def scrape_imdb_top_250():
    """Scrape IMDB Top 250 movies and return data as a list of dictionaries."""
    url = "https://www.imdb.com/chart/top/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching IMDB page: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    movies = []

    # First try: parse JSON-LD ItemList (reliable, contains all 250 entries)
    try:
        ld_json_tags = soup.find_all('script', type='application/ld+json')
        for tag in ld_json_tags:
            try:
                payload = json.loads(tag.string)
            except Exception:
                # sometimes the script tag contains multiple JSON objects or whitespace
                try:
                    payload = json.loads(tag.get_text())
                except Exception:
                    continue

            # Look for ItemList with itemListElement
            if isinstance(payload, dict) and payload.get('@type') == 'ItemList' and payload.get('itemListElement'):
                elements = payload.get('itemListElement')
                for elem in elements:
                    if len(movies) >= 250:
                        break
                    try:
                        # ListItem schema: {'@type':'ListItem','position':1,'item':{...}}
                        position = elem.get('position') or elem.get('@type') or None
                        item = elem.get('item') or elem
                        title = item.get('name') or item.get('alternateName') or item.get('url') or 'N/A'
                        # extract year from name if present like "The Shawshank Redemption (1994)"
                        year = 0
                        if isinstance(title, str):
                            # try to strip trailing year in parentheses
                            import re
                            m = re.search(r"\((\d{4})\)", title)
                            if m:
                                try:
                                    year = int(m.group(1))
                                except Exception:
                                    year = 0
                        rating = 0.0
                        movie_url = item.get('url') or ''

                        movies.append({
                            'id': str(uuid.uuid4()),
                            'rank': int(elem.get('position')) if elem.get('position') else (len(movies) + 1),
                            'title': title if isinstance(title, str) else str(title),
                            'year': year,
                            'rating': rating,
                            'movie_url': movie_url
                        })
                    except Exception as e:
                        # skip bad element
                        continue
                if movies:
                    return movies

    except Exception:
        # fall through to HTML parsing fallback
        pass

    # Fallback: classic table/list parsing (non-JS fallback)
    rows = soup.select("tbody.lister-list tr")
    if not rows:
        rows = soup.select("table.chart.full-width tbody tr")
    if not rows:
        rows = soup.select("div.lister-list .lister-item")

    for rank, row in enumerate(rows, 1):
        if len(movies) >= 250:
            break
        try:
            title_a = (
                row.select_one("td.titleColumn a")
                or row.select_one("a")
                or row.select_one("h3.lister-item-header a")
            )
            title = title_a.text.strip() if title_a else "N/A"

            year_span = (
                row.select_one("span.secondaryInfo")
                or row.select_one("span.lister-item-year")
                or row.select_one("span.year")
            )
            year_text = year_span.text if year_span else ""
            year_digits = ''.join(ch for ch in year_text if ch.isdigit())
            year = int(year_digits) if year_digits else 0

            rating_tag = (
                row.select_one("td.ratingColumn.imdbRating strong")
                or row.select_one("div.ratings-imdb-rating strong")
                or row.select_one("span.rating")
            )
            rating = float(rating_tag.text.strip()) if rating_tag and rating_tag.text.strip() else 0.0

            href = title_a['href'] if title_a and title_a.has_attr('href') else ''
            movie_url = f"https://www.imdb.com{href.split('?')[0]}" if href else ''

            movies.append({
                "id": str(uuid.uuid4()),
                "rank": rank,
                "title": title,
                "year": year,
                "rating": rating,
                "movie_url": movie_url
            })
        except Exception:
            continue

    return movies

def save_to_json(data):
    """Save scraped data to JSON."""
    try:
        df = pd.DataFrame(data)
        df.to_json("imdb_top_250.json", orient="records", lines=True)
        print(f"Saved {len(data)} movies to imdb_top_250.json")
    except Exception as e:
        print(f"Error saving to JSON: {e}")


def _requests_session_with_retries(total_retries=3, backoff_factor=0.3):
    session = requests.Session()
    retries = Retry(total=total_retries, backoff_factor=backoff_factor, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    return session


def fetch_movie_details(movie, session, timeout=10):
    """Fetch a movie page and extract year and rating. Returns dict with updates."""
    url = movie.get('movie_url')
    if not url:
        return {'year': 0, 'rating': 0.0}
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code != 200:
            return {'year': 0, 'rating': 0.0}
        soup = BeautifulSoup(r.text, 'html.parser')

        # Try JSON-LD on movie page
        year = 0
        rating = 0.0
        try:
            ld = soup.find('script', type='application/ld+json')
            if ld and ld.string:
                data = json.loads(ld.string)
                # datePublished could be '1994-09-23' or similar
                dp = data.get('datePublished')
                if dp and isinstance(dp, str):
                    year_digits = ''.join(ch for ch in dp if ch.isdigit())
                    if len(year_digits) >= 4:
                        year = int(year_digits[:4])
                # aggregateRating may be nested
                agg = data.get('aggregateRating') or {}
                rv = agg.get('ratingValue')
                if rv:
                    try:
                        rating = float(rv)
                    except Exception:
                        rating = 0.0
        except Exception:
            pass

        # Fallbacks: meta tags or visible elements
        if year == 0:
            # look for year in title or meta
            try:
                # common meta: <meta property="og:title" content="Title (1994) - IMDb"/>
                meta_title = soup.find('meta', property='og:title')
                if meta_title and meta_title.get('content'):
                    import re
                    m = re.search(r"\((\d{4})\)", meta_title['content'])
                    if m:
                        year = int(m.group(1))
            except Exception:
                pass

        if rating == 0.0:
            try:
                # try meta rating
                meta_rating = soup.find('meta', itemprop='ratingValue') or soup.find('meta', property='og:rating')
                if meta_rating and meta_rating.get('content'):
                    rating = float(meta_rating['content'])
            except Exception:
                pass

        return {'year': year or 0, 'rating': rating or 0.0}
    except Exception:
        return {'year': 0, 'rating': 0.0}


def enrich_movies_with_details(movies, max_workers=10):
    """Enrich movies list in-place with year and rating using concurrent fetches."""
    session = _requests_session_with_retries()
    start = time.time()
    updated = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_movie_details, m, session): m for m in movies}
        for fut in concurrent.futures.as_completed(futures):
            m = futures[fut]
            try:
                res = fut.result()
                m['year'] = res.get('year', 0)
                m['rating'] = res.get('rating', 0.0)
                updated += 1
            except Exception:
                m['year'] = 0
                m['rating'] = 0.0
    elapsed = time.time() - start
    print(f"Fetched details for {updated} movies in {elapsed:.1f}s")
    return movies

def create_bigquery_table(client):
    """Create BigQuery dataset and table if they don't exist."""
    try:
        dataset_id = "imdb_dataset"
        dataset_ref = client.dataset(dataset_id)
        client.create_dataset(dataset_ref, exists_ok=True)
        print(f"Dataset {dataset_id} created or exists")

        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rank", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("rating", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("movie_url", "STRING", mode="REQUIRED")
        ]

        table_id = "top_250_movies"
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"Table {table_id} created or exists")
    except Exception as e:
        print(f"Error creating BigQuery table: {e}")

def load_to_bigquery(data):
    """Load data into BigQuery."""
    try:
        client = bigquery.Client(project="perfect-eon-470909-a6")
        create_bigquery_table(client)
        table_ref = client.dataset("imdb_dataset").table("top_250_movies")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        df = pd.DataFrame(data)
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Loaded {len(data)} rows into imdb_dataset.top_250_movies")
    except Exception as e:
        print(f"Error loading to BigQuery: {e}")

def main():
    print("Scraping IMDB Top 250...")
    movies = scrape_imdb_top_250()
    if movies:
        # Enrich with year and rating before saving and loading
        print("Fetching per-movie details (year, rating)...")
        enrich_movies_with_details(movies, max_workers=10)
        save_to_json(movies)
        if os.environ.get('IMDB_UPLOAD', '') == '1':
            print("Loading to BigQuery...")
            load_to_bigquery(movies)
        else:
            print("Skipping BigQuery upload (set IMDB_UPLOAD=1 to enable)")
    else:
        print("No data scraped.")

if __name__ == "__main__":
    main()