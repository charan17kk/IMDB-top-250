import json
import csv
import html
import argparse


def convert(json_path='imdb_top_250.json', csv_path='imdb_top_250.csv'):
    """Convert newline-delimited JSON to CSV, unescaping HTML entities in titles."""
    count = 0
    with open(json_path, 'r', encoding='utf-8') as jf, open(csv_path, 'w', newline='', encoding='utf-8') as cf:
        writer = csv.writer(cf)
        writer.writerow(['rank', 'title', 'year', 'rating', 'movie_url'])
        for line in jf:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            rank = obj.get('rank', '')
            # unescape HTML entities in title
            title = html.unescape(obj.get('title', '') or '')
            year = obj.get('year', '')
            rating = obj.get('rating', '')
            movie_url = obj.get('movie_url', '')
            writer.writerow([rank, title, year, rating, movie_url])
            count += 1
    print(f'Wrote {count} rows to {csv_path}')


def _main():
    p = argparse.ArgumentParser()
    p.add_argument('--json', default='imdb_top_250.json')
    p.add_argument('--csv', default='imdb_top_250.csv')
    args = p.parse_args()
    convert(args.json, args.csv)


if __name__ == '__main__':
    _main()
