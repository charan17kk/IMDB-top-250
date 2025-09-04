import json
import csv

json_path = 'imdb_top_250.json'
csv_path = 'imdb_top_250.csv'
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
        title = obj.get('title', '')
        year = obj.get('year', '')
        rating = obj.get('rating', '')
        movie_url = obj.get('movie_url', '')
        writer.writerow([rank, title, year, rating, movie_url])
        count += 1
print(f'Wrote {count} rows to {csv_path}')
