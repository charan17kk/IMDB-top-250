from google.cloud import bigquery
import os

PROJECT = "perfect-eon-470909-a6"
DATASET = "imdb_dataset"
TABLE = "top_250_movies"

client = bigquery.Client(project=PROJECT)
full_table = f"`{PROJECT}.{DATASET}.{TABLE}`"

print(f"Creating sorted replacement of {full_table}...")

sql_replace = f"""
CREATE OR REPLACE TABLE {full_table}
AS
SELECT *
FROM {full_table}
ORDER BY rank ASC
"""

job = client.query(sql_replace)
job.result()
print("Table replaced with ordered copy (logical ordering by rank applied in the statement).")

# Verify first 10 rows ordered by rank
sql_check = f"SELECT rank, title, year, rating, movie_url FROM {full_table} ORDER BY rank ASC LIMIT 10"
rows = client.query(sql_check).result()
print("First 10 rows after reorder:")
for r in rows:
    print(r.rank, r.title)

print("Done.")
