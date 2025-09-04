import requests

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

r = requests.get('https://www.imdb.com/chart/top/', headers=headers)
print(r.status_code, len(r.text))
print('Contains tbody.lister-list:', 'tbody class="lister-list"' in r.text)
print('\n---- snippet start ----\n')
print(r.text[:1200])
print('\n---- snippet end ----\n')
