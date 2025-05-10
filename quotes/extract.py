import requests
import pandas as pd
import csv

url = "https://quotes-api12.p.rapidapi.com/quotes/author/Unknown"

headers = {
    "x-rapidapi-key": "e7cbee4ae1mshd0704b6dcc65548p1632e3jsncb04d537a049",
    "x-rapidapi-host": "quotes-api12.p.rapidapi.com"
}

all_quotes = []
page = 1
limit = 10

while True:
    querystring = {"page": str(page), "limit": str(limit)}
    response = requests.get(url, headers=headers, params=querystring)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        break
    
    data = response.json()
    
    # Adjust according to actual response structure
    quotes = data.get('quotes') or data.get('data')
    if not quotes:
        break
    
    all_quotes.extend(quotes)
    
    print(f"Fetched page {page} with {len(quotes)} quotes")
    page += 1

print(f"\nTotal quotes fetched: {len(all_quotes)}")

# ---- Saving to CSV ----
# Adjust fieldnames according to the actual fields in each quote
csv_filename = "quotes.csv"

if all_quotes:
    # Check what fields are inside the first quote
    example_quote = all_quotes[0]
    fieldnames = example_quote.keys()

    with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_quotes)

    print(f"Saved all quotes to '{csv_filename}'")
else:
    print("No quotes to save.")
