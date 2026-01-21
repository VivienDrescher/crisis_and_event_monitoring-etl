import requests
from datetime import date, timedelta

# File naming components
file_prefix = "number_of_political_violence_events_by_country-month-year_as-of-"
file_suffix = ".xlsx"

# How far back to search (1 year)
days_to_check = 365

found = False
today = date.today()

for day_offset in range(days_to_check):
    check_date = today - timedelta(days=day_offset)
    folder = check_date.strftime("%Y-%m")
    filename = f"{file_prefix}{check_date.strftime('%d%b%Y')}{file_suffix}"
    url = f"https://acleddata.com/system/files/{folder}/{filename}"
    
    print(f"Checking: {url}")
    
    # HEAD request with redirects allowed
    r = requests.head(url, allow_redirects=True)
    
    if r.status_code == 200:
        print("\n✅ Found latest file!")
        print("Folder:", folder)
        print("Filename:", filename)
        print("URL:", url)
        found = True
        break

if not found:
    print("\nNo file found in the last year")

