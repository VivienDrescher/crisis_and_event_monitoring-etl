import requests

url = "http://data.gdeltproject.org/events/"
output_file = "20260119.export.CSV.zip"

response = requests.get(url)
if response.status_code == 200:
    with open(output_file, "wb") as f:
        f.write(response.content)
    print(f"File downloaded as {output_file}")
else:
    print("Failed to download file:", response.status_code)