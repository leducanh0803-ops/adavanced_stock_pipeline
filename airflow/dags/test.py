import requests
import pandas as pd
import io

HF_API_KEY = "hfd_7c174cfb50eb466c9237d05700220412"
HEADERS = {"X-API-Key": HF_API_KEY}
BASE = "https://api.hfdatalibrary.com/v1"
DATE = "2024-01-02"

#Use the working endpoint — just /bars/AAPL with 1min data
r = requests.get(
    f"{BASE}/variables/AAPL",
    headers=HEADERS,
    params={"start": DATE, "end": DATE, "format": "parquet"},
    timeout=30,
)
print("Status:", r.status_code)

if r.status_code == 200:
    df = pd.read_parquet(io.BytesIO(r.content))
    print("Shape:", df.shape)
    print("Columns:", df.columns.tolist())
    print(df.head(3))
else:
    print(r.text[:300])