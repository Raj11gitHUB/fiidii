import os
import requests
import pandas as pd
from datetime import date, timedelta


BASE_URL = "https://www.nseindia.com/api/historical/indices"
HEADERS = {
"User-Agent": "Mozilla/5.0",
"Accept": "application/json",
"Referer": "https://www.nseindia.com"
}


DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)




def fetch_index(index_name, symbol):
end = date.today()
start = end - timedelta(days=5) # small window avoids NSE blocking


params = {
"indexType": index_name,
"from": start.strftime("%d-%m-%Y"),
"to": end.strftime("%d-%m-%Y")
}


with requests.Session() as s:
s.headers.update(HEADERS)
r = s.get(BASE_URL, params=params, timeout=20)
r.raise_for_status()
data = r.json()["data"]


df = pd.DataFrame(data)
df = df[["CH_TIMESTAMP", "OPEN", "HIGH", "LOW", "CLOSE", "TOTTRDQTY"]]
df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
df["Date"] = pd.to_datetime(df["Date"])


file_path = f"{DATA_DIR}/{symbol}.csv"


if os.path.exists(file_path):
old = pd.read_csv(file_path, parse_dates=["Date"])
df = pd.concat([old, df]).drop_duplicates("Date").sort_values("Date")


df.to_csv(file_path, index=False)
print(f"Updated {symbol}")




if __name__ == "__main__":
fetch_index("NIFTY 50", "NIFTY")
fetch_index("NIFTY BANK", "BANKNIFTY")
