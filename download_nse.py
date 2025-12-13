import os
from datetime import date, timedelta
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

class Download:
    def __init__(self):
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "*/*",
            "Referer": "https://www.nseindia.com",
        }
        self.session = requests.Session()
        self.session.headers.update(self.header)

    def bhav_copy(self, start_date, end_date, download_path):
        os.makedirs(download_path, exist_ok=True)
        for i in daterange(start_date, end_date):
            url_prefix = "https://archives.nseindia.com/content/historical/DERIVATIVES/"
            c1 = i.strftime("%Y/%b").upper()
            c2 = i.strftime("%d%b%Y").upper()
            file_name = f"fo{c2}bhav.csv.zip"
            url = f"{url_prefix}{c1}/{file_name}"
            print("Downloading:", url)
            resp = self.session.get(url, allow_redirects=True)
            if resp.status_code == 200:
                zip_file = ZipFile(BytesIO(resp.content))
                zip_file.extractall(download_path)
                print("Saved:", file_name)
            else:
                print("Failed:", resp.status_code)

    def nse_oi(self, start_date, end_date, download_path):
        os.makedirs(download_path, exist_ok=True)
        for i in daterange(start_date, end_date):
            url_prefix = "https://archives.nseindia.com/archives/nsccl/mwpl/"
            c = i.strftime("%d%m%Y")
            file_name = f"nseoi_{c}.zip"
            url = url_prefix + file_name
            print("Downloading:", url)
            resp = self.session.get(url)
            if resp.status_code == 200:
                zip_file = ZipFile(BytesIO(resp.content))
                zip_file.extractall(download_path)
                print("Saved:", file_name)
            else:
                print("Failed:", resp.status_code)

    def client_oi(self, start_date, end_date, download_path, copy_path):
        os.makedirs(download_path, exist_ok=True)
        os.makedirs(copy_path, exist_ok=True)
        files = ["fao_participant_oi_", "fao_participant_vol_"]
        categories = ["Client", "FII", "DII", "Pro"]
        for url_suffix in files:
            for i in daterange(start_date, end_date):
                c = i.strftime("%d%m%Y")
                file_name = f"{url_suffix}{c}.csv"
                url = f"https://archives.nseindia.com/content/nsccl/{file_name}"
                print("Downloading:", url)
                resp = self.session.get(url)
                if resp.status_code != 200:
                    print("Failed:", resp.status_code)
                    continue
                file_path = os.path.join(download_path, file_name)
                with open(file_path, "wb") as f:
                    f.write(resp.content)
                try:
                    df = pd.read_csv(file_path, header=None)
                    df["Date"] = i.strftime("%d-%m-%Y")
                except:
                    print("Invalid CSV:", file_name)
                    continue
                for cat in categories:
                    part = df[df[0] == cat]
                    if not part.empty:
                        out_file = f"{url_suffix}{cat}.csv"
                        out_path = os.path.join(copy_path, out_file)
                        part.to_csv(out_path, mode="a", index=False, header=False)

    def stats(self, start_date, end_date, download_path, copy_path):
        os.makedirs(download_path, exist_ok=True)
        os.makedirs(copy_path, exist_ok=True)
        categories = [
            "INDEX FUTURES", "NIFTY FUTURES", "BANKNIFTY FUTURES",
            "INDEX OPTIONS", "NIFTY OPTIONS", "BANKNIFTY OPTIONS",
            "STOCK FUTURES"
        ]
        for i in daterange(start_date, end_date):
            c = i.strftime("%d-%b-%Y")
            file_name = f"fii_stats_{c}.xls"
            url = f"https://archives.nseindia.com/content/fo/{file_name}"
            print("Downloading:", url)
            resp = self.session.get(url)
            if resp.status_code != 200:
                print("Failed:", resp.status_code)
                continue
            file_path = os.path.join(download_path, file_name)
            with open(file_path, "wb") as f:
                f.write(resp.content)
            try:
                df = pd.read_excel(file_path, header=None)
                df["Date"] = i.strftime("%d-%m-%Y")
            except:
                print("Invalid XLS:", file_name)
                continue
            for cat in categories:
                part = df[df[0] == cat]
                if not part.empty:
                    out_file = f"{cat}.csv"
                    out_path = os.path.join(copy_path, out_file)
                    part.to_csv(out_path, mode="a", index=False, header=False)

# Run script
if __name__ == "__main__":
    d = Download()
    dow_path = "./fiidata"
    start_date = date(2025, 12, 1)
    end_date = date(2025, 12,5)
    d.bhav_copy(start_date, end_date, dow_path)
    d.nse_oi(start_date, end_date, dow_path)
    d.client_oi(start_date, end_date, dow_path, dow_path)
    d.stats(start_date, end_date, dow_path, dow_path)
