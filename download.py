import os

dow_path = "./fiidii"
os.makedirs(dow_path, exist_ok=True)

print("Download script running... (Add your download code here)")
import os
from datetime import date, timedelta
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


class download:
    def __init__(self):
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "*/*",
            "Referer": "https://www.nseindia.com",
        }

        self.session = requests.Session()
        self.session.headers.update(self.header)

    def bhav_copy(self, start_date, end_date, dow_path_bhav):
        os.makedirs(dow_path_bhav, exist_ok=True)
        os.chdir(dow_path_bhav)

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
                zip_file.extractall("")
                print("Saved:", file_name)
            else:
                print("Failed:", resp.status_code)

    def nse_oi(self, start_date, end_date, dow_path_nse_oi):
        os.makedirs(dow_path_nse_oi, exist_ok=True)
        os.chdir(dow_path_nse_oi)

        for i in daterange(start_date, end_date):
            url_prefix = "https://archives.nseindia.com/archives/nsccl/mwpl/"
            c = i.strftime("%d%m%Y")

            file_name = f"nseoi_{c}.zip"
            url = url_prefix + file_name

            print("Downloading:", url)
            resp = self.session.get(url)

            if resp.status_code == 200:
                zip_file = ZipFile(BytesIO(resp.content))
                zip_file.extractall("")
                print("Saved:", file_name)
            else:
                print("Failed:", resp.status_code)

    def client_oi(self, start_date, end_date, dow_path_client, copy_path_client):
        os.makedirs(copy_path_client, exist_ok=True)
        os.makedirs(dow_path_client, exist_ok=True)
        os.chdir(dow_path_client)

        url_prefix = "https://archives.nseindia.com/content/nsccl/"
        files = ["fao_participant_oi_", "fao_participant_vol_"]

        for url_suffix in files:
            for i in daterange(start_date, end_date):

                c = i.strftime("%d%m%Y")
                file_name = f"{url_suffix}{c}.csv"
                url = url_prefix + file_name

                print("Downloading:", url)
                resp = self.session.get(url)

                if resp.status_code != 200:
                    print("Failed:", resp.status_code)
                    continue

                with open(file_name, "wb") as f:
                    f.write(resp.content)

                try:
                    df = pd.read_csv(file_name, header=None)
                    df["Date"] = i.strftime("%d-%m-%Y")
                except:
                    print("Invalid CSV:", file_name)
                    continue

                categories = ["Client", "FII", "DII", "Pro"]

                for cat in categories:
                    part = df[df[0] == cat]
                    if not part.empty:
                        out_path = f"{copy_path_client}/{url_suffix}{cat}.csv"
                        part.to_csv(out_path, mode="a", index=False, header=False)

    def stats(self, start_date, end_date, dow_path_stats, copy_path_stats):
        os.makedirs(copy_path_stats, exist_ok=True)
        os.makedirs(dow_path_stats, exist_ok=True)
        os.chdir(dow_path_stats)

        url_prefix = "https://archives.nseindia.com/content/fo/"

        for i in daterange(start_date, end_date):
            c = i.strftime("%d-%b-%Y")
            file_name = f"fii_stats_{c}.xls"
            url = url_prefix + file_name

            print("Downloading:", url)
            resp = self.session.get(url)

            if resp.status_code != 200:
                print("Failed:", resp.status_code)
                continue

            with open(file_name, "wb") as f:
                f.write(resp.content)

            try:
                df = pd.read_excel(file_name, header=None)
            except:
                print("Invalid XLS:", file_name)
                continue

            df["Date"] = i.strftime("%d-%m-%Y")

            categories = [
                "INDEX FUTURES", "NIFTY FUTURES", "BANKNIFTY FUTURES",
                "INDEX OPTIONS", "NIFTY OPTIONS", "BANKNIFTY OPTIONS",
                "STOCK FUTURES"
            ]

            for cat in categories:
                part = df[df[0] == cat]
                if not part.empty:
                    out_path = f"{copy_path_stats}/{cat}.csv"
                    part.to_csv(out_path, mode="a", index=False, header=False)


d = download()

dow_path = "./fiidata"     # IMPORTANT CHANGE
start_date = date(2025, 12, 9)
end_date = date(2025, 12, 10)

d.client_oi(start_date, end_date, dow_path, dow_path)
