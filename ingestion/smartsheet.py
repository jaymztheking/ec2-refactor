import requests
import pandas as pd

class Smartsheet:
    def __init__(self, sheetid: str, api_token: str) -> None:
        self.sheetid = sheetid
        self.api_token = api_token

    def download_csv(self, file_name) -> bool:
        url = f"https://api.smartsheet.com/2.0/sheets/{self.sheetid}"
        headers = {"Authorization": "Bearer "+self.api_token, "Accept": "text/csv"}
        csv = str(requests.get(url, headers=headers).content, 'utf-8')
        with open(file_name, 'w') as csvfile:
            csvfile.write(csv)
        return True


x = Smartsheet('8065621301716868', '4pPCMRoY46kbRwVRsyWADkrQCo0Wfje1qM0SA')
a = x.download_csv()
