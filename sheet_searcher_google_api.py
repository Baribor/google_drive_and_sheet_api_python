from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
from googleapiclient.errors import HttpError
import asyncio

SERVICE_ACCOUNT_KEY = "./service_account.json"  # path to service account key
SPREAD_SHEET_ID = "1PJZaRKUjrivB7B68Mkr1i3qxYyoeKoB7H56L_D8RvBc"  # Id of spread sheet


class Google_Service_Handler:
    def __init__(self, service_account_key, sheet_id) -> None:
        self.service_account_key = service_account_key
        self.sheet_id = sheet_id
        self.init_services()

    def init_services(self):
        credentials = service_account.Credentials.from_service_account_file(
            filename=self.service_account_key)
        self.sheet_service = build("sheets", "v4", credentials=credentials)
        self.drive_service = build("drive", "v3", credentials=credentials)

    async def getFile(self, drive_link):
        if "/file/d/" in drive_link:
            file_id = drive_link.split("/file/d/")[-1].split("/")[0]
        elif "open?id=" in drive_link:
            file_id = drive_link.split("id=")[-1].split("&")[0]
        else:
            print("INVALID LINK TYPE:")
            print(drive_link)
            return None

        try:
            print(f"ATTEMPTING: {file_id}")
            request_file = self.drive_service.files().get_media(fileId=file_id)
            bytes = io.BytesIO()
            downloader = MediaIoBaseDownload(bytes, request_file)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(F'Download progress {int(status.progress() * 100)}.')
            print(bytes)
        except HttpError as error:
            print(F'An error occurred: {error}')

    async def run(self):
        res = self.sheet_service.spreadsheets().get(
            spreadsheetId=self.sheet_id).execute()
        sheetName = res.get("sheets", [])[0].get("properties").get("title")
        content = self.sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREAD_SHEET_ID, range=sheetName).execute()
        values = content.get("values")
        for row in values[1:]:
            url = row[1]
            await self.getFile(url)


async def main():
    handler = Google_Service_Handler(SERVICE_ACCOUNT_KEY, SPREAD_SHEET_ID)
    await handler.run()

if __name__ == '__main__':
    asyncio.run(main())
