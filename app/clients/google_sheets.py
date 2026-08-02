from __future__ import annotations

from pathlib import Path
from typing import Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


def quote_sheet_name(sheet_name: str) -> str:
    escaped = sheet_name.replace("'", "''")
    return f"'{escaped}'"


class GoogleSheetsClient:
    def __init__(self, *, credentials_path: str | Path) -> None:
        self.credentials_path = Path(credentials_path)
        if not self.credentials_path.exists():
            raise RuntimeError(f"Файл Google service account не найден: {self.credentials_path}")

        credentials = Credentials.from_service_account_file(
            str(self.credentials_path),
            scopes=[SHEETS_SCOPE],
        )
        self._service = build("sheets", "v4", credentials=credentials, cache_discovery=False)

    def clear_values(self, *, spreadsheet_id: str, sheet_name: str, a1_range: str) -> dict[str, Any]:
        full_range = f"{quote_sheet_name(sheet_name)}!{a1_range}"
        return (
            self._service.spreadsheets()
            .values()
            .clear(spreadsheetId=spreadsheet_id, range=full_range, body={})
            .execute()
        )

    def get_values(self, *, spreadsheet_id: str, sheet_name: str, a1_range: str) -> list[list[Any]]:
        full_range = f"{quote_sheet_name(sheet_name)}!{a1_range}"
        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=full_range)
            .execute()
        )
        return result.get("values", [])

    def update_values(
        self,
        *,
        spreadsheet_id: str,
        sheet_name: str,
        start_cell: str,
        values: list[list[Any]],
    ) -> dict[str, Any]:
        full_range = f"{quote_sheet_name(sheet_name)}!{start_cell}"
        return (
            self._service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=full_range,
                valueInputOption="USER_ENTERED",
                body={"values": values},
            )
            .execute()
        )

    def batch_update_values(
        self,
        *,
        spreadsheet_id: str,
        sheet_name: str,
        updates: list[tuple[str, list[list[Any]]]],
    ) -> dict[str, Any]:
        data = [
            {
                "range": f"{quote_sheet_name(sheet_name)}!{a1_range}",
                "values": values,
            }
            for a1_range, values in updates
        ]
        return (
            self._service.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"valueInputOption": "USER_ENTERED", "data": data},
            )
            .execute()
        )
