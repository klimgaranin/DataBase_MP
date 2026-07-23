from __future__ import annotations

import unittest

from tools.sheets_audit import (
    _column_letter,
    _count_formulas,
    _extract_formula_functions,
    _extract_importrange_sources,
    _extract_sheet_refs,
    _redact_rows,
    _summarize_columns,
)


class SheetsAuditTests(unittest.TestCase):
    def test_count_formulas(self) -> None:
        rows = [["Артикул", "Цена"], ["10031", "=SUM(A:A)"], ["10032", "plain"]]
        self.assertEqual(_count_formulas(rows), 1)

    def test_extract_importrange_sources(self) -> None:
        rows = [["=IMPORTRANGE(\"https://docs.google.com/spreadsheets/d/abc/edit\";\"DATA!A:A\")"]]
        self.assertEqual(_extract_importrange_sources(rows), ["https://docs.google.com/spreadsheets/d/abc/edit"])

    def test_redact_rows_masks_secret_like_cells(self) -> None:
        rows = [["name", "api_key=123"], ["token", "safe"]]
        self.assertEqual(_redact_rows(rows), [["name", "***REDACTED***"], ["***REDACTED***", "safe"]])

    def test_extract_formula_functions(self) -> None:
        formula = '=IFERROR(XLOOKUP($C2;DATA!A:A;DATA!B:B);"")'
        self.assertEqual(_extract_formula_functions(formula), ["IFERROR", "XLOOKUP"])

    def test_extract_sheet_refs(self) -> None:
        formula = '=SUMIFS(DATA!$M:$M;\'Аналитика WB\'!$C:$C;$A2)'
        self.assertEqual(_extract_sheet_refs(formula), ["DATA", "Аналитика WB"])

    def test_column_letter(self) -> None:
        self.assertEqual(_column_letter(1), "A")
        self.assertEqual(_column_letter(26), "Z")
        self.assertEqual(_column_letter(27), "AA")

    def test_summarize_columns(self) -> None:
        rows = [
            ["Артикул", "Продажи"],
            [10031, "=SUMIFS(DATA!A:A;DATA!B:B;A2)"],
            [10032, "=SUMIFS(DATA!A:A;DATA!B:B;A3)"],
        ]
        summary = _summarize_columns(rows)
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["column"], "B")
        self.assertEqual(summary[0]["formula_count"], 2)
        self.assertEqual(summary[0]["top_functions"], ["SUMIFS"])
        self.assertEqual(summary[0]["referenced_sheets"], ["DATA"])


if __name__ == "__main__":
    unittest.main()
