from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.source_file_audit import _read_csv, _redact


class SourceFileAuditTests(unittest.TestCase):
    def test_redact_secret_like_values(self) -> None:
        self.assertEqual(_redact("Authorization: abc"), "***REDACTED***")
        self.assertEqual(_redact("Артикул"), "Артикул")

    def test_read_csv_semicolon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "data.csv"
            path.write_text("Артикул;Остаток\n10031;10\n", encoding="utf-8")
            audit = _read_csv(path, sample_rows=5)
        self.assertEqual(audit["type"], "csv")
        self.assertEqual(audit["delimiter"], ";")
        self.assertEqual(audit["headers"], ["Артикул", "Остаток"])


if __name__ == "__main__":
    unittest.main()
