from __future__ import annotations

import base64
import io
import zipfile
import unittest

from tools.xlsm_powerquery_audit import (
    extract_data_mashup_parts,
    sanitize_text,
    split_powerquery_section,
)


def _build_data_mashup_xml(parts: dict[str, str]) -> bytes:
    package = b"\x00\x00\x00\x00\x00\x00\x00\x00"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in parts.items():
            archive.writestr(name, content)
    package += buffer.getvalue()
    payload = base64.b64encode(package).decode("ascii")
    return f'<?xml version="1.0" encoding="utf-16"?><DataMashup>{payload}</DataMashup>'.encode("utf-16")


class XlsmPowerQueryAuditTests(unittest.TestCase):
    def test_sanitize_authorization_and_jwt(self) -> None:
        text = 'Headers=[Authorization="eyJabc.def.ghi12345678901234567890"], token="secret-value"'
        sanitized = sanitize_text(text)
        self.assertIn("Authorization=\"***REDACTED***\"", sanitized)
        self.assertIn('token="***REDACTED***"', sanitized)
        self.assertNotIn("secret-value", sanitized)

    def test_extract_data_mashup_parts(self) -> None:
        raw_xml = _build_data_mashup_xml({"Formulas/Section1.m": "section Section1;"})
        parts = extract_data_mashup_parts(raw_xml)
        self.assertEqual(parts["Formulas/Section1.m"], "section Section1;")

    def test_split_powerquery_section(self) -> None:
        section = """section Section1;

shared #"Остатки WB" = let
    Источник = Json.Document(Web.Contents("https://example.test", [Headers=[Authorization="eyJabc.def.ghi12345678901234567890"]])),
    #"Переименованные столбцы" = Table.RenameColumns(Источник,{{"supplierArticle", "Артикул"}})
in
    #"Переименованные столбцы";

shared #"Заказы WB" = let
    WB1 = Excel.CurrentWorkbook(){[Name="Заказы_WB1"]}[Content]
in
    WB1;
"""
        queries = split_powerquery_section(section)
        self.assertEqual([query["name"] for query in queries], ["Остатки WB", "Заказы WB"])
        self.assertEqual(queries[0]["sources"], ["https://example.test"])
        self.assertNotIn("eyJabc", queries[0]["body"])


if __name__ == "__main__":
    unittest.main()
