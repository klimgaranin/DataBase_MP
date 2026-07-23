from __future__ import annotations

import unittest

from tools.project_standards_check import REQUIRED_FILES, ROOT, main


class ProjectStandardsCheckTests(unittest.TestCase):
    def test_required_standard_files_exist(self) -> None:
        missing = [rel_path for rel_path in REQUIRED_FILES if not (ROOT / rel_path).exists()]
        self.assertEqual(missing, [])

    def test_main_passes(self) -> None:
        self.assertEqual(main(), 0)


if __name__ == "__main__":
    unittest.main()

