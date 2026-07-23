from __future__ import annotations

import unittest

from tools.project_standards_check import KNOWLEDGE_ROOT, REQUIRED_FILES, main


class ProjectStandardsCheckTests(unittest.TestCase):
    def test_required_standard_files_exist(self) -> None:
        if not KNOWLEDGE_ROOT.exists():
            self.skipTest("Developer_Knowledge рядом не найден")
        missing = [rel_path for rel_path in REQUIRED_FILES if not (KNOWLEDGE_ROOT / rel_path).exists()]
        self.assertEqual(missing, [])

    def test_main_passes(self) -> None:
        self.assertEqual(main(), 0)


if __name__ == "__main__":
    unittest.main()
