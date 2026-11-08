#!/usr/bin/env python3
"""Selftest of check_record_parity.py: a new field without a contract row is red, a row without a field is red, equal sets are green."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_record_parity as parity  # noqa: E402

CORE = """
// a comment, with a comma, and { braces }
#[derive(Clone, uniffi::Record)]
pub struct Sample {
    /// the id, always set
    pub id: String,
    pub size: u64,
}

#[derive(uniffi::Enum)]
pub enum Outcome {
    Done,
    Failed { code: u16, detail: String },
}
"""
BRIDGE = """
| BREC-001 | Sample | id, size | Sample | Sample | P-BR-05 |
| BREC-002 | Outcome | Done, Failed.code, Failed.detail | Outcome | Outcome | P-BR-05 |
"""


class RecordParity(unittest.TestCase):
    def problems(self, core, bridge):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "core.rs").write_text(core)
            (root / "bridge.md").write_text(bridge)
            return parity.compare(parity.core_records(root), parity.contract_rows(root / "bridge.md"))

    def test_equal_sets_are_green(self):
        self.assertEqual(self.problems(CORE, BRIDGE), [])

    def test_a_new_field_without_a_row_is_red(self):
        self.assertEqual(self.problems(CORE.replace("pub size: u64,", "pub size: u64,\n    pub local_url: Option<String>,"), BRIDGE), ["Sample.local_url of the core has no contract field"])

    def test_a_new_variant_field_without_a_row_is_red(self):
        self.assertEqual(self.problems(CORE.replace("detail: String", "detail: String, session_code: u8"), BRIDGE), ["Outcome.Failed.session_code of the core has no contract field"])

    def test_a_row_without_a_field_is_red(self):
        self.assertEqual(self.problems(CORE, BRIDGE.replace("id, size", "id, size, gone")), ["contract row Sample lists gone, the core has no such field"])


if __name__ == "__main__":
    unittest.main()
