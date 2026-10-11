"""Check documented feature names against the source manifest without Cargo."""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]


def documented_features(text):
    section = text.split("## Feature flags\n", 1)[1].split("\n## ", 1)[0]
    return set(re.findall(r"^- `([^`]+)`:", section, re.M))


class DocumentedFeatureTests(unittest.TestCase):
    def test_documented_features_exist_in_cargo_manifest(self):
        manifest = (ROOT / "Cargo.toml").read_text()
        features = manifest.split("[features]\n", 1)[1].split("\n[", 1)[0]
        declared = set(re.findall(r"^([A-Za-z0-9_-]+)\s*=", features, re.M))
        documented = documented_features((ROOT / "README.md").read_text())
        self.assertTrue(documented, "a missing feature inventory must not pass")
        self.assertEqual(documented - declared, set())

    def test_feature_inventory_parser_is_nonvacuous(self):
        self.assertEqual(
            documented_features("## Feature flags\n- `known`: valid\n- `removed`: stale\n"),
            {"known", "removed"},
        )
        self.assertEqual(documented_features("## Feature flags\nNo bullets\n"), set())


if __name__ == "__main__":
    unittest.main()
