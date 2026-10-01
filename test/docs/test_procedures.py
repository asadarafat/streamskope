"""Regression checks for the operator's retrieval procedure."""

from pathlib import Path
import shutil
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
from docs.procedures import inspect_retrieval_procedure


class RetrievalProcedureTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.page = self.root / "website/docs/guide/secret-retrieval.md"
        self.page.parent.mkdir(parents=True)
        shutil.copyfile(ROOT / self.page.relative_to(self.root), self.page)
        ui = Path("src/features/kafka/ui")
        (self.root / ui).mkdir(parents=True)
        for component in ("ProfileDialog.tsx", "ProfileTrustRecipeSelector.tsx",
                          "TrustRecipeManagementButton.tsx", "RemoteTrustAcquisitionPanel.tsx"):
            shutil.copyfile(ROOT / ui / component, self.root / ui / component)

    def test_accepts_current_guide_and_ui_labels(self):
        inspect_retrieval_procedure(self.root)

    def test_rejects_omitted_apply_even_when_mentioned_outside_the_procedure(self):
        lines = self.page.read_text().splitlines()
        self.page.write_text("\n".join(line for line in lines
                                       if not line.startswith("6. Choose **Apply to connection**"))
                             + "\n\n**Apply to connection** fills the draft.\n")
        with self.assertRaisesRegex(ValueError, "missing procedure step: Apply to connection"):
            inspect_retrieval_procedure(self.root)

    def test_rejects_testing_before_apply(self):
        lines = self.page.read_text().splitlines()
        apply = next(i for i, line in enumerate(lines)
                     if line.startswith("6. Choose **Apply to connection**"))
        lines[apply], lines[apply + 1] = lines[apply + 1], lines[apply]
        self.page.write_text("\n".join(lines))
        with self.assertRaisesRegex(ValueError, "procedure step out of order: Test connection"):
            inspect_retrieval_procedure(self.root)

    def test_rejects_omitted_result_review_despite_ssh_identity_review(self):
        self.page.write_text("\n".join(line for line in self.page.read_text().splitlines()
                                       if not line.startswith("5. Review")))
        with self.assertRaisesRegex(ValueError, "missing procedure step: Review"):
            inspect_retrieval_procedure(self.root)

    def test_rejects_stale_documented_control(self):
        self.page.write_text(self.page.read_text().replace("**Manage…**", "**Manage retrieval profiles**"))
        with self.assertRaisesRegex(ValueError, "missing documented control: Manage…"):
            inspect_retrieval_procedure(self.root)

    def test_requires_review_when_ui_control_changes(self):
        component = self.root / "src/features/kafka/ui/ProfileTrustRecipeSelector.tsx"
        component.write_text(component.read_text().replace('label="Retrieval preset"',
                                                           'label="Saved retrieval"'))
        with self.assertRaisesRegex(ValueError, "UI control changed: Retrieval preset"):
            inspect_retrieval_procedure(self.root)


if __name__ == "__main__":
    unittest.main()
