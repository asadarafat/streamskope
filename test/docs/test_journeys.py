from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs.journeys import inspect_skope_journeys


class JourneyTests(unittest.TestCase):
    def test_new_workflow_cannot_rely_on_a_video_without_written_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            docs = root / "website/docs"
            docs.mkdir(parents=True)
            (root / "website/zensical.toml").write_text('[project]\nnav = [{"Under the Skope" = [{"New workflow" = "new.md"}]}]\n')
            (docs / "new.md").write_text('# New workflow\n<video></video>\n')
            with self.assertRaisesRegex(ValueError, "new.md: missing written section"):
                inspect_skope_journeys(root)
            (docs / "new.md").write_text('''# New workflow
## What this workflow does
Compare a resource with a known source.
## The problem it solves
An operator cannot explain a configuration difference.
## Before you start
Have read permission on both sources.
## How to solve it
1. Select the sources.
2. Read the comparison.
## Check the result
Keep the differing value and both observation times.
''')
            inspect_skope_journeys(root)

    def test_existing_guides_answer_the_reader_questions(self):
        inspect_skope_journeys(Path(__file__).resolve().parents[2])


if __name__ == "__main__":
    unittest.main()
