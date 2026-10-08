"""Execute credential admission and inspect real release-workflow authority boundaries."""

import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[2]


def workflow(name):
    # YAML 1.1's implicit boolean loader otherwise changes GitHub's `on` key.
    return yaml.load((ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)


class ReleaseWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.pages = workflow("docs.yml")
        self.records = self.pages["jobs"]["records"]
        self.steps = self.records["steps"]

    def step(self, identifier):
        matches = [step for step in self.steps if step.get("id") == identifier]
        self.assertEqual(len(matches), 1)
        return matches[0]

    def test_credential_selection_executes_pair_policy_without_exposing_values(self):
        selector = self.step("credentials")
        self.assertEqual(selector["env"], {
            "APP_CLIENT_ID": "${{ vars.STREAMSKOPE_RELEASE_APP_CLIENT_ID }}",
            "APP_PRIVATE_KEY": "${{ secrets.STREAMSKOPE_RELEASE_APP_PRIVATE_KEY }}",
        })
        for client, private_key, expected, exit_code in [
            ("", "", "use_app=false\n", 0),
            ("fixture-client", "fixture-private-key\nsecond-line", "use_app=true\n", 0),
            ("fixture-client", "", "", 1),
            ("", "fixture-private-key\nsecond-line", "", 1),
        ]:
            with self.subTest(client_present=bool(client), key_present=bool(private_key)):
                with tempfile.TemporaryDirectory() as directory:
                    output = Path(directory) / "output"
                    result = subprocess.run(
                        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", selector["run"]],
                        cwd=directory, timeout=5, text=True, capture_output=True,
                        env={"PATH": os.defpath, "GITHUB_OUTPUT": str(output),
                             "APP_CLIENT_ID": client, "APP_PRIVATE_KEY": private_key},
                    )
                    self.assertEqual(result.returncode, exit_code)
                    actual = output.read_text() if output.exists() else ""
                    self.assertEqual(actual, expected)
                    for sentinel in ("fixture-client", "fixture-private-key", "second-line"):
                        self.assertNotIn(sentinel, result.stdout + result.stderr + actual)

    def test_app_token_is_repository_scoped_and_failure_cannot_fall_back(self):
        selector, app = self.step("credentials"), self.step("app")
        checkout = next(step for step in self.steps
                        if step.get("uses", "").startswith("actions/checkout@"))
        self.assertLess(self.steps.index(selector), self.steps.index(app))
        self.assertLess(self.steps.index(app), self.steps.index(checkout))
        self.assertNotIn("continue-on-error", selector)
        self.assertNotIn("continue-on-error", app)
        self.assertEqual(app["if"], "steps.credentials.outputs.use_app == 'true'")
        self.assertRegex(app["uses"], r"\Aactions/create-github-app-token@[a-f0-9]{40}\Z")
        inputs = app["with"]
        self.assertEqual(inputs["owner"], "${{ github.repository_owner }}")
        self.assertEqual(inputs["repositories"], "${{ github.event.repository.name }}")
        self.assertEqual({key: value for key, value in inputs.items()
                          if key.startswith("permission-")}, {
            "permission-contents": "write", "permission-pull-requests": "write",
            "permission-actions": "read",
        })
        self.assertEqual(self.records["permissions"], {
            "contents": "write", "pull-requests": "write", "actions": "read",
        })
        self.assertNotIn("skip-token-revoke", inputs)
        self.assertEqual(checkout["with"]["token"], "${{ steps.app.outputs.token || github.token }}")
        self.assertEqual(self.step("records")["env"]["GH_TOKEN"], checkout["with"]["token"])
        self.assertEqual(self.step("records")["env"]["RELEASE_DOCS_AUTOMATIC_CI"],
                         "${{ steps.credentials.outputs.use_app }}")

    def test_archive_observation_has_one_cli_owner_and_no_shell_poll(self):
        runs = [step["run"] for step in self.steps if "run" in step]
        archive = [run for run in runs if "tools/package/release-documentation.ts" in run]
        self.assertEqual(len(archive), 1)
        self.assertEqual(shlex.split(archive[0]), [
            "node", "--import", "tsx", "tools/package/release-documentation.ts", "reconcile",
        ])
        for run in runs:
            self.assertNotRegex(run, r"(?m)^\s*(?:for\s|while\s|sleep\s|gh\s+pr\s+view\b)")
        self.assertFalse(any(step.get("name") == "Require the documentation PR to merge"
                             for step in self.steps))

    def test_pages_stays_release_only_and_independent_of_archive_handoff(self):
        self.assertEqual(self.pages["on"], {"release": {"types": ["published"]}})
        jobs = self.pages["jobs"]
        self.assertNotIn("needs", jobs["records"])
        self.assertNotIn("needs", jobs["select"])
        self.assertEqual(jobs["build"]["needs"], "select")
        self.assertEqual(jobs["deploy"]["needs"], "build")
        self.assertEqual(jobs["build"]["if"], "needs.select.outputs.publish == 'true'")
        source = next(step for step in jobs["build"]["steps"]
                      if step.get("uses", "").startswith("actions/checkout@"))
        self.assertEqual(source["with"]["ref"], "${{ github.sha }}")
        self.assertTrue(any(step.get("run") == "python3 tools/docs.py guard"
                            for step in jobs["select"]["steps"]))
        self.assertTrue(any(step.get("run") == "python3 tools/docs.py guard"
                            for step in jobs["deploy"]["steps"]))

    def test_ordinary_pr_ci_never_excludes_app_authors(self):
        ci = workflow("ci.yml")
        self.assertEqual(set(ci["on"]), {"pull_request", "workflow_call"})
        self.assertIn(ci["on"]["pull_request"], ("", None))

        def inspect(value):
            if isinstance(value, dict):
                for key, nested in value.items():
                    if key == "if":
                        self.assertNotRegex(nested, r"github\.(?:actor|triggering_actor)|\b(?:author|sender|user)\b|\[bot\]")
                    inspect(nested)
            elif isinstance(value, list):
                for nested in value:
                    inspect(nested)

        inspect(ci)


if __name__ == "__main__":
    unittest.main()
