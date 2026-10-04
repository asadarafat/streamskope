"""Behavioral checks for the public documentation artifact boundary."""

import importlib.util
import json
import re
from pathlib import Path
import subprocess
import tempfile
import unittest
import sys
from io import BytesIO
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
spec = importlib.util.spec_from_file_location("docs_cli", Path(__file__).resolve().parents[2] / "tools/docs.py")
docs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(docs)


class DocumentationCommandTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        subprocess.run(["git", "init", "--quiet", str(self.root)], check=True)
        (self.root / "package.json").write_text(json.dumps({
            "scripts": {"dev": "start", "check": "check"},
        }))

    def track(self, relative, content):
        file = self.root / relative
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content)
        subprocess.run(["git", "add", "--", relative], cwd=self.root, check=True)

    def test_accepts_current_scripts_with_arguments_and_environment(self):
        self.track("README.md",
                   "npm ci\nHOST=localhost npm run dev\n"
                   "npm run check -- --ci\nnpm run-script check\n")
        docs.inspect_repository_commands(self.root)

    def test_reports_all_removed_scripts_with_file_and_line(self):
        self.track("README.md", "# Development\n`npm run dev:web`\n")
        self.track("fixture/README.md", "```bash\nnpm run fixture:start -- --name demo\n```\n")
        with self.assertRaises(ValueError) as raised:
            docs.inspect_repository_commands(self.root)
        message = str(raised.exception)
        self.assertIn("README.md:2: npm run dev:web", message)
        self.assertIn("fixture/README.md:2: npm run fixture:start", message)
        self.assertIn("Available scripts: check, dev", message)

    def test_excludes_untracked_and_ignored_private_notes(self):
        self.track("README.md", "npm run check\n")
        (self.root / ".gitignore").write_text("private/\n")
        (self.root / "private").mkdir()
        (self.root / "private/notes.md").write_text("npm run retired-private-command\n")
        (self.root / "untracked.md").write_text("npm run retired-local-command\n")
        docs.inspect_repository_commands(self.root)

    def test_checks_tracked_working_tree_edits(self):
        self.track("README.md", "npm run dev\n")
        (self.root / "README.md").write_text("npm run removed\n")
        with self.assertRaisesRegex(ValueError, "README.md:1: npm run removed"):
            docs.inspect_repository_commands(self.root)


class DocumentationLimitTests(unittest.TestCase):
    def test_rejects_a_stale_operator_limit(self):
        source = (docs.WEBSITE / "docs/guide/data-handling.md").read_text()
        for boundary in ("Retained message bytes", "Broker search scan",
                         "Saved query library", "Portable query document"):
            with self.subTest(boundary=boundary):
                changed, count = re.subn(
                    r"(\|\s*" + re.escape(boundary) + r"\s*\|)[^|]+",
                    r"\1 incorrect ", source,
                )
                self.assertEqual(count, 1)
                with tempfile.TemporaryDirectory() as directory:
                    page = Path(directory) / "data.md"
                    page.write_text(changed)
                    with self.assertRaisesRegex(ValueError, boundary):
                        docs.inspect_message_limits(page)

    def test_rejects_an_omitted_operator_limit(self):
        source = (docs.WEBSITE / "docs/guide/data-handling.md").read_text()
        with tempfile.TemporaryDirectory() as directory:
            page = Path(directory) / "data.md"
            page.write_text("\n".join(line for line in source.splitlines()
                                      if not re.search(r"\|\s*Value preview\s*\|", line)))
            with self.assertRaisesRegex(ValueError, "Value preview: expected .*found missing"):
                docs.inspect_message_limits(page)


class DocumentationVersionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "website/docs/releases").mkdir(parents=True)
        (self.root / "website/docs/guide").mkdir()
        self.qualification = self.root / "website/docs/guide/qualification.md"
        self.qualification.write_text(
            "# Qualification evidence\n\nGeneral evidence guidance.\n\n"
            "## Published release: v0.1.0+build.1\n\nHistorical checks.\n")
        self.source_version("0.0.0-dev")
        (self.root / "website/zensical.toml").write_text(
            '[project.extra]\ndesktop_release = "v0.1.0+build.1"\n')
        self.notes("v0.1.0+build.1", "0.1.0")
        self.unreleased = self.root / "website/docs/releases/unreleased.md"
        self.unreleased.write_text(
            "---\ntitle: Unreleased changes\nunreleased: true\n---\n# Unreleased changes\n")

    def source_version(self, version):
        (self.root / "package.json").write_text(json.dumps({"version": version}))

    def notes(self, tag, version):
        (self.root / f"website/docs/releases/{tag}.md").write_text(
            f"---\nrelease_version: {version}\nrelease_tag: {tag}\n---\n# Release\n")

    def context(self, tag, **values):
        return docs.documentation_context(self.root, {
            "GITHUB_EVENT_NAME": "push", "GITHUB_REF_TYPE": "tag",
            "GITHUB_REF_NAME": tag, **values,
        })

    def release_environment(self, version="0.2.0"):
        subprocess.run(["git", "init", "--quiet", str(self.root)], check=True)
        subprocess.run(["git", "-c", "user.name=Docs test", "-c", "user.email=docs@example.invalid",
                        "commit", "--quiet", "--allow-empty", "-m", "Release source"],
                       cwd=self.root, check=True)
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root, text=True).strip()
        tag = "v" + version
        self.source_version(version)
        self.notes(tag, version)
        (self.root / "website/docs/releases/index.md").write_text(
            "# Releases\n\n| [Unreleased changes](unreleased.md) | Unassigned | Development |\n"
            "| [v0.1.0+build.1](v0.1.0+build.1.md) | 0.1.0 | Historical release |\n")
        self.event = self.root / "release-event.json"
        self.event.write_text(json.dumps({"action": "published", "release": {
            "tag_name": tag, "draft": False, "prerelease": "-" in version,
            "body": f"# StreamSkope {tag}\n\nReviewed **final** notes, with edits from the draft.\n",
        }}))
        return {"GITHUB_EVENT_NAME": "release", "GITHUB_REF_NAME": tag,
                "GITHUB_REF": "refs/tags/" + tag, "GITHUB_SHA": revision,
                "GITHUB_EVENT_PATH": str(self.event), "STREAMSKOPE_DOCS_PUBLISH": "1"}

    def test_main_is_a_preview_and_cannot_be_published_under_the_previous_release(self):
        self.assertEqual(docs.documentation_context(self.root, {}), {
            "status": "Development documentation", "source_release": "development",
        })
        for event in ("push", "pull_request", "workflow_dispatch"):
            with self.subTest(event=event), self.assertRaisesRegex(ValueError, "published desktop release event"):
                docs.documentation_context(self.root, {
                    "GITHUB_EVENT_NAME": event, "GITHUB_REF_NAME": "main",
                    "STREAMSKOPE_DOCS_PUBLISH": "1",
                })

    def test_manual_release_checks_unstamped_source_without_assigning_input_version(self):
        context = docs.documentation_context(self.root, {
            "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF_NAME": "main",
            "RELEASE_VERSION": "0.2.0",
        })
        self.assertEqual(context["source_release"], "development")
        self.assertEqual(json.loads((self.root / "package.json").read_text())["version"],
                         "0.0.0-dev")

    def test_requires_unversioned_notes_for_development_source(self):
        for invalid in ("", "# Unreleased changes\n",
                        "---\nunreleased: false\n---\n# Unreleased changes\n",
                        "---\nunreleased: true\nunreleased: true\n---\n# Unreleased changes\n",
                        "---\nunreleased: true\nrelease_tag: v0.2.0\n---\n# Unreleased changes\n",
                        "---\nunreleased: true\nrelease_version: 0.2.0\n---\n# Unreleased changes\n",
                        "---\nunreleased: true\n---\n# StreamSkope 0.2.0\n"):
            with self.subTest(invalid=invalid):
                self.unreleased.write_text(invalid)
                with self.assertRaisesRegex(ValueError, "unversioned unreleased notes"):
                    docs.documentation_context(self.root, {})

    def test_stamped_release_keeps_published_download_baseline(self):
        for version in ("0.2.0", "0.2.0-rc.1"):
            with self.subTest(version=version):
                self.source_version(version)
                self.notes("v" + version, version)
                context = docs.documentation_context(self.root, {
                    "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF_NAME": "main",
                })
                self.assertEqual(context["source_release"], "v" + version)
                self.assertIn('desktop_release = "v0.1.0+build.1"',
                              (self.root / "website/zensical.toml").read_text())

    def test_tag_alone_cannot_assign_a_development_release_version(self):
        with self.assertRaisesRegex(ValueError, "tag differs"):
            self.context("v0.2.0")

    def test_plugin_only_release_keeps_desktop_in_development(self):
        self.assertEqual(self.context("plugins/nsp/v0.1.0")["source_release"], "development")

    def test_publication_aligns_tagged_source_notes_and_downloads_reproducibly(self):
        environment = self.release_environment()
        with self.assertRaisesRegex(ValueError, "downloads must match"):
            docs.documentation_context(self.root, environment)
        docs.publication.prepare_publication(self.root, environment)
        result = docs.documentation_context(self.root, environment)
        self.assertEqual(result, {
            "status": "Published documentation", "source_release": "v0.2.0",
        })
        identity = docs.publication_identity(self.root)
        self.assertEqual(identity, {"revision": environment["GITHUB_SHA"], "desktop_release": "v0.2.0"})
        notes = self.root / "website/docs/releases/v0.2.0.md"
        self.assertEqual(notes.read_text().split("---\n\n", 1)[1], json.loads(self.event.read_text())["release"]["body"])
        self.assertFalse(self.unreleased.exists())
        self.assertIn("[v0.2.0](v0.2.0.md)", (self.root / "website/docs/releases/index.md").read_text())
        self.assertTrue((self.root / "website/docs/releases/v0.1.0+build.1.md").is_file())
        before = {p: p.read_bytes() for p in self.root.rglob("*.md")}
        docs.publication.prepare_publication(self.root, environment)
        self.assertEqual(before, {p: p.read_bytes() for p in self.root.rglob("*.md")})
        (self.root / "website/docs/start").mkdir()
        (self.root / "website/docs/start/installation.md").write_text("<!-- desktop-downloads -->")
        links = docs.desktop_downloads(self.root)
        self.assertEqual(links["tag"], "v0.2.0")
        self.assertTrue(all("/v0.2.0/StreamSkope-0.2.0-" in a["url"] for a in links["assets"]))

    def test_published_prerelease_cannot_replace_stable_documentation(self):
        environment = self.release_environment("0.3.0-rc.1")
        before = self.qualification.read_text()
        with self.assertRaisesRegex(ValueError, "stable desktop"):
            docs.publication.prepare_publication(self.root, environment)
        self.assertEqual(self.qualification.read_text(), before)

    def test_publication_links_only_the_exact_uploaded_qualification_asset(self):
        environment = self.release_environment()
        event = json.loads(self.event.read_text())
        asset = {
            "name": "qualification-v0.2.0.json", "state": "uploaded", "size": 1200,
            "browser_download_url": "https://github.com/asadarafat/streamskope/releases/download/v0.2.0/qualification-v0.2.0.json",
        }
        for assets in (None, [], [{**asset, "size": 0}], [{**asset, "size": True}],
                       [{**asset, "state": "new"}], [{**asset, "name": "qualification-v0.1.0.json"}],
                       [{**asset, "browser_download_url": "https://example.invalid/report"}], [asset]):
            with self.subTest(assets=assets):
                self.event.write_text(json.dumps({**event, "release": {**event["release"], "assets": assets}}))
                docs.publication.prepare_publication(self.root, environment)
                content = self.qualification.read_text()
                self.assertEqual(content.count("## Published release:"), 1)
                self.assertIn("## Published release: v0.2.0", content)
                self.assertIn("## Historical qualification: v0.1.0+build.1", content)
                self.assertIn(environment["GITHUB_SHA"], content)
                self.assertIn("Historical checks.", content)
                if assets == [asset]:
                    self.assertIn(asset["browser_download_url"], content)
                    self.assertIn("does not establish that every check passed", content)
                else:
                    self.assertNotIn("releases/download/", content)
                    self.assertIn("does not mark them as passed", content)

    def test_bad_qualification_structure_does_not_partially_prepare_a_release(self):
        environment = self.release_environment()
        self.qualification.write_text("# Qualification evidence\n")
        original = {p: p.read_bytes() for p in self.root.glob("website/**/*") if p.is_file()}
        with self.assertRaisesRegex(ValueError, "release evidence section"):
            docs.publication.prepare_publication(self.root, environment)
        self.assertEqual(original, {p: p.read_bytes() for p in self.root.glob("website/**/*") if p.is_file()})

    def test_rejects_wrong_event_revision_version_or_notes_before_preparing_publication(self):
        environment = self.release_environment()
        event = json.loads(self.event.read_text())
        original = {p: p.read_bytes() for p in self.root.glob("website/**/*") if p.is_file()}
        for change in (
            {"GITHUB_SHA": "b" * 40}, {"GITHUB_REF": "refs/heads/main"},
            {"GITHUB_REF_NAME": "v0.3.0"}, {"GITHUB_EVENT_NAME": "workflow_dispatch"},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                docs.publication.prepare_publication(self.root, {**environment, **change})
        for key, value in (("draft", True), ("body", ""), ("body", None),
                           ("tag_name", "plugins/nsp/v0.2.0")):
            self.event.write_text(json.dumps({**event, "release": {**event["release"], key: value}}))
            with self.subTest(key=key), self.assertRaises(ValueError):
                docs.publication.prepare_publication(self.root, environment)
        self.event.write_text(json.dumps({**event, "action": "created"}))
        with self.assertRaises(ValueError):
            docs.publication.prepare_publication(self.root, environment)
        self.event.write_text(json.dumps(event))
        for version in ("0.0.0-dev", "0.3.0"):
            self.source_version(version)
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "stamped application version"):
                docs.publication.prepare_publication(self.root, environment)
        self.assertEqual(original, {p: p.read_bytes() for p in self.root.glob("website/**/*") if p.is_file()})

    def test_requires_exact_notes_for_both_published_and_stamped_versions(self):
        self.source_version("0.2.0")
        self.notes("v0.2.0", "0.2.0")
        for tag in ("v0.1.0+build.1", "v0.2.0"):
            path = self.root / f"website/docs/releases/{tag}.md"
            content = path.read_text()
            for invalid in ("", content.replace("release_version:", "wrong_version:"),
                            content.replace("release_tag:", "wrong_tag:"),
                            content.replace("release_tag:", "release_tag: v9.9.9\nrelease_tag:"),
                            content.replace("release_version:", "release_version: 9.9.9\nrelease_version:"),
                            content.replace(f"release_tag: {tag}", f"release_tag: {tag}\nrelease_tag: {tag}")):
                with self.subTest(tag=tag, invalid=invalid):
                    path.write_text(invalid)
                    with self.assertRaisesRegex(ValueError, "exact tag and application version"):
                        docs.documentation_context(self.root, {})
            path.write_text(content)

    def test_source_rejects_non_semver_and_build_metadata(self):
        for version in ("0.2.0+build.1", "0.02.0", "0.2.0-rc.01", "v0.2.0"):
            with self.subTest(version=version):
                self.source_version(version)
                with self.assertRaisesRegex(ValueError, "SemVer without build metadata"):
                    docs.documentation_context(self.root, {})


class DocumentationArtifactTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "guide").mkdir()
        (self.root / "guide/index.html").write_text('<h1 id="connect">Connect</h1>')

    def test_accepts_relative_and_project_subpath_links(self):
        (self.root / "index.html").write_text(
            '<a href="guide/#connect">Guide</a>'
            '<a href="/streamskope/guide/#connect">Connect</a>'
        )
        docs.inspect_site(self.root, "/streamskope/")

    def test_accepts_only_named_public_intro_exports(self):
        (self.root / "assets").mkdir()
        (self.root / "assets/streamskope-intro-light.mp4").write_bytes(b"public light export")
        (self.root / "assets/streamskope-intro-dark.mp4").write_bytes(b"public dark export")
        (self.root / "index.html").write_text('<video src="assets/streamskope-intro-light.mp4"></video>')
        docs.inspect_site(self.root)
        (self.root / "assets/private.mp4").write_bytes(b"private recording")
        with self.assertRaisesRegex(ValueError, "Unexpected publication file"):
            docs.inspect_site(self.root)

    def test_accepts_only_named_public_qualification_summary(self):
        directory = self.root / "assets/qualification"
        directory.mkdir(parents=True)
        names = ("lifecycle-2026-10-04.json", "topic-monitor-2026-10-04.json")
        for name in names:
            (directory / name).write_text('{"schemaVersion":1}')
        (self.root / "index.html").write_text(
            "".join(f'<a href="assets/qualification/{name}">Qualification</a>' for name in names)
        )
        docs.inspect_site(self.root)
        (directory / "raw-live-report.json").write_text('{"private":"not for publication"}')
        with self.assertRaisesRegex(ValueError, "Unexpected publication file"):
            docs.inspect_site(self.root)

    def test_rejects_missing_page_and_anchor(self):
        for link in ["missing/", "guide/#missing"]:
            with self.subTest(link=link):
                (self.root / "index.html").write_text(f'<a href="{link}">Broken</a>')
                with self.assertRaisesRegex(ValueError, "Broken local link"):
                    docs.inspect_site(self.root, "/streamskope/")

    def test_validates_themed_images_even_before_the_browser_assigns_src(self):
        (self.root / "assets").mkdir()
        (self.root / "assets/light.png").write_bytes(b"image")
        (self.root / "index.html").write_text('<img data-sk-light="light.png" data-sk-dark="missing.png">')
        with self.assertRaisesRegex(ValueError, "missing.png"):
            docs.inspect_site(self.root, "/streamskope/")

    def test_rejects_private_artifacts_and_symlinks(self):
        (self.root / "index.html").write_text("<h1>Home</h1>")
        for name in ["private.pem", "profile.json", "private.mp4", "private.m4a", "private.mp3"]:
            with self.subTest(name=name):
                artifact = self.root / name
                artifact.write_text("not public")
                with self.assertRaisesRegex(ValueError, "Unexpected publication file"):
                    docs.inspect_site(self.root, "/")
                artifact.unlink()
        link = self.root / "alias.html"
        link.symlink_to(self.root / "index.html")
        with self.assertRaisesRegex(ValueError, "symlink"):
            docs.inspect_site(self.root, "/")

    def test_rejects_traversal_and_machine_paths(self):
        for html in ['<a href="../../outside.html">Outside</a>', '<p>/Users/alice/private</p>']:
            (self.root / "index.html").write_text(html)
            with self.assertRaises(ValueError):
                docs.inspect_site(self.root, "/")


class PublicationTests(unittest.TestCase):
    revision = "a" * 40
    release = "v0.1.0+build.4"

    def response(self, url, **kwargs):
        if "documentation.json" in url:
            text = json.dumps({"revision": self.revision, "desktop_release": self.release})
        elif "/guide/qualification/" in url:
            text = (f'<meta name="streamskope-docs-revision" content="{self.revision}">'
                    f'<h2>Published release: {self.release}</h2>')
        elif "/guide/" in url:
            plugin = "eda" if "/eda/" in url else "nsp"
            text = f'window.location.replace("../../plugins/{plugin}/" + window.location.hash)'
        else:
            text = f'<meta name="streamskope-docs-revision" content="{self.revision}">'
        return BytesIO(text.encode())

    def test_verifies_exact_revision_pages_and_bookmark_redirects(self):
        with patch.object(docs.publication, "urlopen", side_effect=self.response) as request:
            docs.verify_publication("https://docs.example/", self.revision, self.release, attempts=1)
        self.assertEqual(request.call_count, 8)

    def test_stale_marker_retries_then_verifies_routes(self):
        stale = BytesIO(b'{"revision":"old","desktop_release":"v0.1.0"}')
        with patch.object(docs.publication, "urlopen", side_effect=[stale] + [
            self.response("documentation.json"), *[self.response("") for _ in range(4)],
            self.response("/guide/qualification/"),
            self.response("/guide/eda/"), self.response("/guide/nsp/"),
        ]) as request:
            docs.verify_publication("https://docs.example/", self.revision, self.release, attempts=2, delay=0)
        self.assertEqual(request.call_count, 9)

    def test_matching_marker_does_not_hide_a_stale_page_or_broken_redirect(self):
        for broken in ("/plugins/nsp/", "/guide/eda/", "/guide/qualification/"):
            def response(url, **kwargs):
                return BytesIO(b"old page") if broken in url else self.response(url, **kwargs)
            with self.subTest(broken=broken), patch.object(docs.publication, "urlopen", side_effect=response):
                with self.assertRaisesRegex(ValueError, "Public documentation verification failed"):
                    docs.verify_publication("https://docs.example/", self.revision, self.release, attempts=1)

    def test_rejects_current_revision_with_previous_release_qualification(self):
        def response(url, **kwargs):
            if "/guide/qualification/" in url:
                return BytesIO((f'<meta name="streamskope-docs-revision" content="{self.revision}">'
                                '<h2>Published release: v0.0.1</h2>').encode())
            return self.response(url, **kwargs)
        with patch.object(docs.publication, "urlopen", side_effect=response):
            with self.assertRaisesRegex(ValueError, "describes another release"):
                docs.verify_publication("https://docs.example/", self.revision, self.release, attempts=1)


class MediaSelectionTests(unittest.TestCase):
    def test_media_and_its_dependencies_require_playback_but_prose_does_not(self):
        for path in ("website/docs/intro/index.html", "website/docs/assets/film.mp4",
                     "tools/docs/media.mjs", "website/overrides/main.html", "package-lock.json"):
            self.assertTrue(docs.media_inputs(path), path)
        self.assertFalse(docs.media_inputs("website/docs/plugins/nsp.md"))

    def test_local_cache_is_invalidated_by_working_tree_media_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "--quiet", str(root)], check=True)
            media = root / "website/docs/assets/film.mp4"
            media.parent.mkdir(parents=True)
            media.write_bytes(b"first")
            required, fingerprint = docs.media_selection(root, {})
            self.assertTrue(required)
            (root / ".cache").mkdir()
            (root / ".cache/docs-media-qualified").write_text(fingerprint)
            self.assertFalse(docs.media_selection(root, {})[0])
            media.write_bytes(b"changed")
            self.assertTrue(docs.media_selection(root, {})[0])

    def test_unknown_ci_history_requires_full_playback(self):
        self.assertTrue(docs.media_selection(environment={"CI": "true"})[0])

    def test_ci_diff_skips_prose_but_detects_deleted_media_for_push_and_pr(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", "-c", "user.name=Docs test",
                    "-c", "user.email=docs@example.invalid", *args], cwd=root, text=True).strip()
            git("init", "--quiet")
            media = root / "website/docs/assets/film.mp4"
            media.parent.mkdir(parents=True)
            media.write_bytes(b"film")
            (root / "README.md").write_text("before")
            git("add", ".")
            git("commit", "--quiet", "-m", "fixture")
            base = git("rev-parse", "HEAD")
            (root / "README.md").write_text("prose change")
            git("commit", "--quiet", "-am", "prose")
            event = root / "event.json"
            event.write_text(json.dumps({"before": base, "pull_request": {"base": {"sha": base}}}))
            for expected in (False, True):
                if expected:
                    media.unlink()
                    git("commit", "--quiet", "-am", "remove media")
                for name in ("push", "pull_request"):
                    self.assertEqual(docs.media_selection(root, {
                        "CI": "true", "GITHUB_EVENT_NAME": name, "GITHUB_EVENT_PATH": str(event),
                    })[0], expected)


if __name__ == "__main__":
    unittest.main()
