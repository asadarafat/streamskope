"""Verify media integrity admission and per-page terminology without network access."""
import hashlib
from io import BytesIO
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from docs.video_assets import fetch_videos, video_assets, inspect_video_references
from docs.terminology import Terminology, explain_terms


class VideoTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "website").mkdir()
        self.bytes = b"approved video"
        self.asset = {"path": "assets/skope/example.mp4", "name": "example.mp4",
                      "url": "https://github.com/asadarafat/streamskope/releases/download/documentation-media/v1/example.mp4",
                      "sha256": hashlib.sha256(self.bytes).hexdigest(), "bytes": len(self.bytes)}
        self.manifest()

    def manifest(self):
        (self.root / "website/media.json").write_text(json.dumps({
            "schemaVersion": 1, "release": "documentation-media/v1", "assets": [self.asset]}))

    def test_bad_download_does_not_poison_cache_and_is_retried(self):
        with patch("docs.video_assets.urlopen", return_value=BytesIO(b"wrong content")):
            with self.assertRaisesRegex(ValueError, "approved digest"):
                fetch_videos(self.root)
        with patch("docs.video_assets.urlopen", return_value=BytesIO(self.bytes)) as fetch:
            fetch_videos(self.root)
            fetch_videos(self.root)
            self.assertEqual(fetch.call_count, 1)

    def test_corrupt_cache_is_refetched(self):
        with patch("docs.video_assets.urlopen", return_value=BytesIO(self.bytes)):
            fetch_videos(self.root)
        cache = self.root / ".cache/docs-media" / self.asset["sha256"] / self.asset["name"]
        cache.write_bytes(b"corrupt")
        with patch("docs.video_assets.urlopen", return_value=BytesIO(self.bytes)) as fetch:
            fetch_videos(self.root)
            self.assertEqual(fetch.call_count, 1)
        self.assertEqual(cache.read_bytes(), self.bytes)

    def test_path_or_url_cannot_escape_approved_distribution(self):
        for key, value in [("path", "../secret.mp4"), ("url", "https://example.com/latest.mp4"),
                           ("bytes", True), ("sha256", "latest")]:
            old = self.asset[key]
            self.asset[key] = value
            self.manifest()
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "identity"):
                video_assets(self.root)
            self.asset[key] = old

    def test_pages_uses_pinned_urls_without_binary_masters(self):
        site = self.root / "site"
        site.mkdir()
        (site / "index.html").write_text(f'<source src="{self.asset["url"]}">')
        inspect_video_references(site, self.root)
        (site / "example.mp4").write_bytes(self.bytes)
        with self.assertRaisesRegex(ValueError, "Pages artifact"):
            inspect_video_references(site, self.root)


class TerminologyTests(unittest.TestCase):
    def render(self, content):
        parser = Terminology()
        parser.feed(content)
        return "".join(parser.output)

    def test_explains_first_prose_use_on_each_page_preserving_ui_and_code(self):
        source = ('<nav>EDA</nav><article class="md-content__inner"><h1>EDA capture</h1>'
                  '<p>Select <strong>EDA API URL</strong>. Supply EDA API credentials.</p>'
                  '<pre>EDA API</pre><p>EDA API calls use TLS.</p></article>')
        output = self.render(source)
        self.assertIn('<strong>EDA API URL</strong>', output)
        self.assertIn('<pre>EDA API</pre>', output)
        self.assertEqual(output.count('Nokia Event-Driven Automation'), 1)
        self.assertEqual(output.count('application programming interface'), 1)
        self.assertIn('Transport Layer Security', output)
        self.assertEqual(output, self.render(source))
        self.assertEqual(output, self.render(output))

    def test_existing_definition_and_identifiers_are_not_expanded_twice(self):
        output = self.render('<article class="md-content__inner"><p>'
                             'Transport Layer Security (TLS). API (application programming interface). '
                             'TLS API API_TOKEN &amp; <code>NSP_API_URL</code>.</p></article>')
        self.assertNotIn('skope-term', output)
        self.assertIn('API_TOKEN &amp;', output)

    def test_brand_explanation_does_not_invent_an_acronym(self):
        output = self.render('<article class="md-content__inner"><p>NATS and AIO.</p></article>')
        self.assertIn('All-in-One', output)
        self.assertNotIn('Not Another', output)
        self.assertIn('publish/subscribe', output)

    def test_plural_first_uses_share_a_definition_with_the_singular(self):
        source = '<article class="md-content__inner"><p>Read ACLs and writer IDs. One ACL has an ID.</p></article>'
        output = self.render(source)
        self.assertIn('ACLs (access control lists)', output)
        self.assertIn('IDs (identifiers)', output)
        self.assertNotIn('ACL (access control list)', output)
        self.assertEqual(output, self.render(output))

    def test_search_indexes_full_names_without_changing_titles_or_locations(self):
        with tempfile.TemporaryDirectory() as directory:
            site = Path(directory)
            (site / "index.html").write_text('<article class="md-content__inner"><p>TLS trust.</p></article>')
            (site / "search.json").write_text(json.dumps({"config": {}, "items": [{
                "location": "guide/trust/#tls", "title": "TLS trust", "text": "TLS trust <code>TLS_CA</code>"}]}))
            explain_terms(site)
            item = json.loads((site / "search.json").read_text())["items"][0]
            self.assertEqual(item["title"], "TLS trust")
            self.assertEqual(item["location"], "guide/trust/#tls")
            self.assertIn("Transport Layer Security", item["text"])
            self.assertIn("<code>TLS_CA</code>", item["text"])


if __name__ == "__main__":
    unittest.main()
