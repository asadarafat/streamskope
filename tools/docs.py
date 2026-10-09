"""Build and validate public documentation using the pinned Zensical toolchain."""

import argparse
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import unquote, urlsplit

from docs import publication
from docs.publication import documentation_context, publication_identity, verify_publication
from docs.media import media_inputs, media_selection
from docs.downloads import desktop_downloads, container_downloads
from docs.plugins import SNAPSHOT, capture_publications, plugin_context
from docs.procedures import inspect_retrieval_procedure
from docs.releases import configure_release_navigation
from docs.qualification import write_qualification

ROOT = Path(__file__).resolve().parents[1]
WEBSITE = ROOT / "website"
SITE = ROOT / "dist/site"
BUILD = WEBSITE / ".site"
ALLOWED_SUFFIXES = {
    ".html", ".css", ".js", ".svg", ".png", ".webp", ".ico",
    ".woff", ".woff2", ".ttf", ".xml", ".gz", ".map",
}


def inspect_repository_commands(root=ROOT):
    """Check npm script examples in tracked Markdown against the root manifest."""
    root = Path(root)
    scripts = json.loads((root / "package.json").read_text(encoding="utf8"))["scripts"]
    tracked = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "*.md"], cwd=root,
    ).decode("utf8").split("\0")
    command_pattern = re.compile(r"\bnpm[ \t]+(?:run|run-script)[ \t]+([\w.:-]+)")
    failures = []
    files = 0
    for relative in filter(None, tracked):
        file = root / relative
        if not file.is_file():
            continue
        files += 1
        for number, line in enumerate(file.read_text(encoding="utf8").splitlines(), start=1):
            for command in command_pattern.findall(line):
                if command not in scripts:
                    failures.append(f"{relative}:{number}: npm run {command}")
    if failures:
        raise ValueError(
            "Unknown documented npm scripts:\n" + "\n".join(failures)
            + "\nAvailable scripts: " + ", ".join(sorted(scripts))
        )
    print(f"Documented npm commands verified: {files} tracked Markdown files", flush=True)


def inspect_message_limits(page=None):
    """Keep the operator's numeric limits aligned with executable contracts."""
    page = Path(page) if page is not None else WEBSITE / "docs/guide/data-handling.md"
    limits = json.loads(subprocess.check_output([
        "node", "--import", "tsx", "--eval",
        "const {KAFKA_MESSAGE_LIMITS:m,KAFKA_FETCH_LIMITS:f}="
        "require('./src/features/kafka/contracts/types.ts');"
        "const {KAFKA_MESSAGE_OPERATION_LIMITS:e}="
        "require('./src/features/kafka/ui/message-operations.ts');"
        "const {KAFKA_QUERY_LIMITS:q,KAFKA_CONTINUATION_LIMITS:c}=require('./src/features/kafka/contracts/query-search.ts');"
        "const {KAFKA_QUERY_LIBRARY_LIMITS:l}=require('./src/features/kafka/contracts/query-library.ts');"
        "const {KAFKA_RECORD_LOCATOR_LIMITS:b}=require('./src/features/kafka/contracts/record-locator.ts');"
        "const {KAFKA_QUERY_TRANSFER_LIMITS:t}=require('./src/features/kafka/contracts/query-transfer.ts');"
        "const {KAFKA_VIEW_TRANSFER_LIMITS:v}=require('./src/features/kafka/contracts/view-transfer.ts');"
        "const {KAFKA_TOPIC_CATALOG_LIMITS:n}=require('./src/features/kafka/contracts/topic-catalog.ts');"
        "const {KAFKA_ORIGINAL_RECORD_LIMITS:o}=require('./src/features/kafka/contracts/record-bytes.ts');"
        "const {RECORD_EXPORT_LIMITS:r}=require('./src/features/kafka/contracts/record-export.ts');"
        "const {RECORD_ANALYSIS_LIMITS:a}=require('./src/features/kafka/contracts/record-analysis.ts');"
        "process.stdout.write(JSON.stringify({"
        "retained:m.retainedMessages,bytes:m.retainedBytes,record:m.messageBytes,"
        "preview:m.previewBytes,fetch:f.maxMessages,original:o.bytes,"
        "content:e.exportContentBytes,document:e.exportBytes,window:f.defaultTimeWindowMs,"
        "scan:q.scanRecords,scanBytes:q.scanBytes,scanMs:q.durationMs,"
        "continuationMs:c.lifetimeMs,continuationPasses:c.passes,"
        "rangeRecords:r.records,rangeScan:r.scanRecords,rangeScanBytes:r.scanBytes,rangeBytes:r.bytes,"
        "rangeMs:r.durationMs,rangeDownloadMs:r.downloadDurationMs,rangePasses:r.passes,rangeTtl:r.artifactLifetimeMs,rangeDownloads:r.downloads,rangeReceipt:r.receiptBytes,"
        "analysisColumns:a.columns,analysisPath:a.pathCharacters,analysisSegments:a.pathSegments,analysisGroups:a.groups,"
        "analysisKey:a.groupKeyBytes,analysisCell:a.cellBytes,analysisRows:a.previewRows,analysisPreview:a.previewBytes,analysisResult:a.resultBytes,analysisWork:a.work,analysisRecordWork:a.recordWork,"
        "viewBytes:v.documentBytes,noteTopics:n.topics,noteDescription:n.descriptionBytes,noteOwner:n.ownerCharacters,noteLabels:n.labels,noteLinks:n.links,"
        "queries:l.queries,libraryBytes:l.fileBytes,queryBytes:t.documentBytes,bookmarksPerView:b.bookmarksPerView,bookmarksPerLibrary:b.bookmarksPerLibrary}));",
    ], cwd=ROOT, text=True))
    expected = {
        "Retained message count": f"{limits['retained']:,}",
        "Retained message bytes": f"{limits['bytes'] / 1_048_576:g} MiB",
        "Full record content": f"{limits['record'] / 1_048_576:g} MiB",
        "Original record bytes": f"{limits['original'] / 1024:g} KiB",
        "Value preview": f"{limits['preview'] / 1024:g} KiB",
        "Maximum bounded fetch count": f"{limits['fetch']:,}",
        "Serialized export record content": f"{limits['content'] / 1_048_576:g} MiB",
        "Complete JSON export document": f"{limits['document'] / 1_048_576:g} MiB",
        "Default recent time window": f"{limits['window'] / 60_000:g} minutes",
        "Broker search pass": f"{limits['scan']:,} records / {limits['scanBytes'] / 1_048_576:g} MiB / {limits['scanMs'] / 1000:g} seconds",
        "Read continuation": f"Latest checkpoint / {limits['continuationMs'] / 60_000:g} minutes / {limits['continuationPasses']:,} passes",
        "Range export records": f"{limits['rangeRecords']:,}",
        "Range export scan": f"{limits['rangeScan']:,} records / {limits['rangeScanBytes'] / (1024 * 1_048_576):g} GiB",
        "Range export output": f"{limits['rangeBytes'] / 1_048_576:g} MiB",
        "Range export duration": f"{limits['rangeMs'] / 60_000:g} minutes / {limits['rangePasses']:,} passes",
        "Range export downloads": f"{limits['rangeTtl'] / 60_000:g} minutes / {limits['rangeDownloads']} simultaneous reads",
        "Range download duration": f"{limits['rangeDownloadMs'] / 60_000:g} minutes",
        "Range export receipt": f"{limits['rangeReceipt'] / 1_048_576:g} MiB",
        "Analysis selected fields": f"{limits['analysisColumns']} / {limits['analysisPath']} path characters / {limits['analysisSegments']} segments",
        "Analysis groups": f"{limits['analysisGroups']} / {limits['analysisKey'] / 1024:g} KiB per group key",
        "Analysis scalar value": f"{limits['analysisCell'] / 1024:g} KiB",
        "Analysis preview": f"{limits['analysisRows']} rows / {limits['analysisPreview'] / 1024:g} KiB",
        "Analysis result": f"{limits['analysisResult'] / 1024:g} KiB",
        "Analysis evaluation work": f"{limits['analysisWork']:,} units",
        "Analysis work per record": f"{limits['analysisRecordWork']:,} units",
        "Saved view and topic-note library": f"{limits['queries']} views / {limits['noteTopics']} annotated topics / {limits['libraryBytes'] / 1_048_576:g} MiB",
        "Portable view document": f"{limits['viewBytes'] / 1024:g} KiB",
        "Local topic notes": f"{limits['noteDescription']:,} UTF-8 description bytes / {limits['noteOwner']} owner characters / {limits['noteLabels']} labels / {limits['noteLinks']} links",
        "Saved bookmarks": f"{limits['bookmarksPerView']} per view / {limits['bookmarksPerLibrary']} per library",
        "Portable query document": f"{limits['queryBytes'] / 1024:g} KiB",
    }
    rows = {}
    for line in page.read_text(encoding="utf8").splitlines():
        cells = [cell.strip() for cell in line.split("|")]
        if len(cells) == 5 and cells[1] in expected:
            if cells[1] in rows:
                raise ValueError(f"Duplicate documented limit: {cells[1]}")
            rows[cells[1]] = cells[2]
    failures = [f"{name}: expected {value}, found {rows.get(name, 'missing')}"
                for name, value in expected.items() if rows.get(name) != value]
    if failures:
        raise ValueError("Documented message limits differ from runtime:\n" + "\n".join(failures))
    print(f"Documented message limits verified: {len(expected)} runtime bounds", flush=True)


class Links(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.ids = set()
        self.links = []
        self.assets = []
        self.feed(html)

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        for name in ("href", "src", "poster"):
            if attrs.get(name):
                self.links.append(attrs[name])
        for name in ("data-sk-light", "data-sk-dark"):
            if attrs.get(name):
                self.assets.append(attrs[name])


def inspect_site(root, base_path="/"):
    root = Path(root).resolve()
    pages = {}
    for file in root.rglob("*"):
        relative = file.relative_to(root).as_posix()
        if file.is_symlink():
            raise ValueError(f"Publication symlink: {relative}")
        if file.is_dir():
            continue
        special = relative in {
            ".nojekyll", "objects.inv", "search.json", "search/search_index.json", "documentation.json", "plugin-publications.json",
            "assets/launch-score.mp3", "launch/assets/launch-score.mp3",
            "assets/streamskope-intro-light.mp4", "assets/streamskope-intro-dark.mp4",
            "assets/qualification/lifecycle-2026-10-04.json",
            "assets/qualification/topic-monitor-2026-10-04.json",
            "assets/qualification/pre-release-2026-10-06.json",
            "assets/qualification/containerlab-2026-10-07.json",
        } or file.name == "LICENSE"
        if not special and file.suffix not in ALLOWED_SUFFIXES:
            raise ValueError(f"Unexpected publication file: {relative}")
        if file.suffix == ".html":
            html = file.read_text(encoding="utf8")
            if re.search(r"/(?:Users|home)/[^/<\s]+/|(?:\.codex|openspec|ownership/records)/", html):
                raise ValueError(f"Private path in public HTML: {relative}")
            pages[file] = Links(html)
    if root / "index.html" not in pages:
        raise ValueError("Missing site index.html")
    for source, document in pages.items():
        for link in document.links + [base_path.rstrip("/") + "/assets/" + name for name in document.assets]:
            url = urlsplit(link)
            if url.scheme or url.netloc:
                continue
            path = unquote(url.path)
            if path.startswith("/"):
                prefix = base_path.rstrip("/") + "/"
                if not path.startswith(prefix):
                    raise ValueError(f"Broken local link outside site base: {link}")
                target = root / path[len(prefix):]
            else:
                target = source.parent / path if path else source
            target = target.resolve()
            if not target.is_relative_to(root):
                raise ValueError(f"Broken local link outside artifact: {link}")
            if target.is_dir():
                target /= "index.html"
            if not target.exists():
                raise ValueError(f"Broken local link in {source.name}: {link}")
            fragment = unquote(url.fragment)
            if fragment and target in pages and fragment not in pages[target].ids:
                raise ValueError(f"Broken local link anchor in {source.name}: {link}")
    print(f"Documentation artifact verified: {len(pages)} HTML pages", flush=True)


def run(command):
    subprocess.run([str(arg) for arg in command], cwd=ROOT, check=True)


def setup():
    venv = ROOT / ".cache/zensical"
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    requirements = WEBSITE / "requirements.txt"
    digest = hashlib.sha256(requirements.read_bytes()).hexdigest()
    marker = venv / ".requirements-sha256"
    if not python.exists():
        run([sys.executable, "-m", "venv", venv])
    if not marker.exists() or marker.read_text() != digest:
        run([python, "-m", "pip", "install", "--disable-pip-version-check", "-r", requirements])
        run([python, "-m", "pip", "check"])
        marker.write_text(digest)
    return python


def prepare(url, serving=False):
    publication.inspect_release_content(ROOT, published=os.environ.get("STREAMSKOPE_DOCS_PUBLISH") == "1")
    source = (WEBSITE / "zensical.toml").read_text()
    source = configure_release_navigation(
        source, ROOT, published=os.environ.get("STREAMSKOPE_DOCS_PUBLISH") == "1")
    source = re.sub(r'^site_url = .*$', f"site_url = {json.dumps(url)}", source, flags=re.M)
    if serving:
        source = source.replace('site_dir = ".site"', 'site_dir = ".preview"')
    source += "\n[project.extra.documentation]\n" + "".join(
        f"{key} = {json.dumps(value)}\n" for key, value in documentation_context().items()
    )
    source += f'revision = {json.dumps(publication_identity()["revision"])}\n'
    downloads = desktop_downloads(ROOT)
    source += "\n[project.extra.desktop_downloads]\n" + "".join(
        f"{key} = {json.dumps(value)}\n" for key, value in downloads.items() if key != "assets"
    )
    for asset in downloads["assets"]:
        source += "\n[[project.extra.desktop_downloads.assets]]\n" + "".join(
            f"{key} = {json.dumps(value)}\n" for key, value in asset.items()
        )
    browser_downloads = container_downloads(ROOT)
    source += "\n[project.extra.container_downloads]\n" + "".join(
        f"{key} = {json.dumps(value).lower() if isinstance(value, bool) else json.dumps(value)}\n"
        for key, value in browser_downloads.items() if key != "assets"
    )
    for asset in browser_downloads["assets"]:
        source += "\n[[project.extra.container_downloads.assets]]\n" + "".join(
            f"{key} = {json.dumps(value)}\n" for key, value in asset.items()
        )
    # Only these public manifest fields enter the rendered compatibility reference.
    for fields in plugin_context(ROOT, published=os.environ.get("STREAMSKOPE_DOCS_PUBLISH") == "1"):
        source += "\n[[project.extra.source_plugins]]\n" + "".join(
            f"{key} = {json.dumps(value)}\n" for key, value in fields.items())
    configuration = WEBSITE / (".zensical.serve.toml" if serving else ".zensical.local.toml")
    configuration.write_text(source)
    # Logo has one source owner; copied output is ignored, never hand-maintained.
    shutil.copyfile(
        ROOT / "src/platform/ui/assets/streamskope.svg",
        WEBSITE / "docs/assets/streamskope.svg",
    )
    media = WEBSITE / "docs/launch/assets"
    media.mkdir(parents=True, exist_ok=True)
    (media / "destination.js").write_text(
        "window.streamSkopeLaunchDestination = " + json.dumps(url + "start/quickstart/") + ";\n"
    )
    # Export only public design values from their application-owned contracts.
    tokens = json.loads(subprocess.check_output([
        "node", "--input-type=module", "-e",
        "import {streamSkopeColors as colors} from './src/platform/ui/colorContract.ts';"
        "import {streamSkopeTypography as type} from './src/platform/ui/typographyContract.ts';"
        "process.stdout.write(JSON.stringify({colors, font: type.family.interface}));",
    ], cwd=ROOT, text=True))
    theme_css = []
    for theme in ("light", "dark"):
        palette = tokens["colors"][theme]
        variables = {
            "canvas": palette["background"]["default"],
            "surface": palette["background"]["paper"],
            "navigation": palette["navigation"]["background"],
            "text": palette["text"]["primary"],
            "muted": palette["text"]["secondary"],
            "accent": palette["primary"]["main"],
            "accent-text": palette["primary"]["contrastText"],
            "border": palette["divider"],
            "font": tokens["font"],
        }
        selector = ":root" if theme == "light" else ':root[data-theme="dark"]'
        theme_css.append(selector + " {\n" + f"  color-scheme: {theme};\n" +
                         "".join(f"  --film-{key}: {value};\n" for key, value in variables.items()) + "}\n")
    (media / "theme.css").write_text("".join(theme_css))
    for name in ("streamskope.svg", "profiles.png", "topics.png", "messages.png", "message-value.png", "consumers.png", "launch-score.mp3"):
        shutil.copyfile(WEBSITE / "docs/assets" / name, media / name)
    for name in ("profiles", "topics", "messages", "message-value", "consumers"):
        shutil.copyfile(WEBSITE / "docs/assets" / f"{name}-dark.png", media / f"{name}-dark.png")
    return configuration


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["setup", "prepare", "build", "serve", "check", "qualify", "verify", "guard", "pending"])
    parser.add_argument("--component", choices=["desktop", "eda", "nsp", "all"], default="all")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8002)
    parser.add_argument("--media", action="store_true", help="Force full intro playback qualification")
    parser.add_argument("--url", help="Published site URL for verification")
    parser.add_argument("--revision")
    parser.add_argument("--release")
    args = parser.parse_args()
    if args.action == "pending":
        pending_changes(args.component)
        return
    if args.action == "guard":
        publication.guard_publication()
        return
    public_host = "127.0.0.1" if args.host == "0.0.0.0" else args.host
    url = args.url or os.environ.get("STREAMSKOPE_DOCS_URL", f"http://{public_host}:{args.port}/")
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"} or not parsed.netloc
        or parsed.query or parsed.fragment or parsed.username or parsed.password
    ):
        raise ValueError("STREAMSKOPE_DOCS_URL must be HTTP(S), without credentials, query or fragment")
    if not url.endswith("/"):
        url += "/"
    if args.action == "verify":
        verify_publication(url, args.revision, args.release)
        return
    if args.action == "prepare" and os.environ.get("STREAMSKOPE_DOCS_PUBLISH") == "1":
        publication.prepare_publication()
        capture_publications(ROOT)
    if args.action == "qualify":
        started = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        for name in ("qualification.json", "browser-checks.json"):
            (ROOT / ".artifacts/website" / name).unlink(missing_ok=True)
        publication.inspect_release_content(ROOT, published=os.environ.get("STREAMSKOPE_DOCS_PUBLISH") == "1")
        inspect_repository_commands()
        inspect_message_limits()
        inspect_retrieval_procedure(ROOT)
        python = setup()
        run([python, "-m", "unittest", "discover", "-s", "test/docs", "-p", "test_*.py"])
        run(["node", "--test", "test/docs/accessibility.test.mjs"])
        run([sys.executable, "tools/docs.py", "build"])
        changed, fingerprint = media_selection()
        media = args.media or changed
        print("Intro playback: " + ("required" if media else "unchanged; skipped"), flush=True)
        if media and os.environ.get("CI") == "true":
            run(["npx", "--no-install", "playwright", "install", "--with-deps", "firefox"])
        run(["node", "tools/docs/smoke.mjs", *(["--media"] if media else [])])
        if media:
            marker = ROOT / ".cache/docs-media-qualified"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(fingerprint)
        write_qualification(ROOT, started, media, fingerprint)
        return
    if args.action == "check":
        inspect_site(SITE, urlsplit(url).path)
        return
    python = setup()
    if args.action == "setup":
        return
    config = prepare(url, serving=args.action == "serve")
    if args.action == "prepare":
        return
    if args.action == "serve":
        run([python, "-m", "zensical", "serve", "--config-file", config, "--dev-addr", f"{args.host}:{args.port}"])
    else:
        run([python, "-m", "zensical", "build", "--clean", "--strict", "--config-file", config])
        if SITE.exists():
            shutil.rmtree(SITE)
        shutil.copytree(BUILD, SITE)
        (SITE / "documentation.json").write_text(json.dumps(publication_identity()) + "\n")
        if os.environ.get("STREAMSKOPE_DOCS_PUBLISH") == "1":
            shutil.copyfile(ROOT / SNAPSHOT, SITE / "plugin-publications.json")
        inspect_site(SITE, urlsplit(url).path)


def pending_changes(component):
    """Generate a fresh committed-source inventory; ordinary docs checks stay offline."""
    environment = dict(os.environ)
    if not environment.get("GH_TOKEN"):
        environment["GH_TOKEN"] = subprocess.check_output(
            ["gh", "auth", "token"], cwd=ROOT, text=True).strip()
    if not environment.get("GITHUB_REPOSITORY"):
        environment["GITHUB_REPOSITORY"] = subprocess.check_output(
            ["gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"],
            cwd=ROOT, env=environment, text=True).strip()
    # Fetch without --force: a rewritten local release tag must fail visibly.
    subprocess.run(["git", "fetch", "origin", "--tags"], cwd=ROOT, check=True)
    environment["GITHUB_SHA"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    directory = ROOT / ".artifacts/release-pending"
    directory.mkdir(parents=True, exist_ok=True)
    # A unique directory keeps concurrent Markdown/evidence pairs together.
    snapshot = Path(tempfile.mkdtemp(prefix=f"{component}-", dir=directory))
    output = snapshot / "pending.md"
    subprocess.run([
        "node", "--import", "tsx", "tools/package/release-changelog.ts",
        "pending", component, str(output),
    ], cwd=ROOT, env=environment, check=True)
    evidence = json.loads(Path(str(output) + ".json").read_text(encoding="utf8"))
    markdown = output.read_text(encoding="utf8")
    selected = {"desktop", "eda", "nsp"} if component == "all" else {component}
    if (evidence.get("sourceSha") != environment["GITHUB_SHA"]
            or evidence.get("repository") != environment["GITHUB_REPOSITORY"]
            or evidence.get("mode") != "pending"
            or {item["component"] for item in evidence.get("components", [])} != selected
            or evidence.get("markdownSha256") != hashlib.sha256(markdown.encode("utf8")).hexdigest()):
        raise ValueError("Pending inventory does not match this committed source")
    print(markdown, flush=True)
    print(f"Markdown and selection evidence: {snapshot.relative_to(ROOT)}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Documentation command failed: {error}", file=sys.stderr)
        sys.exit(1)
