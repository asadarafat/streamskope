"""Build and validate public documentation using the pinned Zensical toolchain."""

import argparse
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
WEBSITE = ROOT / "website"
SITE = ROOT / "dist/site"
BUILD = WEBSITE / ".site"
ALLOWED_SUFFIXES = {
    ".html", ".css", ".js", ".svg", ".png", ".webp", ".ico",
    ".woff", ".woff2", ".ttf", ".xml", ".gz", ".map",
}


class Links(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.ids = set()
        self.links = []
        self.feed(html)

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        for name in ("href", "src", "poster"):
            if attrs.get(name):
                self.links.append(attrs[name])


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
            ".nojekyll", "objects.inv", "search.json", "search/search_index.json",
            "assets/launch-score.mp3", "launch/assets/launch-score.mp3",
            "assets/streamskope-intro-light.mp4", "assets/streamskope-intro-dark.mp4",
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
        for link in document.links:
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
    source = (WEBSITE / "zensical.toml").read_text()
    source = re.sub(r'^site_url = .*$', f"site_url = {json.dumps(url)}", source, flags=re.M)
    if serving:
        source = source.replace('site_dir = ".site"', 'site_dir = ".preview"')
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
    parser.add_argument("action", choices=["setup", "prepare", "build", "serve", "check"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8002)
    args = parser.parse_args()
    public_host = "127.0.0.1" if args.host == "0.0.0.0" else args.host
    url = os.environ.get("STREAMSKOPE_DOCS_URL", f"http://{public_host}:{args.port}/")
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"} or not parsed.netloc
        or parsed.query or parsed.fragment or parsed.username or parsed.password
    ):
        raise ValueError("STREAMSKOPE_DOCS_URL must be HTTP(S), without credentials, query or fragment")
    if not url.endswith("/"):
        url += "/"
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
        inspect_site(BUILD, urlsplit(url).path)
        if SITE.exists():
            shutil.rmtree(SITE)
        shutil.copytree(BUILD, SITE)
        inspect_site(SITE, urlsplit(url).path)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Documentation command failed: {error}", file=sys.stderr)
        sys.exit(1)
