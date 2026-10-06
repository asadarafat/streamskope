"""Generate release navigation from the notes present in this documentation snapshot."""

from datetime import date
import json
from pathlib import Path
import re
import tomllib

from docs.publication import release_version, stable_version, validate_notes


ROOT = Path(__file__).resolve().parents[2]
INDEX_PATH = "releases/index.md"
PREVIEW_PATH = "releases/unreleased.md"
RECENT_RELEASES = 5
HISTORY_MARKER = "<!-- release-history -->"


def _metadata_field(fields, name):
    values = re.findall(r"^" + name + r":[ \t]*([^\r\n]*)$", fields, re.M)
    if len(values) > 1:
        raise ValueError(f"Release metadata requires one {name} value")
    if not values:
        return None
    value = values[0].strip()
    if value.startswith('"'):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid quoted release metadata: {name}") from error
        if not isinstance(value, str):
            raise ValueError(f"Release metadata requires a string: {name}")
    elif value.startswith("'"):
        if not value.endswith("'") or len(value) < 2:
            raise ValueError(f"Invalid quoted release metadata: {name}")
        value = value[1:-1].replace("''", "'")
    return value


def _intro_summary(content):
    for paragraph in re.split(r"\n\s*\n", content):
        paragraph = paragraph.strip()
        if paragraph and not re.match(r"[#|>*+-]|```|<!--", paragraph):
            return " ".join(paragraph.split())
    return "Read full release notes"


def _version_key(tag):
    """Apply SemVer precedence, then distinguish historical build identities."""
    version = release_version(tag, historical=True)
    core, separator, prerelease = version.partition("-")
    identifiers = tuple((0, int(value)) if value.isdigit() else (1, value)
                        for value in prerelease.split(".")) if separator else ()
    build = tag.partition("+build.")[2]
    return (*map(int, core.split(".")), not separator, identifiers,
            not build, int(build) if build else 0)


def release_navigation(root=ROOT, published=False):
    """Keep all archived pages addressable while selecting five published stable tags."""
    root = Path(root)
    directory = root / "website/docs/releases"
    project = tomllib.loads((root / "website/zensical.toml").read_text(encoding="utf8"))["project"]
    desktop_tag = project["extra"]["desktop_release"]
    if not isinstance(desktop_tag, str) or release_version(desktop_tag, historical=True) is None:
        raise ValueError("Release navigation requires an exact documented desktop release")
    desktop_version = _version_key(desktop_tag)[:5]
    index = root / "website/docs" / INDEX_PATH
    if not index.is_file():
        raise ValueError("Release navigation requires its overview page")
    if index.read_text(encoding="utf8").count(HISTORY_MARKER) != 1:
        raise ValueError("Release overview requires exactly one release-history marker")
    pages = []
    for page in directory.glob("*.md"):
        if page.name in {"index.md", "unreleased.md"}:
            continue
        tag = page.stem
        version = release_version(tag, historical=True)
        if version is None:
            raise ValueError(f"{page.name}: release notes filename needs an exact desktop tag")
        try:
            validate_notes(root, tag, version)
        except ValueError as error:
            raise ValueError(f"{page.name}: {error}") from error
        content = page.read_text(encoding="utf8")
        metadata = re.match(r"---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)", content)
        status = _metadata_field(metadata.group(1), "release_status")
        if status not in (None, "published", "pending"):
            raise ValueError(f"{page.name}: release_status must be one pending or published value")
        release_date = _metadata_field(metadata.group(1), "release_date") or ""
        if release_date:
            try:
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", release_date):
                    raise ValueError("Expected YYYY-MM-DD")
                date.fromisoformat(release_date)
            except ValueError as error:
                raise ValueError(f"{page.name}: release_date requires a valid YYYY-MM-DD date") from error
        summary = _metadata_field(metadata.group(1), "release_summary")
        pages.append({"tag": tag, "path": f"releases/{page.name}",
                      "version": version, "date": release_date,
                      "summary": summary or _intro_summary(content[metadata.end():]),
                      "pending": status == "pending" or _version_key(tag)[:5] > desktop_version,
                      "prerelease": "-" in version or "+build." in tag})
    pages.sort(key=lambda page: _version_key(page["tag"]), reverse=True)
    recent = [page for page in pages if not page["pending"]
              and stable_version(page["tag"]) is not None][:RECENT_RELEASES]
    pending = [page for page in pages if page["pending"]]
    if published and pending:
        raise ValueError("Published documentation cannot include pending release notes: "
                         + ", ".join(page["path"] for page in pending))
    preview_path = PREVIEW_PATH if not published and (directory / "unreleased.md").is_file() else ""
    releases = pages
    if preview_path:
        releases = [{"tag": "Unreleased", "path": preview_path, "version": "", "date": "",
                     "summary": f"Development changes after {desktop_tag}",
                     "pending": True, "prerelease": False}, *releases]
    return {
        "recent_tags": [page["tag"] for page in recent],
        "recent_paths": [page["path"] for page in recent],
        "all_tags": [page["tag"] for page in pages],
        "all_paths": [page["path"] for page in pages],
        "pending_tags": [page["tag"] for page in pending],
        "pending_paths": [page["path"] for page in pending],
        "preview_path": preview_path,
        "index_path": INDEX_PATH,
        "releases": releases,
    }


def configure_release_navigation(source, root=ROOT, published=False):
    """Replace the source placeholder with a complete branch and its public UI fields."""
    fields = release_navigation(root, published)
    entries = []
    if fields["preview_path"]:
        entries.append(("Unreleased", fields["preview_path"]))
    entries.extend(zip(fields["all_tags"], fields["all_paths"]))
    entries.append(("See all releases", fields["index_path"]))
    branch = '{ "Releases" = [\n' + ",\n".join(
        "    { " + json.dumps(title) + " = " + json.dumps(path) + " }"
        for title, path in entries) + "\n  ] }"
    source, count = re.subn(r'\{\s*"Releases"\s*=\s*"releases/index\.md"\s*\}',
                            lambda _: branch, source)
    if count != 1:
        raise ValueError("Release navigation requires one Releases overview placeholder")
    source += "\n[project.extra.release_navigation]\n" + "".join(
        f"{key} = {json.dumps(value)}\n" for key, value in fields.items() if key != "releases")
    if not fields["releases"]:
        source += "releases = []\n"
    for release in fields["releases"]:
        source += "\n[[project.extra.release_navigation.releases]]\n" + "".join(
            f"{key} = {json.dumps(value)}\n" for key, value in release.items())
    return source
