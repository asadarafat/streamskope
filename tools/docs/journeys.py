"""Guard the reader questions required by the project's advanced workflow guides."""
from pathlib import Path
import re
import tomllib


def inspect_skope_journeys(root):
    root = Path(root)
    navigation = tomllib.loads((root / "website/zensical.toml").read_text())["project"]["nav"]
    entries = next(entry["Under the Skope"] for entry in navigation if "Under the Skope" in entry)
    paths = [path for entry in entries for path in entry.values() if path != "skope/index.md"]
    failures = []
    for relative in paths:
        content = (root / "website/docs" / relative).read_text()
        sections = {}
        for match in re.finditer(r"^## (.+)\n([\s\S]*?)(?=^## |\Z)", content, re.M):
            sections[match[1]] = match[2].strip()
        for title in ("What this workflow does", "The problem it solves", "Before you start",
                      "How to solve it", "Check the result"):
            if not sections.get(title):
                failures.append(f"{relative}: missing written section: {title}")
        if not re.search(r"^1\. ", sections.get("How to solve it", ""), re.M):
            failures.append(f"{relative}: missing written action sequence")
    if failures:
        raise ValueError("Under the Skope reader questions are incomplete:\n" + "\n".join(failures))
    print(f"Under the Skope reader questions verified: {len(paths)} workflows")
