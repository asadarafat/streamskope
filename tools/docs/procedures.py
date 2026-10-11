"""Guard operator procedures against omitted actions and renamed UI controls."""

from pathlib import Path
import re


UI = Path("src/features/kafka/ui")
RETRIEVAL_CONTROLS = {
    "Retrieve certificates and credentials": "ProfileDialog.tsx",
    "Retrieval preset": "ProfileTrustRecipeSelector.tsx",
    "Manage…": "TrustRecipeManagementButton.tsx",
    "Retrieve": "RemoteTrustAcquisitionPanel.tsx",
    "Apply to connection": "RemoteTrustAcquisitionPanel.tsx",
    "Test connection": "ProfileDialog.tsx",
    "Save profile": "ProfileDialog.tsx",
    "Update profile": "ProfileDialog.tsx",
}

RECORD_CONTROLS = {
    "Records": "OperationalPreferencesDialog.tsx",
    "Default key encoding": "RecordCodecPreferencesPanel.tsx",
    "Default value encoding": "RecordCodecPreferencesPanel.tsx",
    "Save record encodings": "RecordCodecPreferencesPanel.tsx",
    "Detect encoding": "record-presentation.ts",
}


def inspect_record_procedure(root):
    """Guard saved-encoding instructions; this is not a decoding acceptance test."""
    root = Path(root)
    relative = "website/docs/guide/structured-events.md"
    page = (root / relative).read_text(encoding="utf8")
    failures = []
    for label, component in RECORD_CONTROLS.items():
        source = (root / UI / component).read_text(encoding="utf8")
        literal = re.escape(label)
        if not re.search(rf'["\']{literal}["\']|>\s*{literal}\s*<', source):
            failures.append(f"UI control changed: {label} ({UI / component})")
        if label not in page:
            failures.append(f"missing documented control: {label}")
    section = re.search(r"^## Choose how records are interpreted\s*\n(.*?)(?=^## |\Z)",
                        page, re.M | re.S)
    steps = re.findall(r"^\d+\.\s+[^\n]+(?:\n[ \t]+[^\n]+)*",
                       section[1] if section else "", re.M)
    previous = -1
    for label, pattern in [
        ("Disconnect and open Records", r"Disconnect Kafka.*\*\*Preferences → Records\*\*"),
        ("Save record encodings", r"\*\*Save record encodings\*\*"),
        ("Reconnect and load", r"Reconnect.*load messages"),
    ]:
        position = next((i for i, step in enumerate(steps) if re.search(pattern, step, re.S)), None)
        if position is None:
            failures.append(f"missing procedure step: {label}")
        elif position <= previous:
            failures.append(f"procedure step out of order: {label}")
        else:
            previous = position
    if failures:
        raise ValueError(f"{relative}: record procedure drift:\n" + "\n".join(failures))
    print("Record procedure verified: Disconnect → Records → Save → Reconnect/read", flush=True)


def inspect_retrieval_procedure(root):
    """Keep the guide aligned with the existing HTTPS and SSH acquisition flow.

    Product behavior is covered by electron-https-trust.spec.ts and
    web-trust-acquisition-lifecycle.spec.ts; this checks documentation only.
    """
    root = Path(root)
    relative = "website/docs/guide/secret-retrieval.md"
    page = (root / relative).read_text(encoding="utf8")
    failures = []
    for label, component in RETRIEVAL_CONTROLS.items():
        source = (root / UI / component).read_text(encoding="utf8")
        literal = re.escape(label)
        if not re.search(rf'["\']{literal}["\']|>\s*{literal}\s*<', source):
            failures.append(f"UI control changed: {label} ({UI / component})")
        if f"**{label}**" not in page:
            failures.append(f"missing documented control: {label}")

    section = re.search(r"^## Use a recipe\s*\n(.*?)(?=^## |\Z)", page, re.M | re.S)
    steps = re.findall(r"^\d+\.\s+[^\n]+(?:\n[ \t]+[^\n]+)*",
                       section[1] if section else "", re.M)
    actions = [
        ("Retrieve", r"\*\*Retrieve\*\*"),
        ("Review", r"^\d+\.\s+Review\b"),
        ("Apply to connection", r"\*\*Apply to connection\*\*"),
        ("Test connection", r"\*\*Test connection\*\*"),
        ("Save profile", r"\*\*Save profile\*\*"),
    ]
    previous = -1
    for label, pattern in actions:
        position = next((i for i, step in enumerate(steps) if re.search(pattern, step)), None)
        if position is None:
            failures.append(f"missing procedure step: {label}")
        elif position <= previous:
            failures.append(f"procedure step out of order: {label}")
        else:
            previous = position
    if failures:
        raise ValueError(f"{relative}: retrieval procedure drift:\n" + "\n".join(failures))
    print("Retrieval procedure verified: Retrieve → Review → Apply → Test → Save", flush=True)
