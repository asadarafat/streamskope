"""Explain the first prose use on each rendered page, preserving source literals."""
from html import escape
from html.parser import HTMLParser
from pathlib import Path
import json
import re

# Product names receive an explanation, not an invented acronym expansion.
TERMS = {
    "FPS": "frames per second",
    "BPM": "beats per minute",
    "MP4": "a standardized media container for video and audio",
    "PVC": "Kubernetes persistent volume claim",
    "SOCKS": "a socket-based proxy protocol",
    "NTLM": "Microsoft challenge-response authentication",
    "MUI": "a React user interface component library",
    "MIT": "a permissive license originating at the Massachusetts Institute of Technology",
    "RSA": "Rivest–Shamir–Adleman public-key cryptography",
    "SHA256": "Secure Hash Algorithm with a 256-bit digest",
    "SIGINT": "the interrupt signal",
    "SIGTERM": "the termination signal",
    "OAUTHBEARER": "the Simple Authentication and Security Layer mechanism for OAuth bearer tokens",
    "PLAIN": "the Simple Authentication and Security Layer username-and-password mechanism",
    "AIO": "All-in-One; a broker lab for development and testing",
    "EDA": "Nokia Event-Driven Automation",
    "NSP": "Nokia Network Services Platform",
    "NATS": "a publish/subscribe messaging system",
    "API": "application programming interface",
    "CLI": "command-line interface",
    "UI": "user interface",
    "CI": "continuous integration",
    "PR": "pull request",
    "TLS": "Transport Layer Security",
    "SASL": "Simple Authentication and Security Layer",
    "SCRAM": "Salted Challenge Response Authentication Mechanism",
    "SHA-256": "Secure Hash Algorithm with a 256-bit digest",
    "CA": "certificate authority",
    "PEM": "Privacy-Enhanced Mail; a text encoding used for certificates and keys",
    "JKS": "Java KeyStore",
    "PKCS12": "Public-Key Cryptography Standards number 12; a certificate and key container format",
    "PKCS#12": "Public-Key Cryptography Standards number 12; a certificate and key container format",
    "ACL": "access control list",
    "DLQ": "dead-letter queue",
    "OIDC": "OpenID Connect",
    "SSO": "single sign-on",
    "RBAC": "role-based access control",
    "JWT": "JavaScript Object Notation Web Token",
    "JWKS": "JavaScript Object Notation Web Key Set",
    "JSON": "JavaScript Object Notation",
    "JSONL": "JavaScript Object Notation Lines; one serialized value per line",
    "CSV": "comma-separated values",
    "YAML": "YAML Ain't Markup Language; a structured text format",
    "UTF-8": "Unicode Transformation Format with 8-bit code units",
    "URL": "uniform resource locator",
    "HTTP": "Hypertext Transfer Protocol",
    "HTTPS": "Hypertext Transfer Protocol protected by Transport Layer Security",
    "REST": "Representational State Transfer",
    "SSE": "server-sent events",
    "SSH": "Secure Shell",
    "TCP": "Transmission Control Protocol",
    "IP": "Internet Protocol",
    "DNS": "Domain Name System",
    "NAT": "network address translation",
    "VPN": "virtual private network",
    "VM": "virtual machine",
    "OS": "operating system",
    "CPU": "central processing unit",
    "RAM": "random-access memory",
    "RSS": "resident set size; memory currently resident in physical memory",
    "IPC": "inter-process communication",
    "JVM": "Java Virtual Machine",
    "JDK": "Java Development Kit",
    "JMX": "Java Management Extensions",
    "ISR": "in-sync replicas",
    "OCI": "Open Container Initiative",
    "GHCR": "GitHub Container Registry",
    "FUSE": "Filesystem in Userspace",
    "POSIX": "Portable Operating System Interface",
    "WSL": "Windows Subsystem for Linux",
    "DPAPI": "Windows Data Protection API",
    "UID": "user identifier",
    "GID": "group identifier",
    "UUID": "universally unique identifier",
    "GUID": "globally unique identifier",
    "ID": "identifier",
    "UTC": "Coordinated Universal Time",
    "ISO": "International Organization for Standardization",
    "ISO 8601": "an international standard for date and time representations",
    "HTML": "Hypertext Markup Language",
    "CSS": "Cascading Style Sheets",
    "SDK": "software development kit",
    "SQL": "Structured Query Language",
    "SemVer": "Semantic Versioning",
    "ASCII": "American Standard Code for Information Interchange",
    "NDJSON": "newline-delimited JSON",
    "p95": "the 95th percentile",
    "p99": "the 99th percentile",
    "SLA": "service-level agreement",
    "SSL": "Secure Sockets Layer; the predecessor to Transport Layer Security",
    "URI": "uniform resource identifier",
    "TTL": "time to live",
    "AES": "Advanced Encryption Standard",
    "GCM": "Galois/Counter Mode",
    "WCAG": "Web Content Accessibility Guidelines",
    "GNOME": "a Linux desktop environment",
    "GSSAPI": "Generic Security Services Application Program Interface",
    "IAM": "identity and access management",
    "mTLS": "mutual Transport Layer Security",
    "AMD64": "the 64-bit x86 processor architecture",
    "ARM64": "the 64-bit Arm processor architecture",
    "DMG": "Apple disk image",
    "NSIS": "Nullsoft Scriptable Install System",
    "ACK": "acknowledgement",
    "P95": "the 95th percentile",
    "KiB": "kibibytes; 1 KiB equals 1,024 bytes",
    "MiB": "mebibytes; 1 MiB equals 1,048,576 bytes",
    "GiB": "gibibytes; 1 GiB equals 1,073,741,824 bytes",
}
PLURALS = {
    "APIs": ("API", "application programming interfaces"),
    "ACLs": ("ACL", "access control lists"),
    "CAs": ("CA", "certificate authorities"),
    "IDs": ("ID", "identifiers"),
    "UUIDs": ("UUID", "universally unique identifiers"),
    "URLs": ("URL", "uniform resource locators"),
    "VMs": ("VM", "virtual machines"),
    "CPUs": ("CPU", "central processing units"),
    "DLQs": ("DLQ", "dead-letter queues"),
    "SDKs": ("SDK", "software development kits"),
}
TERMS.update({plural: meaning for plural, (_, meaning) in PLURALS.items()})
PATTERN = re.compile(r"(?<![\w-])(" + "|".join(
    re.escape(term) for term in sorted(TERMS, key=len, reverse=True)) + r")(?![\w-])")
VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
# Preserve headings/anchors, exact UI labels, command strings and executable samples.
LITERAL = {"code", "pre", "script", "style", "button", "kbd", "strong", "h1", "h2", "h3", "h4", "h5", "h6", "svg", "noscript", "aside", "a"}


class Terminology(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.output = []
        self.stack = []
        self.seen = set()
        self.expanded = []

    def handle_starttag(self, tag, attrs):
        self.output.append(self.get_starttag_text())
        if tag not in VOID:
            attributes = dict(attrs)
            classes = attributes.get("class", "").split()
            scope = "md-content__inner" in classes or bool(self.stack and self.stack[-1][1])
            literal = (tag in LITERAL or "skope-term" in classes
                       or bool(self.stack and self.stack[-1][2]))
            if attributes.get("data-skope-term") in TERMS:
                term = attributes["data-skope-term"]
                self.seen.add(PLURALS.get(term, (term, ""))[0])
            self.stack.append((tag, scope, literal))

    def handle_startendtag(self, tag, attrs):
        self.output.append(self.get_starttag_text())

    def handle_endtag(self, tag):
        self.output.append(f"</{tag}>")
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                del self.stack[index:]
                break

    def handle_data(self, data):
        if not self.stack or not self.stack[-1][1] or self.stack[-1][2]:
            self.output.append(data)
            return
        def explain(match):
            term = match[0]
            canonical = PLURALS.get(term, (term, ""))[0]
            if canonical in self.seen or re.match(r" (?:Connector|Capture)\b", data[match.end():]):
                return term
            self.seen.add(canonical)
            meaning = TERMS[term]
            # Respect existing "Meaning (TERM)" and "TERM (meaning)" introductions.
            before, after = data[:match.start()], data[match.end():]
            short = meaning.split(";")[0]
            if (re.search(re.escape(short) + r"\s*\($", before, re.I)
                    or re.match(r"\s*\(" + re.escape(short), after, re.I)):
                return term
            self.expanded.append(term)
            return f'<span class="skope-term" data-skope-term="{escape(term)}">{term} ({escape(meaning)})</span>'
        self.output.append(PATTERN.sub(explain, data))

    def handle_entityref(self, name):
        self.output.append(f"&{name};")

    def handle_charref(self, name):
        self.output.append(f"&#{name};")

    def handle_comment(self, data):
        self.output.append(f"<!--{data}-->")

    def handle_decl(self, decl):
        self.output.append(f"<!{decl}>")


def explain_terms(site):
    receipt = {}
    for file in Path(site).rglob("*.html"):
        parser = Terminology()
        parser.feed(file.read_text())
        file.write_text("".join(parser.output))
        if parser.expanded:
            receipt[file.relative_to(site).as_posix()] = parser.expanded
    # Search must find the full names readers see, not only the source acronyms.
    index = Path(site) / "search.json"
    if index.exists():
        content = json.loads(index.read_text())
        for item in content["items"]:
            parser = Terminology()
            parser.feed('<article class="md-content__inner">' + item.get("text", "") + '</article>')
            item["text"] = "".join(parser.output)[len('<article class="md-content__inner">'):-len('</article>')]
        index.write_text(json.dumps(content, ensure_ascii=False))
    return receipt
