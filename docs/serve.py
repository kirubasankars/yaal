# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Serve the Yaal docs on localhost, or write the GitHub Pages site."""

import html
import os
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGES = {
    "/": ("Yaal", ROOT / "README.md"),
    "/header": ("Parameter header", ROOT / "docs" / "header.md"),
    "/optional": ("optional", ROOT / "docs" / "optional.md"),
    "/groups": ("optional groups", ROOT / "docs" / "groups.md"),
    "/sort": ("sort and dir", ROOT / "docs" / "sort.md"),
    "/examples": ("Examples", ROOT / "docs" / "examples.md"),
    "/license": ("License", ROOT / "LICENSE"),
}
NAV = [
    ("/", "Home"),
    ("/header", "Header"),
    ("/optional", "Optional"),
    ("/groups", "Groups"),
    ("/sort", "Sort"),
    ("/examples", "Examples"),
]
BASE = ""


def render_markdown(text):
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S).strip()
    blocks = []
    paragraph = []
    items = []
    code = None
    code_lines = []

    def flush_paragraph():
        if paragraph:
            blocks.append("<p>" + inline(" ".join(paragraph)) + "</p>")
            paragraph.clear()

    def flush_items():
        if items:
            blocks.append("<ul>" + "".join(f"<li>{inline(item)}</li>" for item in items) + "</ul>")
            items.clear()

    for line in text.splitlines():
        if code is not None:
            if line.startswith("```"):
                lang = f' class="language-{html.escape(code)}"' if code else ""
                body = html.escape("\n".join(code_lines))
                blocks.append(f"<pre><code{lang}>{body}</code></pre>")
                code = None
                code_lines = []
            else:
                code_lines.append(line)
            continue
        if line.startswith("```"):
            flush_paragraph()
            flush_items()
            code = line[3:].strip()
            continue
        if not line.strip():
            flush_paragraph()
            flush_items()
            continue
        heading = re.match(r"(#{1,3}) (.+)", line)
        if heading:
            flush_paragraph()
            flush_items()
            level = len(heading.group(1))
            blocks.append(f"<h{level}>{inline(heading.group(2))}</h{level}>")
            continue
        if line.startswith("- "):
            flush_paragraph()
            items.append(line[2:].strip())
            continue
        flush_items()
        paragraph.append(line.strip())
    flush_paragraph()
    flush_items()
    if code is not None:
        blocks.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
    return "\n".join(blocks)


def inline(text):
    out = []
    index = 0
    while index < len(text):
        if text[index] == "`":
            end = text.find("`", index + 1)
            if end != -1:
                out.append("<code>" + html.escape(text[index + 1 : end]) + "</code>")
                index = end + 1
                continue
        link = re.match(r"\[([^\]]+)\]\(([^)]+)\)", text[index:])
        if link:
            out.append(f'<a href="{html.escape(href(link.group(2)))}">{inline(link.group(1))}</a>')
            index += link.end()
            continue
        nxt = len(text)
        for mark in ("`", "["):
            found = text.find(mark, index + 1)
            if found != -1:
                nxt = min(nxt, found)
        out.append(html.escape(text[index:nxt]))
        index = nxt
    return "".join(out)


def site_href(path):
    prefix = BASE.rstrip("/")
    if path == "/":
        return prefix + "/"
    return prefix + path + "/"


def href(url):
    if url == "LICENSE":
        return site_href("/license")
    if url.startswith("docs/") and url.endswith(".md"):
        name = url[len("docs/") : -3]
        if name in {"header", "optional", "groups", "sort", "examples"}:
            return site_href("/" + name)
    return url


def page(title, body):
    links = "".join(
        f'<a href="{html.escape(site_href(path))}">{html.escape(label)}</a>' for path, label in NAV
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<style>
:root {{ color-scheme: light dark; }}
body {{ margin: 0; font: 18px/1.5 Georgia, serif; }}
nav {{ display: flex; gap: 1rem; padding: 1rem 2rem; border-bottom: 1px solid #ccc; }}
nav a {{ color: inherit; }}
main {{ max-width: 42rem; margin: 0 auto; padding: 2rem 1.5rem 4rem; }}
pre {{ overflow: auto; padding: 1rem; background: #111; color: #f5f5f5; border-radius: 6px; }}
code {{ font-family: ui-monospace, monospace; font-size: 0.9em; }}
p code, li code {{ padding: 0.1em 0.3em; background: #eee; color: inherit; }}
@media (prefers-color-scheme: dark) {{
  p code, li code {{ background: #222; }}
}}
</style>
</head>
<body>
<nav>{links}</nav>
<main>{body}</main>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        found = PAGES.get(path)
        if found is None:
            self.send_error(404)
            return
        title, file = found
        payload = page(title, rendered(file)).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} {fmt % args}")


def rendered(file):
    text = file.read_text()
    if file.name == "LICENSE":
        return f"<pre>{html.escape(text)}</pre>"
    return render_markdown(text)


def build(out_dir):
    global BASE
    BASE = os.environ.get("DOCS_BASE", "/yaal").rstrip("/")
    out = Path(out_dir)
    for path, (title, file) in PAGES.items():
        dest = out / "index.html" if path == "/" else out / path.strip("/") / "index.html"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(page(title, rendered(file)))
    print(f"docs written to {out}")


def main():
    port = int(os.environ.get("DOCS_PORT", "8080"))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"docs at http://127.0.0.1:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--build":
        build(sys.argv[2])
    else:
        main()
