"""
A stand-in for digimon.net's reference pages, so the pipeline can be
integration-tested without touching the real site. Serves the two pages the
DAG reads, mirroring the real site's markup closely enough for the parsers:

  /reference_en/                              - index with the digit-image
                                                "registered Digimon" counter
  /reference_en/detail.php?directory_name=X   - a detail page per known name

Like the real site, an unknown directory_name gets a 200 with a generic page
instead of a 404. Standard library only, so it can run in a bare python image.

Run standalone (e.g. as a docker-compose service) with:

    python server.py [port]

which serves every name in names.py, importable from alongside this file.
"""

import html
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

GENERIC_PAGE = "<html><body><div class='c-topPage'>Digimon Reference Book</div></body></html>"


def render_reference_index(registered_count: int) -> str:
    # the real site renders each digit as its own image, zero-padded to 5 places
    digits = "".join(
        f"<div class='p-refCountNumList'><img src='/img/num.png' alt='{d}'></div>"
        for d in f"{registered_count:05d}"
    )
    return f"<html><body>{digits}</body></html>"


def render_detail(name: str) -> str:
    english_name = html.escape(name.replace("_", " ").title())
    return f"""<html><body>
<div class="p-ref">
  <div class="c-titleSet__main">{english_name}</div>
  <img src="../cimages/digimon/{html.escape(name)}.jpg" alt="">
  <div class="p-ref__info"><dl>
    <dt>Level</dt><dd>Rookie</dd>
    <dt>Type</dt><dd>Test</dd>
    <dt>Attribute</dt><dd>Vaccine</dd>
    <dt>Special Move</dt><dd>・Move A・Move B</dd>
  </dl></div>
  <p class="p-ref__txt">Fixture background text for {english_name}.</p>
</div>
</body></html>"""


def make_handler(names: list[str], registered_count: int | None = None) -> type[BaseHTTPRequestHandler]:
    known = set(names)
    count = len(names) if registered_count is None else registered_count

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlparse(self.path)
            if url.path.rstrip("/") == "/reference_en":
                body = render_reference_index(count)
            elif url.path == "/reference_en/detail.php":
                name = parse_qs(url.query).get("directory_name", [""])[0]
                body = render_detail(name) if name in known else GENERIC_PAGE
            else:
                self.send_error(404)
                return
            payload = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=UTF-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):
            pass

    return Handler


def make_server(names: list[str], port: int = 0, registered_count: int | None = None) -> ThreadingHTTPServer:
    # registered_count lets a test simulate the site having more/fewer
    # Digimon than names.py knows about; defaults to len(names)
    return ThreadingHTTPServer(("0.0.0.0", port), make_handler(names, registered_count))


if __name__ == "__main__":
    from names import digimon_names

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    server = make_server(digimon_names, port)
    print(f"Fake digimon.net serving {len(digimon_names)} Digimon on port {port}", flush=True)
    server.serve_forever()
