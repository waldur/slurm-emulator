"""POST each case to a slurmrestd and print status + errors/warnings (and whether a payload came back).

usage: probe.py cases.json (--unix /path/to/socket | --url http://host:port) [--header K:V ...]
"""

import http.client
import json
import socket
import sys
from pathlib import Path


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost")
        self.unix_path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.unix_path)


def main() -> None:
    args = sys.argv[1:]
    cases = json.loads(Path(args[0]).read_text())
    headers = {"Content-Type": "application/json"}
    for i, a in enumerate(args):
        if a == "--header":
            k, v = args[i + 1].split(":", 1)
            headers[k] = v
    out = []
    for case in cases:
        if "--unix" in args:
            conn = UnixHTTPConnection(args[args.index("--unix") + 1])
        else:
            host = args[args.index("--url") + 1].removeprefix("http://")
            conn = http.client.HTTPConnection(host)
        body = case.get("raw")
        if body is None and case.get("body") is not None:
            body = json.dumps(case["body"])
        conn.request("POST", case["path"], body=body, headers=headers)
        resp = conn.getresponse()
        text = resp.read().decode()
        try:
            data = json.loads(text)
            result = {
                "status": resp.status,
                "errors": data.get("errors"),
                "warnings": data.get("warnings"),
                "payload_keys": sorted(k for k in data if k not in ("meta", "errors", "warnings")),
            }
        except ValueError:
            result = {"status": resp.status, "text": text[:300]}
        out.append({"case": case["name"], **result})
    print(json.dumps(out, indent=1))


main()
