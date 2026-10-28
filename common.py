"""Tiny HTTP+JSON helper shared by nodes and the client (standard library only)."""
import json
import urllib.error
import urllib.request


def http_json(method, host, port, path, body=None, timeout=2.0):
    """Send a request, return (status_code, parsed_json).
    Connection problems raise OSError (URLError / timeout are subclasses)."""
    url = f"http://{host}:{port}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, (json.loads(raw) if raw else None)
        except ValueError:
            return e.code, None
