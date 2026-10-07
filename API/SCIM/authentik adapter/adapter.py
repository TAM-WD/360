import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LIMIT = 1024 * 1024
ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"
PATCH_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
USER_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:User"


def packed(data):
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def reply(status, data):
    return status, packed(data)


def error(status, detail):
    return reply(status, {"schemas": [ERROR_SCHEMA], "status": str(status), "detail": detail})


def validate_origin(value):
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme != "https" or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.port not in (None, 443)
            or not re.fullmatch(r"[A-Za-z0-9-]+\.scim-api\.passport\.yandex\.net", parsed.hostname or "")
            or parsed.path.rstrip("/") != "/v2"):
        raise ValueError("Expected the existing HTTPS Yandex SCIM /v2 origin")
    return value.rstrip("/")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Transport:
    def __init__(self, origin):
        self.origin = validate_origin(origin)

    def request(self, method, path, token, payload=None):
        request = urllib.request.Request(
            self.origin + path, data=None if payload is None else packed(payload), method=method,
            headers={"Authorization": token, "Accept": "application/scim+json",
                     "Content-Type": "application/scim+json"})
        # Per-request opener: no cookies, shared sessions, redirects or ambient proxy.
        opener = urllib.request.build_opener(NoRedirect(), urllib.request.ProxyHandler({}))
        try:
            response = opener.open(request, timeout=20)
        except urllib.error.HTTPError as exc:
            response = exc
        except (urllib.error.URLError, TimeoutError, OSError):
            return error(502, "Upstream connection failed; inspect the adapter status, not credentials")
        with response:
            body = response.read(LIMIT + 1)
            if len(body) > LIMIT:
                return error(502, "Upstream response too large; reduce SCIM page size")
            if 300 <= response.code < 400:
                return error(502, "Upstream redirect refused")
            return response.code, body


def decode(body):
    result = json.loads(body)
    if not isinstance(result, dict):
        raise ValueError("Expected an object")
    return result


class Bridge:
    def __init__(self, transport, pause=time.sleep):
        self.transport = transport
        self.pause = pause

    def handle(self, method, target, token, body=b""):
        if method == "GET" and target == "/healthz":
            return reply(200, {"status": "ok", "adapter": "y360-lab-v1"})
        if not token.startswith("Bearer ") or not token[7:].strip() or len(token) > 8192:
            return error(401, "Bearer token required")
        parsed = urllib.parse.urlsplit(target)
        if parsed.scheme or parsed.netloc or parsed.fragment or "%" in parsed.path:
            return error(400, "Invalid path")
        route = re.fullmatch(r"/v2/(Users|Groups|ServiceProviderConfig)(?:/([A-Za-z0-9_-]+))?/?", parsed.path)
        if not route:
            return error(404, "Route not supported by this lab adapter")
        resource, identifier = route.groups()
        path = "/" + resource + ("/" + identifier if identifier else "")
        if parsed.query:
            if method != "GET":
                return error(400, "Queries are supported only for GET")
            path += "?" + parsed.query
        if method == "GET":
            if resource == "ServiceProviderConfig":
                if identifier:
                    return error(404, "Unknown resource")
                return reply(200, {
                    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
                    "patch": {"supported": False},
                    "bulk": {"supported": False, "maxOperations": 0, "maxPayloadSize": 0},
                    "filter": {"supported": False, "maxResults": 100},
                    "changePassword": {"supported": False}, "sort": {"supported": False},
                    "etag": {"supported": False}, "authenticationSchemes": []})
            return self.transport.request("GET", path, token)
        if method not in ("POST", "PUT") or resource != "Users":
            return error(405, "Lab adapter permits user POST/PUT only; deletion and group writes are disabled")
        if len(body) > LIMIT:
            return error(413, "Request too large")
        try:
            desired = decode(body)
        except (ValueError, UnicodeError):
            return error(400, "Invalid JSON object")
        if method == "POST":
            if identifier:
                return error(405, "Create at /Users only")
            if desired.get("schemas") != [USER_SCHEMA] or not desired.get("userName"):
                return error(400, "Expected a SCIM User with userName")
            return self.transport.request("POST", path, token, desired)
        if not identifier:
            return error(405, "PUT requires an existing user ID")
        return self.update(path, identifier, token, desired)

    def update(self, path, identifier, token, desired):
        allowed = {"schemas", "id", "userName", "externalId", "name", "active"}
        if set(desired) - allowed:
            return error(400, "Use the adapter mapping: PUT supports name and active only")
        if desired.get("schemas") != [USER_SCHEMA]:
            return error(400, "Unexpected schema")
        if str(desired.get("id", identifier)) != identifier:
            return error(400, "ID differs from request path")
        name = desired.get("name", {})
        if not isinstance(name, dict) or set(name) - {"givenName", "familyName"}:
            return error(400, "Only givenName and familyName are managed")
        if any(not isinstance(value, str) for value in name.values()):
            return error(400, "Name values must be strings")
        if "active" in desired and type(desired["active"]) is not bool:
            return error(400, "active must be boolean")
        status, body = self.transport.request("GET", path, token)
        if status != 200:
            return status, body
        try:
            current = decode(body)
        except (ValueError, UnicodeError):
            return error(502, "Invalid upstream User JSON")
        if str(current.get("id")) != identifier:
            return error(502, "Upstream returned another user ID")
        if desired.get("userName") != current.get("userName"):
            return error(409, "User rename is outside the lab adapter scope")
        if current.get("externalId") and desired.get("externalId") != current["externalId"]:
            return error(409, "Changing externalId is outside the lab adapter scope")
        operations = []
        for key, value in name.items():
            if (current.get("name") or {}).get(key) != value:
                operations.append({"op": "replace", "path": "name." + key, "value": value})
        if "active" in desired and current.get("active") is not desired["active"]:
            operations.append({"op": "replace", "path": "active", "value": desired["active"]})
        if not operations:
            return 200, body
        status, result = self.transport.request("PATCH", path, token,
            {"schemas": [PATCH_SCHEMA], "Operations": operations})
        print(json.dumps({"event": "translated_update", "upstream_method": "PATCH",
                          "status": status, "fields": [op["path"] for op in operations]}), flush=True)
        if status not in (200, 204):
            return status, result
        # Return a full, actually saved User, even when PATCH returns 204.
        for attempt in range(3):
            if attempt:
                self.pause(attempt)
            status, result = self.transport.request("GET", path, token)
            if status != 200:
                return status, result
            try:
                saved = decode(result)
            except (ValueError, UnicodeError):
                return error(502, "Invalid verification response")
            names_match = all((saved.get("name") or {}).get(k) == v for k, v in name.items())
            active_match = "active" not in desired or saved.get("active") is desired["active"]
            if str(saved.get("id")) == identifier and names_match and active_match:
                return 200, result
        return error(502, "PATCH succeeded but GET did not confirm the requested name/active values")


class Handler(BaseHTTPRequestHandler):
    server_version = "Y360LabAdapter/1"

    def setup(self):
        super().setup()
        self.connection.settimeout(30)

    def log_message(self, *args):
        pass

    def dispatch(self):
        try:
            if self.headers.get("Transfer-Encoding"):
                status, body = error(400, "Chunked requests are not supported")
            else:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > LIMIT:
                    status, body = error(413, "Invalid body size")
                else:
                    data = self.rfile.read(length) if length else b""
                    if len(data) != length:
                        status, body = error(400, "Incomplete request body")
                    else:
                        status, body = self.server.bridge.handle(
                            self.command, self.path, self.headers.get("Authorization", ""), data)
        except (ValueError, TimeoutError):
            status, body = error(400, "Malformed or incomplete request")
        except Exception:
            status, body = error(500, "Adapter internal error")
        self.send_response(status)
        self.send_header("Content-Type", "application/scim+json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        if self.path != "/healthz":
            print(json.dumps({"event": "request", "method": self.command,
                              "path": self.path.split("?", 1)[0], "status": status}), flush=True)

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = dispatch


def main():
    transport = Transport(os.environ["Y360_UPSTREAM"])
    server = ThreadingHTTPServer(("0.0.0.0", 8080), Handler)
    server.bridge = Bridge(transport)
    print(json.dumps({"event": "started", "version": "lab-v1", "port": 8080}), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
