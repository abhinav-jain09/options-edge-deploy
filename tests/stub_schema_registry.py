"""An in-process stand-in for the Confluent Schema Registry's mode/config/subject/import surface, exactly as
scripts/kafka/ensure-indicator-schemas.sh and assert-indicator-schema-parity.sh drive it.  State is a plain dict so a
test can pre-seed collisions, flip mutability off, or read back what the script left behind."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Registry:
    def __init__(self, mutable: bool = True, global_mode: str = "READWRITE"):
        self.mutable = mutable
        self.global_mode = global_mode
        self.subject_modes: dict[str, str] = {}
        self.configs: dict[str, str] = {}
        self.subjects: dict[str, list[dict]] = {}   # subject -> [{"id":..,"version":..,"schema":..}]
        self.by_id: dict[int, str] = {}
        self.next_id = 100
        self.calls: list[str] = []
        self.fail_next_mode_delete = 0   # DELETE /mode/<s> answers 500 this many times before it works (failure injection)
        self.incompatible: set[str] = set()   # subjects whose NEW schema the compatibility endpoint refuses

    def register(self, subject: str, schema: str, id: int | None = None, version: int | None = None):
        if id is None:
            id = self.next_id
            self.next_id += 1
        if version is None:
            version = len(self.subjects.get(subject, [])) + 1
        self.subjects.setdefault(subject, []).append({"id": id, "version": version, "schema": schema})
        self.by_id[id] = schema
        return id, version


def _canon(schema: str) -> str:
    return json.dumps(json.loads(schema), sort_keys=True, separators=(",", ":"))


class Handler(BaseHTTPRequestHandler):
    registry: Registry

    def log_message(self, *a):
        pass

    def _send(self, status: int, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/vnd.schemaregistry.v1+json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n)) if n else {}

    def do_GET(self):
        r = self.registry
        p = self.path
        r.calls.append("GET " + p)
        if p == "/mode":
            return self._send(200, {"mode": r.global_mode})
        if p.startswith("/mode/"):
            s = p[len("/mode/"):].split("?")[0]
            if s in r.subject_modes:
                return self._send(200, {"mode": r.subject_modes[s]})
            if "defaultToGlobal=true" in p:
                return self._send(200, {"mode": r.global_mode})
            return self._send(404, {"error_code": 40409, "message": f"Subject '{s}' does not have subject-level mode configured"})
        if p.startswith("/config/"):
            s = p[len("/config/"):]
            if s in r.configs:
                return self._send(200, {"compatibilityLevel": r.configs[s]})
            return self._send(404, {"error_code": 40408, "message": "Subject does not have subject-level compatibility configured"})
        if p.startswith("/schemas/ids/") and p.split("?")[0].endswith("/subjects"):
            i = int(p.split("?")[0].split("/")[-2])
            names = [s for s, vs in r.subjects.items() for v in vs if v["id"] == i]
            return self._send(200, names) if names else self._send(404, {"error_code": 40403, "message": "Schema not found"})
        if p.startswith("/schemas/ids/") and p.split("?")[0].endswith("/versions"):
            i = int(p.split("?")[0].split("/")[-2])
            rows = [{"subject": s, "version": v["version"]} for s, vs in r.subjects.items() for v in vs if v["id"] == i]
            return self._send(200, rows) if rows else self._send(404, {"error_code": 40403, "message": "Schema not found"})
        if p.startswith("/schemas/ids/"):
            i = int(p.split("?")[0].split("/")[-1])
            if i in r.by_id:
                return self._send(200, {"schema": r.by_id[i]})
            return self._send(404, {"error_code": 40403, "message": "Schema not found"})
        if p.startswith("/subjects/") and p.endswith("/versions"):
            s = p[len("/subjects/"):-len("/versions")]
            if s in r.subjects:
                return self._send(200, [v["version"] for v in r.subjects[s]])
            return self._send(404, {"error_code": 40401, "message": "Subject not found"})
        if p.startswith("/subjects/") and "/versions/" in p:
            s, _, v = p[len("/subjects/"):].partition("/versions/")
            if v == "latest" and r.subjects.get(s):
                v = str(r.subjects[s][-1]["version"])
            for row in r.subjects.get(s, []):
                if str(row["version"]) == v:
                    return self._send(200, {"subject": s, "version": row["version"], "id": row["id"], "schema": row["schema"]})
            return self._send(404, {"error_code": 40402, "message": "Version not found"})
        return self._send(404, {"error_code": 404, "message": "no route " + p})

    def do_PUT(self):
        r = self.registry
        p = self.path
        body = self._body()
        r.calls.append(f"PUT {p} {json.dumps(body)}")
        if p.startswith("/mode/"):
            if not r.mutable:
                return self._send(422, {"error_code": 42205, "message": "Mode changes are not allowed"})
            r.subject_modes[p[len("/mode/"):]] = body["mode"]
            return self._send(200, {"mode": body["mode"]})
        if p.startswith("/config/"):
            r.configs[p[len("/config/"):]] = body["compatibility"]
            return self._send(200, {"compatibility": body["compatibility"]})
        return self._send(404, {"error_code": 404, "message": "no route " + p})

    def do_DELETE(self):
        r = self.registry
        p = self.path
        r.calls.append("DELETE " + p)
        if p.startswith("/mode/"):
            s = p[len("/mode/"):]
            if r.fail_next_mode_delete > 0:
                r.fail_next_mode_delete -= 1
                return self._send(500, {"error_code": 50001, "message": "injected failure"})
            if s in r.subject_modes:
                mode = r.subject_modes.pop(s)
                return self._send(200, {"mode": mode})
            return self._send(404, {"error_code": 40409, "message": "no subject-level mode"})
        return self._send(404, {"error_code": 404, "message": "no route " + p})

    def do_POST(self):
        r = self.registry
        p = self.path
        body = self._body()
        r.calls.append(f"POST {p} " + ("{schema…}" if "schema" in body else json.dumps(body)))
        if p.startswith("/subjects/") and p.endswith("/versions"):
            s = p[len("/subjects/"):-len("/versions")]
            if "id" in body:
                if r.subject_modes.get(s) != "IMPORT":
                    return self._send(422, {"error_code": 42205, "message": "Subject not in import mode"})
                if body["id"] in r.by_id and _canon(r.by_id[body["id"]]) != _canon(body["schema"]):
                    return self._send(409, {"error_code": 40901, "message": "Overwrite new schema with id is not permitted"})
                id, version = r.register(s, body["schema"], body["id"], body.get("version"))
            else:
                id, version = r.register(s, body["schema"])
            return self._send(200, {"id": id})
        if p.startswith("/compatibility/subjects/") and p.endswith("/versions/latest"):
            s = p[len("/compatibility/subjects/"):-len("/versions/latest")]
            if s not in r.subjects:
                return self._send(404, {"error_code": 40401, "message": "Subject not found"})
            return self._send(200, {"is_compatible": s not in r.incompatible})
        if p.startswith("/subjects/"):
            s = p[len("/subjects/"):].split("?")[0]
            for row in r.subjects.get(s, []):
                if _canon(row["schema"]) == _canon(body["schema"]):
                    return self._send(200, {"subject": s, "version": row["version"], "id": row["id"], "schema": row["schema"]})
            return self._send(404, {"error_code": 40403, "message": "Schema not found"})
        return self._send(404, {"error_code": 404, "message": "no route " + p})


def serve(registry: Registry):
    handler = type("H", (Handler,), {"registry": registry})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"
