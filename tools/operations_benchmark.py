#!/usr/bin/env python3
"""Live operations benchmarks for LPE and LPE-CT.

This tool intentionally uses only Python's standard library. It records
latency and throughput against configured live services; it does not mock
protocol endpoints or synthesize successful release evidence.

Common environment:
  LPE_OPS_BENCH_BASE_URL              Core LPE base URL, e.g. http://127.0.0.1:8080
  LPE_OPS_BENCH_EMAIL                 Mailbox login email
  LPE_OPS_BENCH_PASSWORD              Mailbox login password
  LPE_OPS_BENCH_ITERATIONS            Iterations per short operation, default 5

Optional sections:
  LPE_OPS_BENCH_COLD_START_COMMAND    Command to start/restart core LPE
  LPE_OPS_BENCH_COLD_STOP_COMMAND     Cleanup command after cold-start measurement
  LPE_OPS_BENCH_READY_URL             Readiness URL, default {base}/health/ready

  LPE_OPS_BENCH_IMAP_HOST             IMAP host
  LPE_OPS_BENCH_IMAP_PORT             IMAP port, default 1143
  LPE_OPS_BENCH_IMAP_TLS              true for TLS, default false
  LPE_OPS_BENCH_IMAP_MAILBOX          Mailbox to SELECT, default Inbox
  LPE_OPS_BENCH_IMAP_MIN_EXISTS       Optional minimum EXISTS count for realistic-size evidence

  LPE_OPS_BENCH_ACTIVESYNC_URL        ActiveSync URL, default {base}/Microsoft-Server-ActiveSync
  LPE_OPS_BENCH_ACTIVESYNC_DEVICE_ID  DeviceId, default ops-bench

  LPE_OPS_BENCH_SMTP_HOST             SMTP ingress host
  LPE_OPS_BENCH_SMTP_PORT             SMTP ingress port, default 25
  LPE_OPS_BENCH_SMTP_TLS              true for implicit TLS, default false
  LPE_OPS_BENCH_SMTP_MAIL_FROM        Envelope sender
  LPE_OPS_BENCH_SMTP_RCPT_TO          Envelope recipient

  LPE_OPS_BENCH_LPE_CT_BASE_URL       LPE-CT management API base URL
  LPE_OPS_BENCH_LPE_CT_ADMIN_EMAIL    LPE-CT management admin email
  LPE_OPS_BENCH_LPE_CT_ADMIN_PASSWORD LPE-CT management admin password
  LPE_OPS_BENCH_RETRY_TRACE_IDS       Comma-separated trace ids to POST /retry

Output:
  JSON by default. Use --markdown for a compact operator report.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import ssl
import statistics
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable


JMAP_CORE = "urn:ietf:params:jmap:core"
JMAP_MAIL = "urn:ietf:params:jmap:mail"
JMAP_SUBMISSION = "urn:ietf:params:jmap:submission"
JMAP_WEBSOCKET = "urn:ietf:params:jmap:websocket"

WBXML_TOKENS: dict[tuple[int, str], int] = {
    (0, "Sync"): 0x05,
    (0, "SyncKey"): 0x0B,
    (0, "Collection"): 0x0F,
    (0, "Class"): 0x10,
    (0, "CollectionId"): 0x12,
    (0, "GetChanges"): 0x13,
    (0, "WindowSize"): 0x15,
    (0, "Options"): 0x17,
    (0, "Collections"): 0x1C,
    (0, "DeletesAsMoves"): 0x1E,
    (7, "SyncKey"): 0x12,
    (7, "FolderSync"): 0x16,
    (13, "Ping"): 0x05,
    (13, "Folders"): 0x09,
    (13, "Folder"): 0x0A,
    (13, "Id"): 0x0B,
    (13, "Class"): 0x0C,
    (13, "HeartbeatInterval"): 0x29,
    (17, "BodyPreference"): 0x05,
    (17, "Type"): 0x06,
}


@dataclass
class Measurement:
    name: str
    status: str
    samples_ms: list[float] = field(default_factory=list)
    detail: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "samples": len(self.samples_ms),
        }
        if self.detail:
            result["detail"] = self.detail
        if self.metadata:
            result["metadata"] = self.metadata
        if self.samples_ms:
            ordered = sorted(self.samples_ms)
            result.update(
                {
                    "min_ms": round(ordered[0], 3),
                    "p50_ms": round(statistics.median(ordered), 3),
                    "p95_ms": round(percentile(ordered, 0.95), 3),
                    "max_ms": round(ordered[-1], 3),
                    "avg_ms": round(statistics.fmean(ordered), 3),
                }
            )
        return result


@dataclass
class AccountLogin:
    email: str
    token: str
    account_id: str
    session: dict[str, Any]


def env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if value is None:
        return None
    value = value.strip()
    return value or None


def require_env(name: str, fallback: str | None = None) -> str:
    value = env(name, fallback)
    if value is None:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def bool_env(name: str, default: bool) -> bool:
    value = env(name)
    if value is None:
        return default
    return value.lower() not in {"0", "false", "no", "off"}


def percentile(ordered: list[float], fraction: float) -> float:
    if not ordered:
        return 0.0
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


def timed(call: Callable[[], Any]) -> tuple[float, Any]:
    start = time.perf_counter()
    result = call()
    return (time.perf_counter() - start) * 1000.0, result


def http_json(
    base_url: str,
    method: str,
    path_or_url: str,
    body: Any | None = None,
    token: str | None = None,
    basic: tuple[str, str] | None = None,
    timeout: int = 30,
) -> tuple[int, Any]:
    url = path_or_url if path_or_url.startswith(("http://", "https://")) else base_url.rstrip("/") + path_or_url
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Accept", "application/json")
    if body is not None:
        request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    if basic:
        encoded = base64.b64encode(f"{basic[0]}:{basic[1]}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {encoded}")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw) if raw else None
        except ValueError:
            parsed = raw
        return error.code, parsed


def http_bytes(
    url: str,
    method: str,
    body: bytes | None,
    headers: dict[str, str],
    timeout: int = 30,
) -> tuple[int, bytes, dict[str, str]]:
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read(), dict(response.headers.items())
    except urllib.error.HTTPError as error:
        return error.code, error.read(), dict(error.headers.items())


def require_status(status: int, body: Any, expected: int, label: str) -> Any:
    if status != expected:
        raise RuntimeError(f"{label} failed with HTTP {status}: {body}")
    return body


def login(base_url: str, email: str, password: str) -> AccountLogin:
    status, body = http_json(
        base_url,
        "POST",
        "/api/mail/auth/login",
        {"email": email, "password": password},
    )
    body = require_status(status, body, 200, f"login for {email}")
    token = body["token"]
    status, session = http_json(base_url, "GET", "/api/jmap/session", token=token)
    session = require_status(status, session, 200, f"JMAP session for {email}")
    session["_base_url"] = base_url
    accounts = session.get("accounts") or {}
    if not accounts:
        raise RuntimeError("JMAP session returned no accounts")
    return AccountLogin(email=email, token=token, account_id=next(iter(accounts)), session=session)


def jmap(account: AccountLogin, calls: list[list[Any]]) -> dict[str, Any]:
    api_url = account.session.get("apiUrl", "/api/jmap/api")
    status, body = http_json(
        account.session["_base_url"],
        "POST",
        api_url,
        {
            "using": [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION, JMAP_WEBSOCKET],
            "methodCalls": calls,
        },
        token=account.token,
    )
    return require_status(status, body, 200, "JMAP API request")


def method_response(body: dict[str, Any], name: str) -> dict[str, Any]:
    for response_name, args, _call_id in body.get("methodResponses", []):
        if response_name == name:
            return args
    raise RuntimeError(f"missing JMAP method response {name}: {body}")


def websocket_url(account: AccountLogin) -> str:
    ws_cap = account.session.get("capabilities", {}).get(JMAP_WEBSOCKET, {})
    url = ws_cap.get("url") or "/api/jmap/ws"
    if url.startswith("/"):
        parsed = urllib.parse.urlparse(account.session["_base_url"])
        scheme = "wss" if parsed.scheme == "https" else "ws"
        return f"{scheme}://{parsed.netloc}{url}"
    return url


def recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    while size:
        chunk = sock.recv(size)
        if not chunk:
            raise RuntimeError("unexpected WebSocket EOF")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def ws_connect(account: AccountLogin) -> socket.socket:
    parsed = urllib.parse.urlparse(websocket_url(account))
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)
    path = parsed.path or "/api/jmap/ws"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    raw = socket.create_connection((parsed.hostname, port), timeout=20)
    if parsed.scheme == "wss":
        context = ssl.create_default_context()
        sock: socket.socket = context.wrap_socket(raw, server_hostname=parsed.hostname)
    else:
        sock = raw
    sock.settimeout(30)

    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {parsed.netloc}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Protocol: jmap\r\n"
        f"Authorization: Bearer {account.token}\r\n"
        "\r\n"
    )
    sock.sendall(request.encode("ascii"))
    response = b""
    while b"\r\n\r\n" not in response:
        response += sock.recv(4096)
        if len(response) > 32768:
            raise RuntimeError("oversized WebSocket handshake response")
    headers = response.split(b"\r\n\r\n", 1)[0].decode("iso-8859-1")
    if " 101 " not in headers.split("\r\n", 1)[0]:
        raise RuntimeError(f"WebSocket handshake failed: {headers.splitlines()[0]}")
    if "sec-websocket-protocol: jmap" not in headers.lower():
        raise RuntimeError("WebSocket handshake did not negotiate jmap subprotocol")
    return sock


def ws_send_text(sock: socket.socket, value: dict[str, Any]) -> None:
    payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
    header = bytearray([0x81])
    if len(payload) < 126:
        header.append(0x80 | len(payload))
    elif len(payload) <= 0xFFFF:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", len(payload)))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", len(payload)))
    mask = os.urandom(4)
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(bytes(header) + mask + masked)


def ws_send_close(sock: socket.socket) -> None:
    mask = os.urandom(4)
    sock.sendall(bytes([0x88, 0x80]) + mask)


def ws_recv_text(sock: socket.socket) -> dict[str, Any]:
    while True:
        first, second = recv_exact(sock, 2)
        opcode = first & 0x0F
        masked = second & 0x80
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", recv_exact(sock, 2))[0]
        elif length == 127:
            length = struct.unpack("!Q", recv_exact(sock, 8))[0]
        mask = recv_exact(sock, 4) if masked else b""
        payload = recv_exact(sock, length)
        if masked:
            payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        if opcode == 0x9:
            ws_send_pong(sock, payload)
            continue
        if opcode == 0x8:
            raise RuntimeError("WebSocket closed before benchmark response")
        if opcode == 0x1:
            return json.loads(payload.decode("utf-8"))


def ws_send_pong(sock: socket.socket, payload: bytes) -> None:
    if len(payload) > 125:
        return
    mask = os.urandom(4)
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(bytes([0x8A, 0x80 | len(payload)]) + mask + masked)


def wbxml_node(page: int, name: str, text: str | None = None, children: list[Any] | None = None) -> dict[str, Any]:
    return {"page": page, "name": name, "text": text, "children": children or []}


def encode_wbxml(root: dict[str, Any]) -> bytes:
    out = bytearray([0x03, 0x01, 0x6A, 0x00])
    encode_wbxml_node(root, 0, out)
    return bytes(out)


def encode_wbxml_node(node: dict[str, Any], current_page: int, out: bytearray) -> int:
    page = int(node["page"])
    if page != current_page:
        out.extend([0x00, page])
        current_page = page
    token = WBXML_TOKENS[(page, str(node["name"]))]
    text = node.get("text")
    children = node.get("children") or []
    has_content = text is not None or bool(children)
    out.append(token | (0x40 if has_content else 0x00))
    if text is not None:
        out.append(0x03)
        out.extend(str(text).encode("utf-8"))
        out.append(0x00)
    for child in children:
        current_page = encode_wbxml_node(child, current_page, out)
    if has_content:
        out.append(0x01)
    return current_page


def basic_header(email: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{email}:{password}".encode("utf-8")).decode("ascii")


def active_sync_url(base_url: str) -> str:
    return env("LPE_OPS_BENCH_ACTIVESYNC_URL", f"{base_url.rstrip('/')}/Microsoft-Server-ActiveSync") or ""


def benchmark_cold_start(base_url: str) -> Measurement:
    command = env("LPE_OPS_BENCH_COLD_START_COMMAND")
    if not command:
        return Measurement("cold_start.ready", "skipped", detail="LPE_OPS_BENCH_COLD_START_COMMAND is not set")
    ready_url = env("LPE_OPS_BENCH_READY_URL", f"{base_url.rstrip()}/health/ready") or ""
    stop_command = env("LPE_OPS_BENCH_COLD_STOP_COMMAND")
    timeout_s = int(env("LPE_OPS_BENCH_COLD_START_TIMEOUT_SECONDS", "120") or "120")
    process: subprocess.Popen[Any] | None = None
    start = time.perf_counter()
    try:
        process = subprocess.Popen(command, shell=True)
        deadline = time.time() + timeout_s
        last_error = ""
        while time.time() < deadline:
            try:
                status, body = http_json("", "GET", ready_url, timeout=5)
                if 200 <= status < 300:
                    return Measurement(
                        "cold_start.ready",
                        "ok",
                        samples_ms=[(time.perf_counter() - start) * 1000.0],
                        metadata={"ready_url": ready_url, "response": body},
                    )
                last_error = f"HTTP {status}: {body}"
            except Exception as error:  # noqa: BLE001 - operator evidence should preserve last failure
                last_error = str(error)
            time.sleep(0.5)
        return Measurement("cold_start.ready", "failed", detail=f"readiness timeout: {last_error}")
    finally:
        if stop_command:
            subprocess.run(stop_command, shell=True, check=False)
        elif process and process.poll() is None:
            process.terminate()


def benchmark_jmap(account: AccountLogin, iterations: int) -> list[Measurement]:
    measurements = [
        Measurement("mailbox.list.workspace", "ok"),
        Measurement("jmap.mailbox_query", "ok"),
        Measurement("jmap.email_query", "ok"),
        Measurement("jmap.email_query_changes", "ok"),
        Measurement("jmap.websocket_reconnect_push_enable", "ok"),
    ]
    mailbox_ids: list[str] = []
    inbox_id: str | None = None

    for _ in range(iterations):
        elapsed, workspace = timed(lambda: http_json(account.session["_base_url"], "GET", "/api/mail/workspace", token=account.token))
        status, body = workspace
        require_status(status, body, 200, "mail workspace")
        measurements[0].samples_ms.append(elapsed)

        elapsed, mailbox_query = timed(lambda: jmap(account, [["Mailbox/query", {"accountId": account.account_id, "position": 0, "limit": 512}, "m0"]]))
        mailbox_query_response = method_response(mailbox_query, "Mailbox/query")
        mailbox_ids = mailbox_query_response.get("ids") or mailbox_ids
        measurements[1].samples_ms.append(elapsed)

        elapsed, email_query = timed(lambda: jmap(account, [["Email/query", {"accountId": account.account_id, "position": 0, "limit": 256}, "e0"]]))
        email_query_response = method_response(email_query, "Email/query")
        query_state = email_query_response.get("queryState")
        measurements[2].samples_ms.append(elapsed)
        if query_state:
            elapsed, _query_changes = timed(
                lambda: jmap(
                    account,
                    [[
                        "Email/queryChanges",
                        {"accountId": account.account_id, "filter": {}, "sort": [], "sinceQueryState": query_state, "maxChanges": 256},
                        "ec0",
                    ]],
                )
            )
            measurements[3].samples_ms.append(elapsed)

    if mailbox_ids:
        body = jmap(
            account,
            [["Mailbox/get", {"accountId": account.account_id, "ids": mailbox_ids, "properties": ["id", "role", "name"]}, "mg0"]],
        )
        mailboxes = method_response(body, "Mailbox/get").get("list") or []
        inbox = next((mailbox for mailbox in mailboxes if mailbox.get("role") == "inbox"), None)
        inbox_id = (inbox or mailboxes[0]).get("id") if mailboxes else None
    for measurement in measurements[:4]:
        measurement.metadata.update({"account_id": account.account_id})
    measurements[1].metadata["mailbox_count"] = len(mailbox_ids)
    measurements[2].metadata["inbox_mailbox_id"] = inbox_id

    for _ in range(iterations):
        elapsed, _ = timed(lambda: websocket_push_enable_round_trip(account))
        measurements[4].samples_ms.append(elapsed)
    return measurements


def websocket_push_enable_round_trip(account: AccountLogin) -> dict[str, Any]:
    sock = ws_connect(account)
    try:
        ws_send_text(
            sock,
            {
                "@type": "WebSocketPushEnable",
                "dataTypes": ["Mailbox", "Email", "EmailSubmission"],
                "pushState": None,
            },
        )
        response = ws_recv_text(sock)
        if response.get("@type") not in {"WebSocketPushEnableResponse", "WebSocketResponse"}:
            raise RuntimeError(f"unexpected WebSocket response: {response}")
        return response
    finally:
        try:
            ws_send_close(sock)
        finally:
            sock.close()


def benchmark_imap(email: str, password: str, iterations: int) -> list[Measurement]:
    host = env("LPE_OPS_BENCH_IMAP_HOST")
    if not host:
        return [Measurement("imap.select_fetch_search", "skipped", detail="LPE_OPS_BENCH_IMAP_HOST is not set")]
    port = int(env("LPE_OPS_BENCH_IMAP_PORT", "1143") or "1143")
    mailbox = env("LPE_OPS_BENCH_IMAP_MAILBOX", "Inbox") or "Inbox"
    min_exists = int(env("LPE_OPS_BENCH_IMAP_MIN_EXISTS", "0") or "0")
    use_tls = bool_env("LPE_OPS_BENCH_IMAP_TLS", False)
    measurements = [
        Measurement("imap.select", "ok", metadata={"mailbox": mailbox}),
        Measurement("imap.uid_fetch_flags", "ok", metadata={"mailbox": mailbox}),
        Measurement("imap.uid_search_all", "ok", metadata={"mailbox": mailbox}),
        Measurement("imap.search_text", "ok", metadata={"mailbox": mailbox}),
    ]
    exists_count = 0

    for index in range(iterations):
        with imap_connect(host, port, use_tls) as sock:
            imap_read_until_greeting(sock)
            imap_command(sock, "A1", f'LOGIN "{email}" "{password}"')
            elapsed, select_response = timed(lambda: imap_command(sock, "A2", f'SELECT "{mailbox}"'))
            exists_count = max(exists_count, imap_exists_count(select_response))
            measurements[0].samples_ms.append(elapsed)
            elapsed, _ = timed(lambda: imap_command(sock, "A3", "UID FETCH 1:* (UID FLAGS)"))
            measurements[1].samples_ms.append(elapsed)
            elapsed, _ = timed(lambda: imap_command(sock, "A4", "UID SEARCH ALL"))
            measurements[2].samples_ms.append(elapsed)
            elapsed, _ = timed(lambda: imap_command(sock, "A5", "SEARCH TEXT Body"))
            measurements[3].samples_ms.append(elapsed)
            imap_command(sock, "A6", "LOGOUT")
    for measurement in measurements:
        measurement.metadata["exists"] = exists_count
        if min_exists and exists_count < min_exists:
            measurement.status = "failed"
            measurement.detail = f"mailbox had {exists_count} messages; required at least {min_exists}"
    return measurements


def imap_connect(host: str, port: int, use_tls: bool) -> socket.socket:
    raw = socket.create_connection((host, port), timeout=20)
    if use_tls:
        return ssl.create_default_context().wrap_socket(raw, server_hostname=host)
    return raw


def imap_read_until_greeting(sock: socket.socket) -> str:
    return imap_read_until(sock, None)


def imap_command(sock: socket.socket, tag: str, command: str) -> str:
    sock.sendall(f"{tag} {command}\r\n".encode("utf-8"))
    return imap_read_until(sock, tag)


def imap_read_until(sock: socket.socket, tag: str | None) -> str:
    sock.settimeout(60)
    data = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
        text = data.decode("utf-8", errors="replace")
        if tag is None and text.endswith("\r\n"):
            return text
        if tag and f"\r\n{tag} " in text:
            return text
        if len(data) > 32 * 1024 * 1024:
            raise RuntimeError("IMAP response exceeded 32 MiB")
    return data.decode("utf-8", errors="replace")


def imap_exists_count(response: str) -> int:
    for line in response.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0] == "*" and parts[2].upper() == "EXISTS":
            try:
                return int(parts[1])
            except ValueError:
                return 0
    return 0


def benchmark_activesync(base_url: str, account: AccountLogin, password: str, iterations: int) -> list[Measurement]:
    url = active_sync_url(base_url)
    device_id = env("LPE_OPS_BENCH_ACTIVESYNC_DEVICE_ID", "ops-bench") or "ops-bench"
    inbox_id = jmap_inbox_mailbox_id(account)
    if not inbox_id:
        return [Measurement("activesync.sync_ping", "failed", detail="could not discover inbox mailbox id from JMAP")]
    auth = basic_header(account.email, password)
    headers = {
        "Authorization": auth,
        "MS-ASProtocolVersion": "14.1",
        "Content-Type": "application/vnd.ms-sync.wbxml",
        "User-Agent": "lpe-ops-bench/0.1",
    }
    sync_body = encode_wbxml(
        wbxml_node(
            0,
            "Sync",
            children=[
                wbxml_node(
                    0,
                    "Collections",
                    children=[
                        wbxml_node(
                            0,
                            "Collection",
                            children=[
                                wbxml_node(0, "SyncKey", "0"),
                                wbxml_node(0, "CollectionId", inbox_id),
                                wbxml_node(0, "GetChanges", "1"),
                                wbxml_node(0, "WindowSize", "32"),
                                wbxml_node(0, "DeletesAsMoves", "0"),
                                wbxml_node(0, "Options", children=[wbxml_node(17, "BodyPreference", children=[wbxml_node(17, "Type", "1")])]),
                            ],
                        )
                    ],
                )
            ],
        )
    )
    ping_body = encode_wbxml(
        wbxml_node(
            13,
            "Ping",
            children=[
                wbxml_node(13, "HeartbeatInterval", env("LPE_OPS_BENCH_ACTIVESYNC_PING_HEARTBEAT_SECONDS", "1") or "1"),
                wbxml_node(
                    13,
                    "Folders",
                    children=[wbxml_node(13, "Folder", children=[wbxml_node(13, "Id", inbox_id), wbxml_node(13, "Class", "Email")])],
                ),
            ],
        )
    )
    sync = Measurement("activesync.sync", "ok", metadata={"collection_id": inbox_id})
    ping = Measurement("activesync.ping", "ok", metadata={"collection_id": inbox_id})
    for _ in range(iterations):
        elapsed, response = timed(
            lambda: http_bytes(
                f"{url}?Cmd=Sync&User={urllib.parse.quote(account.email)}&DeviceId={urllib.parse.quote(device_id)}&DeviceType=opsbench",
                "POST",
                sync_body,
                headers,
                timeout=60,
            )
        )
        status, body, _ = response
        if status != 200:
            raise RuntimeError(f"ActiveSync Sync HTTP {status}: {body[:500]!r}")
        sync.samples_ms.append(elapsed)
        elapsed, response = timed(
            lambda: http_bytes(
                f"{url}?Cmd=Ping&User={urllib.parse.quote(account.email)}&DeviceId={urllib.parse.quote(device_id)}&DeviceType=opsbench",
                "POST",
                ping_body,
                headers,
                timeout=90,
            )
        )
        status, body, _ = response
        if status != 200:
            raise RuntimeError(f"ActiveSync Ping HTTP {status}: {body[:500]!r}")
        ping.samples_ms.append(elapsed)
    return [sync, ping]


def jmap_inbox_mailbox_id(account: AccountLogin) -> str | None:
    body = jmap(account, [["Mailbox/query", {"accountId": account.account_id, "position": 0, "limit": 512}, "mq"]])
    ids = method_response(body, "Mailbox/query").get("ids") or []
    if not ids:
        return None
    body = jmap(account, [["Mailbox/get", {"accountId": account.account_id, "ids": ids, "properties": ["id", "role", "name"]}, "mg"]])
    mailboxes = method_response(body, "Mailbox/get").get("list") or []
    inbox = next((mailbox for mailbox in mailboxes if mailbox.get("role") == "inbox"), None)
    return (inbox or mailboxes[0]).get("id") if mailboxes else None


def benchmark_smtp_data(iterations: int) -> list[Measurement]:
    host = env("LPE_OPS_BENCH_SMTP_HOST")
    sender = env("LPE_OPS_BENCH_SMTP_MAIL_FROM")
    recipient = env("LPE_OPS_BENCH_SMTP_RCPT_TO")
    if not host or not sender or not recipient:
        return [Measurement("smtp.data_to_final_reply", "skipped", detail="SMTP host, sender, and recipient env vars are required")]
    port = int(env("LPE_OPS_BENCH_SMTP_PORT", "25") or "25")
    use_tls = bool_env("LPE_OPS_BENCH_SMTP_TLS", False)
    measurement = Measurement("smtp.data_to_final_reply", "ok", metadata={"host": host, "port": port, "recipient": recipient})
    for index in range(iterations):
        with smtp_connect(host, port, use_tls) as sock:
            smtp_read_reply(sock)
            smtp_command(sock, f"EHLO ops-bench-{index}.example.test")
            smtp_command(sock, f"MAIL FROM:<{sender}>")
            smtp_command(sock, f"RCPT TO:<{recipient}>")
            smtp_command(sock, "DATA")
            message = (
                f"From: {sender}\r\n"
                f"To: {recipient}\r\n"
                f"Message-ID: <ops-bench-{int(time.time() * 1000)}-{index}@example.test>\r\n"
                f"Subject: LPE operations benchmark {index}\r\n"
                "\r\n"
                "Operations benchmark delivery probe.\r\n"
                ".\r\n"
            )
            elapsed, reply = timed(lambda: smtp_send_data(sock, message))
            if not reply.startswith(("250", "451", "554")):
                raise RuntimeError(f"unexpected SMTP DATA final reply: {reply}")
            measurement.samples_ms.append(elapsed)
            measurement.metadata["last_reply"] = reply.strip()
            smtp_command(sock, "QUIT")
    return [measurement]


def smtp_connect(host: str, port: int, use_tls: bool) -> socket.socket:
    raw = socket.create_connection((host, port), timeout=20)
    if use_tls:
        return ssl.create_default_context().wrap_socket(raw, server_hostname=host)
    return raw


def smtp_read_reply(sock: socket.socket) -> str:
    sock.settimeout(90)
    data = b""
    while True:
        line = b""
        while not line.endswith(b"\n"):
            chunk = sock.recv(1)
            if not chunk:
                break
            line += chunk
        if not line:
            break
        data += line
        if len(line) >= 4 and line[3:4] != b"-":
            break
    return data.decode("utf-8", errors="replace")


def smtp_command(sock: socket.socket, command: str) -> str:
    sock.sendall(f"{command}\r\n".encode("utf-8"))
    return smtp_read_reply(sock)


def smtp_send_data(sock: socket.socket, message: str) -> str:
    sock.sendall(message.encode("utf-8"))
    return smtp_read_reply(sock)


def benchmark_outbound_retry(iterations: int) -> list[Measurement]:
    base_url = env("LPE_OPS_BENCH_LPE_CT_BASE_URL")
    trace_ids = [item.strip() for item in (env("LPE_OPS_BENCH_RETRY_TRACE_IDS", "") or "").split(",") if item.strip()]
    if not base_url or not trace_ids:
        return [Measurement("outbound.retry_throughput", "skipped", detail="LPE-CT base URL and retry trace ids are required")]
    admin_email = require_env("LPE_OPS_BENCH_LPE_CT_ADMIN_EMAIL")
    admin_password = require_env("LPE_OPS_BENCH_LPE_CT_ADMIN_PASSWORD")
    status, login_body = http_json(base_url, "POST", "/api/v1/auth/login", {"email": admin_email, "password": admin_password})
    login_body = require_status(status, login_body, 200, "LPE-CT management login")
    token = login_body["token"]
    sample_count = min(iterations, len(trace_ids))
    measurement = Measurement("outbound.retry_throughput", "ok", metadata={"trace_count": sample_count})
    start = time.perf_counter()
    for trace_id in trace_ids[:sample_count]:
        status, body = http_json(base_url, "POST", f"/api/v1/traces/{urllib.parse.quote(trace_id)}/retry", {}, token=token)
        if status not in {200, 409}:
            raise RuntimeError(f"retry {trace_id} failed with HTTP {status}: {body}")
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    measurement.samples_ms.append(elapsed_ms)
    measurement.metadata["traces_per_second"] = round(sample_count / (elapsed_ms / 1000.0), 3) if elapsed_ms else sample_count
    return [measurement]


def run_section(name: str, call: Callable[[], list[Measurement] | Measurement]) -> list[Measurement]:
    try:
        result = call()
        return [result] if isinstance(result, Measurement) else result
    except Exception as error:  # noqa: BLE001 - benchmark report needs per-section failures
        return [Measurement(name, "failed", detail=str(error))]


def markdown_report(results: list[Measurement]) -> str:
    lines = [
        "# LPE Operations Benchmark",
        "",
        "| Benchmark | Status | Samples | p50 ms | p95 ms | Detail |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for measurement in results:
        summary = measurement.summary()
        lines.append(
            "| {name} | {status} | {samples} | {p50} | {p95} | {detail} |".format(
                name=summary["name"],
                status=summary["status"],
                samples=summary["samples"],
                p50=summary.get("p50_ms", ""),
                p95=summary.get("p95_ms", ""),
                detail=(summary.get("detail") or json.dumps(summary.get("metadata", {}), sort_keys=True)).replace("|", "\\|"),
            )
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markdown", action="store_true", help="render a Markdown report instead of JSON")
    parser.add_argument(
        "--sections",
        default="cold,jmap,imap,activesync,smtp,outbound-retry",
        help="comma-separated sections to run",
    )
    args = parser.parse_args()

    base_url = env("LPE_OPS_BENCH_BASE_URL", env("LPE_JMAP_BASE_URL", "http://127.0.0.1:8080")) or "http://127.0.0.1:8080"
    email = env("LPE_OPS_BENCH_EMAIL", env("LPE_JMAP_OWNER_EMAIL"))
    password = env("LPE_OPS_BENCH_PASSWORD", env("LPE_JMAP_OWNER_PASSWORD"))
    iterations = int(env("LPE_OPS_BENCH_ITERATIONS", "5") or "5")
    sections = {section.strip() for section in args.sections.split(",") if section.strip()}
    account: AccountLogin | None = None
    results: list[Measurement] = []

    if sections & {"jmap", "activesync"}:
        if not email or not password:
            results.append(Measurement("account.login", "failed", detail="mailbox credentials are required for JMAP and ActiveSync benchmarks"))
        else:
            login_measurement = Measurement("account.login", "ok")
            elapsed, account = timed(lambda: login(base_url, email, password))
            login_measurement.samples_ms.append(elapsed)
            login_measurement.metadata["email"] = email
            results.append(login_measurement)

    if "cold" in sections:
        results.extend(run_section("cold_start.ready", lambda: benchmark_cold_start(base_url)))
    if "jmap" in sections and account:
        results.extend(run_section("jmap", lambda: benchmark_jmap(account, iterations)))
    if "imap" in sections:
        if not env("LPE_OPS_BENCH_IMAP_HOST"):
            results.extend(run_section("imap", lambda: benchmark_imap(email or "", password or "", iterations)))
        elif not email or not password:
            results.append(Measurement("imap.select_fetch_search", "failed", detail="mailbox credentials are required"))
        else:
            results.extend(run_section("imap", lambda: benchmark_imap(email, password, iterations)))
    if "activesync" in sections and account and password:
        results.extend(run_section("activesync", lambda: benchmark_activesync(base_url, account, password, iterations)))
    if "smtp" in sections:
        results.extend(run_section("smtp", lambda: benchmark_smtp_data(iterations)))
    if "outbound-retry" in sections:
        results.extend(run_section("outbound.retry_throughput", lambda: benchmark_outbound_retry(iterations)))

    if args.markdown:
        print(markdown_report(results))
    else:
        print(json.dumps({"generated_at_unix": int(time.time()), "results": [item.summary() for item in results]}, indent=2, sort_keys=True))
    return 1 if any(item.status == "failed" for item in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
