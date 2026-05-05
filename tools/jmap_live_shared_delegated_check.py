#!/usr/bin/env python3
"""Live JMAP shared-mailbox and delegated-account validation.

Required environment:
  LPE_JMAP_BASE_URL
  LPE_JMAP_OWNER_EMAIL
  LPE_JMAP_OWNER_PASSWORD
  LPE_JMAP_GRANTEE_EMAIL
  LPE_JMAP_GRANTEE_PASSWORD

Optional environment:
  LPE_JMAP_SENDER_RIGHT=send_on_behalf
  LPE_JMAP_CLEANUP=true

The script uses only Python's standard library so it can run from an
operator workstation without adding repository dependencies.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


JMAP_CORE = "urn:ietf:params:jmap:core"
JMAP_MAIL = "urn:ietf:params:jmap:mail"
JMAP_SUBMISSION = "urn:ietf:params:jmap:submission"
JMAP_CONTACTS = "urn:ietf:params:jmap:contacts"
JMAP_CALENDARS = "urn:ietf:params:jmap:calendars"


@dataclass
class AccountLogin:
    email: str
    token: str
    account_id: str
    session: dict[str, Any]


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or not value.strip():
        raise SystemExit(f"missing required environment variable: {name}")
    return value.strip()


def bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def http_json(
    base_url: str,
    method: str,
    path_or_url: str,
    body: Any | None = None,
    token: str | None = None,
) -> tuple[int, Any]:
    url = path_or_url
    if path_or_url.startswith("/"):
        url = base_url.rstrip("/") + path_or_url
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    if body is not None:
        request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw) if raw else None
        except ValueError:
            parsed = raw
        return error.code, parsed


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
        raise RuntimeError(f"JMAP session for {email} returned no accounts")
    return AccountLogin(
        email=email,
        token=token,
        account_id=next(iter(accounts)),
        session=session,
    )


def jmap(account: AccountLogin, calls: list[list[Any]]) -> dict[str, Any]:
    api_url = account.session.get("apiUrl", "/api/jmap/api")
    status, body = http_json(
        account.session["_base_url"],
        "POST",
        api_url,
        {
            "using": [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION, JMAP_CONTACTS, JMAP_CALENDARS],
            "methodCalls": calls,
        },
        token=account.token,
    )
    return require_status(status, body, 200, "JMAP API request")


def recv_exact(sock: ssl.SSLSocket, size: int) -> bytes:
    chunks: list[bytes] = []
    while size:
        chunk = sock.recv(size)
        if not chunk:
            raise RuntimeError("unexpected WebSocket EOF")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def websocket_url(account: AccountLogin) -> str:
    ws_cap = account.session.get("capabilities", {}).get("urn:ietf:params:jmap:websocket", {})
    url = ws_cap.get("url") or "/api/jmap/ws"
    if url.startswith("/"):
        parsed = urllib.parse.urlparse(account.session["_base_url"])
        scheme = "wss" if parsed.scheme == "https" else "ws"
        return f"{scheme}://{parsed.netloc}{url}"
    return url


def ws_connect(account: AccountLogin) -> ssl.SSLSocket:
    parsed = urllib.parse.urlparse(websocket_url(account))
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)
    path = parsed.path or "/api/jmap/ws"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    raw = socket.create_connection((parsed.hostname, port), timeout=20)
    if parsed.scheme == "wss":
        context = ssl.create_default_context()
        sock = context.wrap_socket(raw, server_hostname=parsed.hostname)
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
    status_line = headers.split("\r\n", 1)[0]
    if " 101 " not in status_line:
        raise RuntimeError(f"WebSocket handshake failed: {status_line}")
    if "sec-websocket-protocol: jmap" not in headers.lower():
        raise RuntimeError("WebSocket handshake did not negotiate jmap subprotocol")
    return sock  # type: ignore[return-value]


def ws_send_text(sock: ssl.SSLSocket, value: dict[str, Any]) -> None:
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


def ws_send_pong(sock: ssl.SSLSocket, payload: bytes) -> None:
    if len(payload) > 125:
        return
    header = bytearray([0x8A, 0x80 | len(payload)])
    mask = os.urandom(4)
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(bytes(header) + mask + masked)


def ws_recv_frame(sock: ssl.SSLSocket) -> tuple[int, bytes]:
    first = recv_exact(sock, 2)
    opcode = first[0] & 0x0F
    masked = bool(first[1] & 0x80)
    length = first[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(sock, 8))[0]
    mask = recv_exact(sock, 4) if masked else b""
    payload = recv_exact(sock, length) if length else b""
    if masked:
        payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return opcode, payload


def ws_recv_text_json(sock: ssl.SSLSocket) -> dict[str, Any]:
    deadline = time.time() + 30
    while time.time() < deadline:
        opcode, payload = ws_recv_frame(sock)
        if opcode == 0x1:
            return json.loads(payload.decode("utf-8"))
        if opcode == 0x9:
            ws_send_pong(sock, payload)
        elif opcode == 0x8:
            raise RuntimeError("WebSocket closed before text response")
    raise RuntimeError("timed out waiting for WebSocket text response")


def enable_push_snapshot(account: AccountLogin) -> str:
    sock = ws_connect(account)
    try:
        ws_send_text(
            sock,
            {
                "@type": "WebSocketPushEnable",
                "dataTypes": ["Mailbox", "Identity", "AddressBook", "Calendar"],
            },
        )
        state_change = ws_recv_text_json(sock)
    finally:
        sock.close()
    if state_change.get("@type") != "StateChange" or not state_change.get("pushState"):
        raise RuntimeError(f"unexpected initial StateChange: {state_change}")
    return state_change["pushState"]


def replay_push(account: AccountLogin, push_state: str) -> dict[str, Any]:
    sock = ws_connect(account)
    try:
        ws_send_text(
            sock,
            {
                "@type": "WebSocketPushEnable",
                "dataTypes": ["Mailbox", "Identity", "AddressBook", "Calendar"],
                "pushState": push_state,
            },
        )
        state_change = ws_recv_text_json(sock)
    finally:
        sock.close()
    if state_change.get("@type") != "StateChange":
        raise RuntimeError(f"unexpected reconnect StateChange: {state_change}")
    return state_change


def upsert_grants(
    base_url: str,
    owner: AccountLogin,
    grantee_email: str,
    sender_right: str,
) -> None:
    requests = [
        (
            "PUT",
            "/api/mail/delegation/mailboxes",
            {"granteeEmail": grantee_email, "mayWrite": True},
            "mailbox delegation",
        ),
        (
            "PUT",
            "/api/mail/delegation/sender",
            {"granteeEmail": grantee_email, "senderRight": sender_right},
            "sender delegation",
        ),
        (
            "PUT",
            "/api/mail/shares",
            {
                "kind": "contacts",
                "granteeEmail": grantee_email,
                "mayRead": True,
                "mayWrite": True,
                "mayDelete": True,
                "mayShare": False,
            },
            "contacts share",
        ),
        (
            "PUT",
            "/api/mail/shares",
            {
                "kind": "calendar",
                "granteeEmail": grantee_email,
                "mayRead": True,
                "mayWrite": True,
                "mayDelete": True,
                "mayShare": False,
            },
            "calendar share",
        ),
    ]
    for method, path, body, label in requests:
        status, response = http_json(base_url, method, path, body, token=owner.token)
        require_status(status, response, 200, label)


def cleanup_grants(
    base_url: str,
    owner: AccountLogin,
    grantee_account_id: str,
    sender_right: str,
) -> None:
    for path in [
        f"/api/mail/delegation/sender/{sender_right}/{grantee_account_id}",
        f"/api/mail/delegation/mailboxes/{grantee_account_id}",
        f"/api/mail/shares/contacts/{grantee_account_id}",
        f"/api/mail/shares/calendar/{grantee_account_id}",
    ]:
        status, response = http_json(base_url, "DELETE", path, token=owner.token)
        if status not in {200, 404}:
            print(f"WARN cleanup failed for {path}: HTTP {status} {response}", file=sys.stderr)


def assert_grantee_jmap_visibility(owner: AccountLogin, grantee: AccountLogin) -> None:
    session = login(owner.session["_base_url"], grantee.email, env("LPE_JMAP_GRANTEE_PASSWORD")).session
    accounts = session.get("accounts", {})
    shared_account = accounts.get(owner.account_id)
    if not shared_account:
        raise RuntimeError("grantee JMAP session does not expose delegated owner mailbox account")
    if shared_account.get("isReadOnly"):
        raise RuntimeError("delegated owner mailbox account is unexpectedly read-only")
    capabilities = shared_account.get("accountCapabilities", {})
    if JMAP_MAIL not in capabilities:
        raise RuntimeError("delegated owner mailbox account lacks Mail capability")
    if JMAP_SUBMISSION not in capabilities:
        raise RuntimeError("delegated owner mailbox account lacks Submission capability")

    grantee.session = session
    mailbox_response = jmap(
        grantee,
        [
            ["Mailbox/get", {"accountId": owner.account_id}, "m1"],
            ["Identity/get", {"accountId": owner.account_id}, "i1"],
            ["AddressBook/query", {"accountId": grantee.account_id}, "abq"],
            ["Calendar/query", {"accountId": grantee.account_id}, "calq"],
        ],
    )
    responses = {call_id: (name, body) for name, body, call_id in mailbox_response["methodResponses"]}
    mailbox_body = responses["m1"][1]
    if not mailbox_body.get("list"):
        raise RuntimeError("Mailbox/get returned no delegated mailboxes")
    if not any(item.get("myRights", {}).get("mayReadItems") for item in mailbox_body["list"]):
        raise RuntimeError("delegated mailbox does not project readable myRights")
    identity_body = responses["i1"][1]
    if not identity_body.get("list"):
        raise RuntimeError("Identity/get returned no delegated sender identity")
    if not any(identity.get("email") == owner.email for identity in identity_body["list"]):
        raise RuntimeError("Identity/get did not expose delegated owner identity")
    if not responses["abq"][1].get("ids"):
        raise RuntimeError("AddressBook/query did not expose shared contacts collection")
    if not responses["calq"][1].get("ids"):
        raise RuntimeError("Calendar/query did not expose shared calendar collection")


def assert_push_replay(state_change: dict[str, Any]) -> None:
    changed_types = {
        data_type
        for account_changes in state_change.get("changed", {}).values()
        for data_type in account_changes.keys()
    }
    expected = {"Mailbox", "Identity", "AddressBook", "Calendar"}
    missing = expected - changed_types
    if missing:
        raise RuntimeError(
            "WebSocket replay did not report shared/delegated data type changes; "
            f"missing {sorted(missing)}, saw {sorted(changed_types)}"
        )


def main() -> int:
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__.strip())
        return 0

    base_url = env("LPE_JMAP_BASE_URL").rstrip("/")
    sender_right = env("LPE_JMAP_SENDER_RIGHT", "send_on_behalf")
    cleanup = bool_env("LPE_JMAP_CLEANUP", True)

    owner = login(base_url, env("LPE_JMAP_OWNER_EMAIL"), env("LPE_JMAP_OWNER_PASSWORD"))
    grantee = login(base_url, env("LPE_JMAP_GRANTEE_EMAIL"), env("LPE_JMAP_GRANTEE_PASSWORD"))

    if owner.account_id == grantee.account_id:
        raise RuntimeError("owner and grantee must be different mailbox accounts")

    push_state = enable_push_snapshot(grantee)
    try:
        upsert_grants(base_url, owner, grantee.email, sender_right)
        replay = replay_push(grantee, push_state)
        assert_push_replay(replay)
        assert_grantee_jmap_visibility(owner, grantee)
    finally:
        if cleanup:
            cleanup_grants(base_url, owner, grantee.account_id, sender_right)

    print("LIVE shared/delegated JMAP validation passed")
    print(f"owner={owner.email}")
    print(f"grantee={grantee.email}")
    print(f"senderRight={sender_right}")
    print("validated=Session, Mailbox/get, Identity/get, AddressBook/query, Calendar/query, WebSocket replay")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
