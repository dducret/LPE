#!/usr/bin/env python3
"""Preflight checks for the ActiveSync real-client mobile lab.

This helper verifies endpoint publication before Outlook mobile or iOS Mail are
used for the manual lab. It does not replace real-client evidence.
"""

from __future__ import annotations

import argparse
import base64
import json
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request


REQUIRED_COMMANDS = {
    "FolderSync",
    "GetItemEstimate",
    "ItemOperations",
    "MoveItems",
    "Ping",
    "Provision",
    "Search",
    "SendMail",
    "SmartForward",
    "SmartReply",
    "Sync",
}


def request(method: str, url: str, headers: dict[str, str], body: bytes | None, insecure: bool):
    context = ssl._create_unverified_context() if insecure else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=context, timeout=20) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read()


def active_sync_url(base_url: str) -> str:
    return urllib.parse.urljoin(base_url.rstrip("/") + "/", "Microsoft-Server-ActiveSync")


def autodiscover_url(base_url: str, path: str) -> str:
    return urllib.parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def basic_auth(email: str, password: str | None) -> dict[str, str]:
    if not password:
        return {}
    token = base64.b64encode(f"{email}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def check(condition: bool, label: str, failures: list[str]) -> None:
    if condition:
        print(f"PASS {label}")
    else:
        print(f"FAIL {label}")
        failures.append(label)


def check_options(args, failures: list[str]) -> None:
    url = active_sync_url(args.base_url)
    query = urllib.parse.urlencode({"User": args.email})
    status, headers, _ = request(
        "OPTIONS",
        f"{url}?{query}",
        basic_auth(args.email, args.password),
        None,
        args.insecure,
    )

    expected_status = 200 if args.password else 401
    check(status == expected_status, f"OPTIONS returns HTTP {expected_status}", failures)

    versions = headers.get("MS-ASProtocolVersions", "")
    commands = {
        value.strip()
        for value in headers.get("MS-ASProtocolCommands", "").split(",")
        if value.strip()
    }
    check("16.1" in {value.strip() for value in versions.split(",")}, "OPTIONS advertises ActiveSync 16.1", failures)
    check(REQUIRED_COMMANDS <= commands, "OPTIONS advertises the implemented lab command set", failures)
    check("GetAttachment" not in commands, "OPTIONS does not advertise unsupported GetAttachment", failures)
    if not args.password:
        check(headers.get("WWW-Authenticate", "").startswith("Basic "), "anonymous OPTIONS returns Basic challenge", failures)


def check_autodiscover_json(args, protocol: str, failures: list[str]) -> None:
    quoted_email = urllib.parse.quote(args.email)
    query = urllib.parse.urlencode({"Protocol": protocol})
    url = autodiscover_url(
        args.base_url,
        f"/autodiscover/autodiscover.json/v1.0/{quoted_email}?{query}",
    )
    status, _, body = request("GET", url, {}, None, args.insecure)
    check(status == 200, f"Autodiscover v2 {protocol} returns HTTP 200", failures)
    if status != 200:
        return
    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        check(False, f"Autodiscover v2 {protocol} returns JSON", failures)
        return
    check(payload.get("Protocol") == "ActiveSync", f"Autodiscover v2 {protocol} selects ActiveSync", failures)
    check(
        str(payload.get("Url", "")).endswith("/Microsoft-Server-ActiveSync"),
        f"Autodiscover v2 {protocol} returns the ActiveSync endpoint",
        failures,
    )


def check_desktop_pox(args, failures: list[str]) -> None:
    url = autodiscover_url(args.base_url, "/autodiscover/autodiscover.xml")
    status, _, body = request("GET", url, {}, None, args.insecure)
    check(status == 200, "default Outlook POX Autodiscover returns HTTP 200", failures)
    if status == 200:
        text = body.decode("utf-8", errors="replace")
        check("<Type>MobileSync</Type>" not in text, "default Outlook POX does not publish MobileSync", failures)


def check_mobilesync_pox(args, failures: list[str]) -> None:
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<Autodiscover xmlns="http://schemas.microsoft.com/exchange/autodiscover/mobilesync/requestschema/2006">
  <Request>
    <EMailAddress>{args.email}</EMailAddress>
    <AcceptableResponseSchema>http://schemas.microsoft.com/exchange/autodiscover/mobilesync/responseschema/2006</AcceptableResponseSchema>
  </Request>
</Autodiscover>
""".encode("utf-8")
    url = autodiscover_url(args.base_url, "/autodiscover/autodiscover.xml")
    status, _, response_body = request(
        "POST",
        url,
        {"Content-Type": "text/xml; charset=utf-8"},
        body,
        args.insecure,
    )
    check(status == 200, "MobileSync POX Autodiscover returns HTTP 200", failures)
    if status == 200:
        text = response_body.decode("utf-8", errors="replace")
        check("<Type>MobileSync</Type>" in text, "MobileSync POX publishes MobileSync", failures)
        check(
            "<Url>" in text and "/Microsoft-Server-ActiveSync" in text,
            "MobileSync POX returns the ActiveSync endpoint",
            failures,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ActiveSync mobile lab preflight checks against a live LPE edge."
    )
    parser.add_argument("--base-url", required=True, help="Public HTTPS base URL, for example https://mail.example.test")
    parser.add_argument("--email", required=True, help="Mailbox account used for the lab")
    parser.add_argument("--password", help="Mailbox password for authenticated OPTIONS")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS certificate verification for lab hosts")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    failures: list[str] = []
    check_options(args, failures)
    check_autodiscover_json(args, "ActiveSync", failures)
    check_autodiscover_json(args, "MobileSync", failures)
    check_desktop_pox(args, failures)
    check_mobilesync_pox(args, failures)
    if failures:
        print("\nFailed checks:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
