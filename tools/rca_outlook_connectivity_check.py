#!/usr/bin/env python3
"""Validate LPE Outlook/RCA discovery and JMAP connectivity.

The script uses only Python's standard library. Passwords are read from
environment variables or CLI arguments so they do not need to be committed.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import textwrap
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


POX_BODY = """\
<?xml version="1.0" encoding="utf-8"?>
<Autodiscover xmlns="http://schemas.microsoft.com/exchange/autodiscover/outlook/requestschema/2006">
  <Request>
    <EMailAddress>{email}</EMailAddress>
    <AcceptableResponseSchema>http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a</AcceptableResponseSchema>
  </Request>
</Autodiscover>
"""

EWS_TIMEZONES_BODY = """\
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
            xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
  <s:Body>
    <m:GetServerTimeZones />
  </s:Body>
</s:Envelope>
"""


@dataclass
class HttpResponse:
    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


def request(
    method: str,
    url: str,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 20,
) -> HttpResponse:
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return HttpResponse(resp.status, dict(resp.headers.items()), resp.read())
    except urllib.error.HTTPError as error:
        return HttpResponse(error.code, dict(error.headers.items()), error.read())


def join_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def content_type(headers: dict[str, str]) -> str:
    for key, value in headers.items():
        if key.lower() == "content-type":
            return value.lower()
    return ""


def check_pox_autodiscover(
    base_url: str,
    email: str,
    expect_ews: bool,
    expect_exchange_providers: bool,
    expect_mapi: bool,
    timeout: int,
) -> None:
    body = POX_BODY.format(email=xml_escape(email)).encode("utf-8")
    for path in [
        "/autodiscover",
        "/autodiscover/autodiscover.xml",
        "/Autodiscover/Autodiscover.xml",
    ]:
        response = request(
            "POST",
            join_url(base_url, path),
            body,
            {
                "Content-Type": "text/xml; charset=utf-8",
                "Accept": "text/xml",
                "User-Agent": "lpe-rca-connectivity-check/0.1",
            },
            timeout,
        )
        text = response.text
        require(response.status == 200, f"{path} returned HTTP {response.status}: {text[:300]}")
        require("xml" in content_type(response.headers), f"{path} did not return XML headers")
        require("<AutoDiscoverSMTPAddress>" in text, f"{path} missing POX user block")
        require(f"<AutoDiscoverSMTPAddress>{email}</AutoDiscoverSMTPAddress>" in text, f"{path} returned wrong mailbox")
        require("<Type>IMAP</Type>" in text, f"{path} missing default IMAP protocol")
        require("<Type>MobileSync</Type>" not in text, f"{path} incorrectly advertises ActiveSync as desktop Exchange")
        if expect_ews:
            require("<Type>WEB</Type>" in text, f"{path} missing opt-in EWS WEB block")
            require("<ASUrl>" in text, f"{path} missing EWS ASUrl")
        if expect_exchange_providers:
            require(
                "      <Protocol>\n        <Type>EXCH</Type>" in text,
                f"{path} missing legacy EXCH provider section",
            )
            require(
                "      <Protocol>\n        <Type>EXPR</Type>" in text,
                f"{path} missing legacy EXPR provider section",
            )
            require("<EwsUrl>" in text, f"{path} missing EWS URL in legacy Exchange provider section")

    if expect_mapi:
        response = request(
            "POST",
            join_url(base_url, "/autodiscover"),
            body,
            {
                "Content-Type": "text/xml; charset=utf-8",
                "Accept": "text/xml",
                "User-Agent": "lpe-rca-connectivity-check/0.1",
                "X-MapiHttpCapability": "1",
            },
            timeout,
        )
        text = response.text
        require(response.status == 200, f"POX MAPI probe returned HTTP {response.status}: {text[:300]}")
        require("mapiHttp" in text, "POX MAPI probe did not publish mapiHttp")
        require("/mapi/emsmdb/" in text, "POX MAPI probe missing EMSMDB URL")
        require("/mapi/nspi/" in text, "POX MAPI probe missing NSPI URL")
    print("ok autodiscover_pox")


def check_json_autodiscover(
    base_url: str,
    email: str,
    expect_ews: bool,
    expect_mapi: bool,
    timeout: int,
) -> None:
    encoded_email = urllib.parse.quote(email, safe="")
    url = join_url(base_url, f"/autodiscover/autodiscover.json/v1.0/{encoded_email}?Protocol=AutoDiscoverV1")
    response = request("GET", url, timeout=timeout)
    require(response.status == 200, f"Autodiscover JSON v1 returned HTTP {response.status}: {response.text[:300]}")
    payload = json.loads(response.text)
    require(payload.get("Protocol") == "AutoDiscoverV1", "Autodiscover JSON did not identify AutoDiscoverV1")
    require(payload.get("Url", "").endswith("/autodiscover/autodiscover.xml"), "Autodiscover JSON returned unexpected POX URL")

    ews_url = join_url(base_url, f"/autodiscover/autodiscover.json/v1.0/{encoded_email}?Protocol=EWS")
    ews_response = request("GET", ews_url, timeout=timeout)
    if expect_ews:
        require(ews_response.status == 200, f"Autodiscover JSON EWS returned HTTP {ews_response.status}: {ews_response.text[:300]}")
        ews_payload = json.loads(ews_response.text)
        require(ews_payload.get("Protocol") == "EWS", "Autodiscover JSON EWS returned wrong protocol")
        require(ews_payload.get("Url", "").endswith("/EWS/Exchange.asmx"), "Autodiscover JSON EWS returned unexpected URL")
    else:
        require(ews_response.status in {200, 404}, f"Autodiscover JSON EWS returned unexpected HTTP {ews_response.status}")

    mapi_url = join_url(base_url, f"/autodiscover/autodiscover.json/v1.0/{encoded_email}?Protocol=MapiHttp")
    mapi_response = request("GET", mapi_url, timeout=timeout)
    if expect_mapi:
        require(mapi_response.status == 200, f"Autodiscover JSON MAPI returned HTTP {mapi_response.status}: {mapi_response.text[:300]}")
        mapi_payload = json.loads(mapi_response.text)
        require(mapi_payload.get("Protocol") == "MapiHttp", "Autodiscover JSON MAPI returned wrong protocol")
        require("/mapi/emsmdb/" in mapi_payload.get("Url", ""), "Autodiscover JSON MAPI returned unexpected URL")
    else:
        require(mapi_response.status in {200, 404}, f"Autodiscover JSON MAPI returned unexpected HTTP {mapi_response.status}")
    print("ok autodiscover_json")


def check_jmap_session(base_url: str, email: str, password: str, timeout: int) -> None:
    login_body = json.dumps({"email": email, "password": password}).encode("utf-8")
    login = request(
        "POST",
        join_url(base_url, "/api/mail/auth/login"),
        login_body,
        {"Content-Type": "application/json", "Accept": "application/json"},
        timeout,
    )
    require(login.status == 200, f"mail login returned HTTP {login.status}: {login.text[:300]}")
    token = json.loads(login.text).get("token")
    require(isinstance(token, str) and token, "mail login did not return a bearer token")

    session = request(
        "GET",
        join_url(base_url, "/api/jmap/session"),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=timeout,
    )
    require(session.status == 200, f"JMAP session returned HTTP {session.status}: {session.text[:300]}")
    require("json" in content_type(session.headers), "JMAP session did not return JSON headers")
    payload = json.loads(session.text)
    require("urn:ietf:params:jmap:core" in payload.get("capabilities", {}), "JMAP session missing core capability")
    require("accounts" in payload and payload["accounts"], "JMAP session returned no accounts")
    print("ok jmap_session")


def check_ews_basic(base_url: str, email: str, password: str, timeout: int) -> None:
    token = base64.b64encode(f"{email}:{password}".encode("utf-8")).decode("ascii")
    response = request(
        "POST",
        join_url(base_url, "/EWS/Exchange.asmx"),
        EWS_TIMEZONES_BODY.encode("utf-8"),
        {
            "Authorization": f"Basic {token}",
            "Content-Type": "text/xml; charset=utf-8",
            "Accept": "text/xml",
            "User-Agent": "lpe-rca-connectivity-check/0.1",
        },
        timeout,
    )
    require(response.status == 200, f"EWS Basic probe returned HTTP {response.status}: {response.text[:500]}")
    require("xml" in content_type(response.headers), "EWS Basic probe did not return XML headers")
    require("<m:GetServerTimeZonesResponse>" in response.text, "EWS Basic probe did not return timezone response")
    require("<m:ResponseCode>NoError</m:ResponseCode>" in response.text, "EWS Basic probe did not authenticate successfully")
    print("ok ews_basic")


def check_mapi_ping(base_url: str, email: str, password: str, timeout: int) -> None:
    token = base64.b64encode(f"{email}:{password}".encode("utf-8")).decode("ascii")
    for path in ["/mapi/emsmdb", "/mapi/nspi"]:
        response = request(
            "POST",
            join_url(base_url, path),
            b"",
            {
                "Authorization": f"Basic {token}",
                "Content-Type": "application/mapi-http",
                "X-RequestType": "PING",
                "X-RequestId": "00000000-0000-0000-0000-000000000123",
                "X-ClientInfo": "lpe-rca-connectivity-check",
            },
            timeout,
        )
        require(response.status == 200, f"MAPI PING {path} returned HTTP {response.status}: {response.text[:300]}")
        require("application/mapi-http" in content_type(response.headers), f"MAPI PING {path} did not return MAPI content")
        response_code = next((value for key, value in response.headers.items() if key.lower() == "x-responsecode"), "")
        require(response_code == "0", f"MAPI PING {path} returned X-ResponseCode {response_code!r}")
    print("ok mapi_ping")


def check_mapi_nspi_bind_octet_stream(base_url: str, email: str, password: str, timeout: int) -> None:
    token = base64.b64encode(f"{email}:{password}".encode("utf-8")).decode("ascii")
    response = request(
        "POST",
        join_url(base_url, f"/mapi/nspi/?mailboxId={urllib.parse.quote(email, safe='@')}"),
        bytes(45),
        {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/octet-stream",
            "X-RequestType": "Bind",
            "X-RequestId": "00000000-0000-0000-0000-000000000124:1",
            "X-ClientInfo": "lpe-rca-connectivity-check",
            "User-Agent": "MapiHttpClient",
        },
        timeout,
    )
    require(response.status == 200, f"MAPI NSPI Bind returned HTTP {response.status}: {response.text[:300]}")
    require("application/mapi-http" in content_type(response.headers), "MAPI NSPI Bind did not return MAPI content")
    response_code = next((value for key, value in response.headers.items() if key.lower() == "x-responsecode"), "")
    require(response_code == "0", f"MAPI NSPI Bind returned X-ResponseCode {response_code!r}: {response.text[:300]}")
    expiration = next((value for key, value in response.headers.items() if key.lower() == "x-expirationinfo"), "")
    require(expiration == "1800000", f"MAPI NSPI Bind returned X-ExpirationInfo {expiration!r}")
    client_info = next((value for key, value in response.headers.items() if key.lower() == "x-clientinfo"), "")
    require(client_info == "lpe-rca-connectivity-check", f"MAPI NSPI Bind did not echo X-ClientInfo")
    print("ok mapi_nspi_bind_octet_stream")


def xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check RCA-facing Autodiscover, JMAP, and optional EWS Basic connectivity.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            Examples:
              LPE_RCA_PASSWORD='...' tools/rca_outlook_connectivity_check.py --expect-ews --expect-exchange-providers --check-ews-basic
              tools/rca_outlook_connectivity_check.py --base-url http://127.0.0.1:8080 --email test@example.test
            """
        ),
    )
    parser.add_argument("--base-url", default=os.getenv("LPE_RCA_BASE_URL", "https://l-p-e.ch"))
    parser.add_argument("--email", default=os.getenv("LPE_RCA_EMAIL", "test@l-p-e.ch"))
    parser.add_argument("--password", default=os.getenv("LPE_RCA_PASSWORD"))
    parser.add_argument("--timeout", type=int, default=int(os.getenv("LPE_RCA_TIMEOUT", "20")))
    parser.add_argument("--expect-ews", action="store_true", help="Require EWS discovery to be published.")
    parser.add_argument(
        "--expect-exchange-providers",
        action="store_true",
        help="Require POX legacy EXCH and EXPR provider sections for RCA Outlook Connectivity.",
    )
    parser.add_argument("--expect-mapi", action="store_true", help="Require MAPI/HTTP discovery to be published.")
    parser.add_argument("--check-ews-basic", action="store_true", help="Exercise Basic auth against /EWS/Exchange.asmx.")
    parser.add_argument("--check-mapi-ping", action="store_true", help="Exercise Basic auth PING against /mapi/emsmdb and /mapi/nspi.")
    parser.add_argument(
        "--check-mapi-nspi-bind-octet-stream",
        action="store_true",
        help="Exercise RCA-style NSPI Bind with Content-Type application/octet-stream.",
    )
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    check_pox_autodiscover(
        base_url,
        args.email,
        args.expect_ews,
        args.expect_exchange_providers,
        args.expect_mapi,
        args.timeout,
    )
    check_json_autodiscover(base_url, args.email, args.expect_ews, args.expect_mapi, args.timeout)

    if args.password:
        check_jmap_session(base_url, args.email, args.password, args.timeout)
        if args.check_ews_basic:
            check_ews_basic(base_url, args.email, args.password, args.timeout)
        if args.check_mapi_ping:
            check_mapi_ping(base_url, args.email, args.password, args.timeout)
        if args.check_mapi_nspi_bind_octet_stream:
            check_mapi_nspi_bind_octet_stream(base_url, args.email, args.password, args.timeout)
    else:
        print("skip jmap_session password not provided")
        if args.check_ews_basic or args.check_mapi_ping or args.check_mapi_nspi_bind_octet_stream:
            raise RuntimeError("requested authenticated checks require --password or LPE_RCA_PASSWORD")

    return 0


if __name__ == "__main__":
    sys.exit(main())
