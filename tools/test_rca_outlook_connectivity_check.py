from __future__ import annotations

import struct
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rca_outlook_connectivity_check as checker
from rca_outlook.http import HttpResponse, update_cookie_header
from rca_outlook.mapi import parse_pox_mapi_http_endpoints, require_published_mapi_url


EMAIL = "outlook-gate1@example.test"
EMSMDB_URL = "https://edge.example.test/published/mapi/emsmdb/?MailboxId=outlook-gate1%40example.test&edge=gate1"
NSPI_URL = "https://edge.example.test/published/mapi/nspi/?MailboxId=outlook-gate1%40example.test&edge=gate1"
CLIENT_INFO = "{11111111-1111-4111-8111-111111111111}:1"


def pox_response(include_legacy: bool = False) -> str:
    legacy = "<Protocol><Type>EXCH</Type></Protocol>" if include_legacy else ""
    return f"""<?xml version=\"1.0\"?>
    <Autodiscover xmlns=\"http://schemas.microsoft.com/exchange/autodiscover/responseschema/2006\">
      <Response><Account>
        <AutoDiscoverSMTPAddress>{EMAIL}</AutoDiscoverSMTPAddress>
        <Protocol><Type>IMAP</Type></Protocol>
        <Protocol><Type>mapiHttp</Type><Version>1</Version>
          <MailStore><ExternalUrl>{EMSMDB_URL.replace('&', '&amp;')}</ExternalUrl></MailStore>
          <AddressBook><ExternalUrl>{NSPI_URL.replace('&', '&amp;')}</ExternalUrl></AddressBook>
        </Protocol>
        {legacy}
      </Account></Response>
    </Autodiscover>"""


def execute_response(rops: bytes, context: str, sequence: str) -> HttpResponse:
    rop_buffer = struct.pack("<H", len(rops)) + rops
    payload = struct.pack("<IIII", 0, 0, 0, len(rop_buffer)) + rop_buffer
    return HttpResponse(
        200,
        {
            "Content-Type": "application/mapi-http",
            "X-ResponseCode": "0",
            "X-RequestId": "{22222222-2222-4222-8222-222222222222}:1",
            "X-ClientInfo": CLIENT_INFO,
        },
        b"PROCESSING\r\nDONE\r\n\r\n" + payload,
        [f"MapiContext={context}; HttpOnly", f"MapiSequence={sequence}; HttpOnly"],
    )


def hierarchy_rops(rows: list[tuple[str, str]]) -> bytes:
    value = bytearray([0x15, 0x02, 0, 0, 0, 0, 0])
    value.extend(struct.pack("<H", len(rows)))
    for name, folder_class in rows:
        value.append(0)
        value.extend(name.encode("utf-16le") + b"\0\0")
        value.extend(folder_class.encode("utf-16le") + b"\0\0")
    return bytes(value)


class MapiGate1ReadinessTests(unittest.TestCase):
    def test_parse_pox_mapi_http_uses_exactly_one_publication(self) -> None:
        endpoints = parse_pox_mapi_http_endpoints(pox_response(), EMAIL)

        self.assertEqual(endpoints.emsmdb_url, EMSMDB_URL)
        self.assertEqual(endpoints.nspi_url, NSPI_URL)
        self.assertEqual(endpoints.protocol_types, ("IMAP", "mapiHttp"))
        with self.assertRaisesRegex(RuntimeError, "legacy EXCH"):
            parse_pox_mapi_http_endpoints(pox_response(include_legacy=True), EMAIL)

    def test_published_url_must_stay_on_public_edge_and_mailbox(self) -> None:
        require_published_mapi_url(EMSMDB_URL, "edge.example.test", EMAIL, "emsmdb")
        with self.assertRaisesRegex(RuntimeError, "public edge host"):
            require_published_mapi_url(EMSMDB_URL, "core.example.test", EMAIL, "emsmdb")

    def test_cookie_updates_keep_context_and_sequence(self) -> None:
        updated = update_cookie_header(
            "MapiContext=old-context; MapiSequence=old-sequence",
            HttpResponse(200, {}, b"", ["MapiSequence=new-sequence; HttpOnly"]),
        )

        self.assertEqual(updated, "MapiContext=old-context; MapiSequence=new-sequence")

    def test_gate1_uses_discovered_urls_and_carries_emsmdb_cookies(self) -> None:
        requests: list[tuple[str, str, bytes | None, dict[str, str]]] = []
        original_request = checker.request

        def fake_request(method, url, body=None, headers=None, *args, **kwargs):
            request_headers = headers or {}
            requests.append((method, url, body, request_headers))
            if url.endswith("/autodiscover/autodiscover.xml"):
                self.assertEqual(request_headers["X-MapiHttpCapability"], "1")
                self.assertEqual(request_headers["X-AnchorMailbox"], EMAIL)
                return HttpResponse(200, {"Content-Type": "text/xml"}, pox_response().encode(), [])
            if request_headers["X-RequestType"] == "Bind":
                self.assertEqual(url, NSPI_URL)
                return HttpResponse(
                    200,
                    {"Content-Type": "application/mapi-http", "X-ResponseCode": "0", "X-ClientInfo": CLIENT_INFO, "X-ExpirationInfo": "30000"},
                    b"",
                    ["MapiContext=nspi-context; HttpOnly", "MapiSequence=nspi-sequence; HttpOnly"],
                )
            if request_headers["X-RequestType"] == "Connect":
                self.assertEqual(url, EMSMDB_URL)
                return execute_response(b"", "connect-context", "connect-sequence")
            self.assertEqual(url, EMSMDB_URL)
            if len([entry for entry in requests if entry[3].get("X-RequestType") == "Execute"]) == 1:
                self.assertIn(b"\xfe\x00\x00\x01", body or b"")
                self.assertTrue((body or b"").endswith(struct.pack("<II", 4096, 0)))
                return execute_response(b"\xfe\x00\x00\x00\x00\x00", "logon-context", "logon-sequence")
            if len([entry for entry in requests if entry[3].get("X-RequestType") == "Execute"]) == 2:
                return execute_response(
                    hierarchy_rops([("Top of Information Store", "IPF.Note")]),
                    "root-context",
                    "root-sequence",
                )
            return execute_response(
                hierarchy_rops(
                    [
                        ("Inbox", "IPF.Note"),
                        ("Sent", "IPF.Note"),
                        ("Drafts", "IPF.Note"),
                        ("Deleted Items", "IPF.Note"),
                        ("Calendar", "IPF.Appointment"),
                        ("Contacts", "IPF.Contact"),
                        ("Tasks", "IPF.Task"),
                        ("Notes", "IPF.StickyNote"),
                        ("Journal", "IPF.Journal"),
                    ]
                ),
                "ipm-context",
                "ipm-sequence",
            )

        checker.request = fake_request
        try:
            checker.check_mapi_gate1_readiness(
                "https://edge.example.test",
                EMAIL,
                "not-logged",
                "edge.example.test",
                False,
                1,
            )
        finally:
            checker.request = original_request

        execute_requests = [entry for entry in requests if entry[3].get("X-RequestType") == "Execute"]
        self.assertEqual(execute_requests[0][3]["Cookie"], "MapiContext=connect-context; MapiSequence=connect-sequence")
        self.assertEqual(execute_requests[1][3]["Cookie"], "MapiContext=logon-context; MapiSequence=logon-sequence")
        self.assertEqual(execute_requests[2][3]["Cookie"], "MapiContext=root-context; MapiSequence=root-sequence")
        self.assertTrue(all("/rpc/rpcproxy.dll" not in entry[1] for entry in requests))


if __name__ == "__main__":
    unittest.main()
