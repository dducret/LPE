#!/usr/bin/env python3
"""Live EWS smoke checks for an opt-in LPE Exchange compatibility endpoint.

The script uses only Python's standard library. Credentials are read from
environment variables by default so command history does not need to contain
secrets.
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
import textwrap
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
MESSAGES_NS = "http://schemas.microsoft.com/exchange/services/2006/messages"
TYPES_NS = "http://schemas.microsoft.com/exchange/services/2006/types"


@dataclass
class EwsClient:
    base_url: str
    email: str
    password: str
    timeout: int

    def call(self, operation: str, body: str) -> str:
        envelope = textwrap.dedent(
            f"""\
            <s:Envelope xmlns:s="{SOAP_NS}" xmlns:m="{MESSAGES_NS}" xmlns:t="{TYPES_NS}">
              <s:Body>
                {body}
              </s:Body>
            </s:Envelope>
            """
        ).encode("utf-8")
        token = base64.b64encode(f"{self.email}:{self.password}".encode("utf-8")).decode("ascii")
        request = urllib.request.Request(
            self.base_url,
            data=envelope,
            method="POST",
            headers={
                "Authorization": f"Basic {token}",
                "Content-Type": "text/xml; charset=utf-8",
                "Accept": "text/xml",
                "User-Agent": "lpe-ews-live-smoke/0.1",
                "X-LPE-Smoke-Operation": operation,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as error:
            payload = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{operation} HTTP {error.code}: {payload[:500]}") from error


def require_all(name: str, payload: str, needles: Iterable[str]) -> None:
    missing = [needle for needle in needles if needle not in payload]
    if missing:
        raise AssertionError(f"{name} missing expected fragments: {missing}\n{payload[:1000]}")


def check_get_server_time_zones(client: EwsClient) -> None:
    payload = client.call("GetServerTimeZones", "<m:GetServerTimeZones />")
    require_all(
        "GetServerTimeZones",
        payload,
        [
            "<m:GetServerTimeZonesResponse>",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            '<t:TimeZoneDefinition Id="UTC"',
        ],
    )


def check_find_folder(client: EwsClient) -> None:
    payload = client.call(
        "FindFolder",
        """
        <m:FindFolder Traversal="Shallow">
          <m:FolderShape><t:BaseShape>AllProperties</t:BaseShape></m:FolderShape>
          <m:ParentFolderIds><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderIds>
        </m:FindFolder>
        """,
    )
    require_all(
        "FindFolder",
        payload,
        [
            "<m:FindFolderResponse>",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<t:ContactsFolder>",
            "<t:CalendarFolder>",
            "<t:TasksFolder>",
        ],
    )


def check_get_user_oof_settings(client: EwsClient) -> None:
    payload = client.call("GetUserOofSettings", "<m:GetUserOofSettings />")
    require_all(
        "GetUserOofSettings",
        payload,
        [
            "<m:GetUserOofSettingsResponse>",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<t:OofState>",
        ],
    )


def check_resolve_names(client: EwsClient) -> None:
    payload = client.call(
        "ResolveNames",
        f"""
        <m:ResolveNames ReturnFullContactData="false">
          <m:UnresolvedEntry>{xml_escape(client.email)}</m:UnresolvedEntry>
        </m:ResolveNames>
        """,
    )
    require_all(
        "ResolveNames",
        payload,
        [
            "<m:ResolveNamesResponse>",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:ResolutionSet TotalItemsInView=\"1\"",
            f"<t:EmailAddress>{xml_escape(client.email)}</t:EmailAddress>",
        ],
    )


def check_get_user_availability(client: EwsClient) -> None:
    start = datetime.now(timezone.utc).replace(microsecond=0)
    end = start + timedelta(days=14)
    payload = client.call(
        "GetUserAvailability",
        f"""
        <m:GetUserAvailabilityRequest>
          <m:MailboxDataArray>
            <t:MailboxData>
              <t:Email><t:Address>{xml_escape(client.email)}</t:Address></t:Email>
            </t:MailboxData>
          </m:MailboxDataArray>
          <t:FreeBusyViewOptions>
            <t:TimeWindow>
              <t:StartTime>{start.isoformat().replace("+00:00", "Z")}</t:StartTime>
              <t:EndTime>{end.isoformat().replace("+00:00", "Z")}</t:EndTime>
            </t:TimeWindow>
          </t:FreeBusyViewOptions>
        </m:GetUserAvailabilityRequest>
        """,
    )
    require_all(
        "GetUserAvailability",
        payload,
        [
            "<m:GetUserAvailabilityResponse>",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<t:FreeBusyViewType>Detailed</t:FreeBusyViewType>",
        ],
    )


def check_task_mutation(client: EwsClient) -> None:
    subject = f"LPE EWS smoke {datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    created = client.call(
        "CreateItem",
        f"""
        <m:CreateItem>
          <m:SavedItemFolderId><t:DistinguishedFolderId Id="tasks"/></m:SavedItemFolderId>
          <m:Items>
            <t:Task>
              <t:Subject>{xml_escape(subject)}</t:Subject>
              <t:Body BodyType="Text">Created by LPE EWS smoke check</t:Body>
              <t:Status>InProgress</t:Status>
            </t:Task>
          </m:Items>
        </m:CreateItem>
        """,
    )
    require_all("CreateItem Task", created, ["<m:CreateItemResponse>", "task:"])
    item_id = created.split('Id="task:', 1)[1].split('"', 1)[0]
    deleted = client.call(
        "DeleteItem",
        f"""
        <m:DeleteItem DeleteType="HardDelete">
          <m:ItemIds><t:ItemId Id="task:{xml_escape(item_id)}"/></m:ItemIds>
        </m:DeleteItem>
        """,
    )
    require_all(
        "DeleteItem Task",
        deleted,
        ["<m:DeleteItemResponse>", "<m:ResponseCode>NoError</m:ResponseCode>"],
    )


def xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run live EWS smoke checks against LPE.")
    parser.add_argument("--url", default=os.getenv("LPE_EWS_URL"))
    parser.add_argument("--email", default=os.getenv("LPE_EWS_EMAIL"))
    parser.add_argument("--password", default=os.getenv("LPE_EWS_PASSWORD"))
    parser.add_argument("--timeout", type=int, default=int(os.getenv("LPE_EWS_TIMEOUT", "20")))
    parser.add_argument(
        "--mutating",
        action="store_true",
        help="Also create and delete a temporary task through EWS.",
    )
    args = parser.parse_args()

    missing = [
        name
        for name, value in {
            "LPE_EWS_URL/--url": args.url,
            "LPE_EWS_EMAIL/--email": args.email,
            "LPE_EWS_PASSWORD/--password": args.password,
        }.items()
        if not value
    ]
    if missing:
        parser.error(f"missing required settings: {', '.join(missing)}")

    client = EwsClient(args.url, args.email, args.password, args.timeout)
    checks = [
        check_get_server_time_zones,
        check_find_folder,
        check_resolve_names,
        check_get_user_oof_settings,
        check_get_user_availability,
    ]
    if args.mutating:
        checks.append(check_task_mutation)

    for check in checks:
        check(client)
        print(f"ok {check.__name__}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
