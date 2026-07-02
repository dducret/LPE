#!/usr/bin/env python3
"""Validate LPE Outlook/RCA discovery and canonical Exchange readiness.

The script uses only Python's standard library. Passwords are read from
environment variables or CLI arguments so they do not need to be committed.
"""

from __future__ import annotations

import json
import re
import sys
import time
import urllib.parse
import uuid

from rca_outlook.cli import parse_args
from rca_outlook.ews import (
    ews_call,
    ews_envelope,
    extract_ews_item_id,
    require_ews_no_error,
)
from rca_outlook.http import (
    basic_auth_header,
    content_type,
    cookie_header,
    header_value,
    join_url,
    request,
    require,
    require_guid_counter_header,
    update_cookie_header,
    url_host,
)
from rca_outlook.mapi import (
    assert_nspi_fixture_payload,
    assert_nspi_get_matches_payload,
    assert_nspi_get_props_payload,
    assert_nspi_query_rows_payload,
    assert_nspi_resolve_names_payload,
    contains_bytes,
    mapi_client_info,
    mapi_empty_deleted_items_rops,
    mapi_execute_body,
    mapi_execute_response_rops,
    mapi_http_binary_payload,
    mapi_request_id,
    mapi_rop_buffer,
    mapi_sent_content_sync_rops,
    mapi_sent_subject_table_rops,
    nspi_first_minimal_id,
    nspi_get_props_request,
    resolve_names_request,
    rpc_rts_conn_a1_body,
    rpc_rts_conn_b1_body,
    utf16z,
)

from datetime import datetime, timedelta, timezone


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


NSPI_BOOTSTRAP_PROPERTY_TAGS = [
    0x3001_001F,
    0x39FE_001F,
    0x3003_001F,
    0x3A00_001F,
    0x0FFE_0003,
    0x3000_0003,
    0x3004_001F,
    0x3002_001F,
]






























































def check_pox_autodiscover(
    base_url: str,
    email: str,
    expect_ews: bool,
    expect_exch_provider: bool,
    expect_expr_provider: bool,
    expect_mapi: bool,
    expected_service_host: str | None,
    insecure_tls: bool,
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
            insecure_tls=insecure_tls,
        )
        text = response.text
        require(response.status == 200, f"{path} returned HTTP {response.status}: {text[:300]}")
        require("xml" in content_type(response.headers), f"{path} did not return XML headers")
        require("<AutoDiscoverSMTPAddress>" in text, f"{path} missing POX user block")
        require(f"<AutoDiscoverSMTPAddress>{email}</AutoDiscoverSMTPAddress>" in text, f"{path} returned wrong mailbox")
        require("<Type>IMAP</Type>" in text, f"{path} missing default IMAP protocol")
        require("<Type>MobileSync</Type>" not in text, f"{path} incorrectly advertises ActiveSync as desktop Exchange")
        if expected_service_host:
            require(
                f"<Server>{expected_service_host}</Server>" in text,
                f"{path} does not publish expected service host {expected_service_host}",
            )
        if expect_ews:
            require("<Type>WEB</Type>" in text, f"{path} missing opt-in EWS WEB block")
            require("<ASUrl>" in text, f"{path} missing EWS ASUrl")
            require(
                not expected_service_host or f"https://{expected_service_host}/EWS/Exchange.asmx" in text,
                f"{path} does not publish EWS on expected service host {expected_service_host}",
            )
        else:
            require("<Type>WEB</Type>" not in text, f"{path} unexpectedly published EWS WEB block")
        if expect_exch_provider:
            require(
                "      <Protocol>\n        <Type>EXCH</Type>" in text,
                f"{path} missing legacy EXCH provider section",
            )
        else:
            require(
                "      <Protocol>\n        <Type>EXCH</Type>" not in text,
                f"{path} unexpectedly published legacy EXCH provider section",
            )
        if expect_expr_provider:
            require(
                "      <Protocol>\n        <Type>EXPR</Type>" in text,
                f"{path} missing legacy EXPR provider section required for Outlook Anywhere/RPC over HTTP",
            )
            require("<CertPrincipalName>" in text, f"{path} missing EXPR certificate principal")
        else:
            require(
                "      <Protocol>\n        <Type>EXPR</Type>" not in text,
                f"{path} unexpectedly published legacy EXPR provider section",
            )
        if expect_exch_provider or expect_expr_provider:
            if expect_ews:
                require("<EwsUrl>" in text, f"{path} missing EWS URL in legacy Exchange provider section")

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
        insecure_tls=insecure_tls,
    )
    text = response.text
    require(response.status == 200, f"POX MAPI probe returned HTTP {response.status}: {text[:300]}")
    if expect_mapi:
        require("mapiHttp" in text, "POX MAPI probe did not publish mapiHttp")
        require("/mapi/emsmdb/" in text, "POX MAPI probe missing EMSMDB URL")
        require("/mapi/nspi/" in text, "POX MAPI probe missing NSPI URL")
        if expect_ews:
            require("<Type>WEB</Type>" in text, "POX MAPI probe suppressed opt-in EWS WEB block")
            require("<ASUrl>" in text, "POX MAPI probe missing EWS ASUrl")
            require(
                not expected_service_host or f"https://{expected_service_host}/EWS/Exchange.asmx" in text,
                f"POX MAPI probe does not publish EWS on expected service host {expected_service_host}",
            )
    else:
        require("mapiHttp" not in text, "POX MAPI probe unexpectedly published mapiHttp")
    print("ok autodiscover_pox")


def check_json_autodiscover(
    base_url: str,
    email: str,
    expect_ews: bool,
    expect_mapi: bool,
    expected_service_host: str | None,
    insecure_tls: bool,
    timeout: int,
) -> None:
    encoded_email = urllib.parse.quote(email, safe="")
    url = join_url(base_url, f"/autodiscover/autodiscover.json/v1.0/{encoded_email}?Protocol=AutoDiscoverV1")
    response = request("GET", url, timeout=timeout, insecure_tls=insecure_tls)
    require(response.status == 200, f"Autodiscover JSON v1 returned HTTP {response.status}: {response.text[:300]}")
    payload = json.loads(response.text)
    require(payload.get("Protocol") == "AutoDiscoverV1", "Autodiscover JSON did not identify AutoDiscoverV1")
    require(payload.get("Url", "").endswith("/autodiscover/autodiscover.xml"), "Autodiscover JSON returned unexpected POX URL")

    ews_url = join_url(base_url, f"/autodiscover/autodiscover.json/v1.0/{encoded_email}?Protocol=EWS")
    ews_response = request("GET", ews_url, timeout=timeout, insecure_tls=insecure_tls)
    if expect_ews:
        require(ews_response.status == 200, f"Autodiscover JSON EWS returned HTTP {ews_response.status}: {ews_response.text[:300]}")
        ews_payload = json.loads(ews_response.text)
        require(ews_payload.get("Protocol") == "EWS", "Autodiscover JSON EWS returned wrong protocol")
        require(ews_payload.get("Url", "").endswith("/EWS/Exchange.asmx"), "Autodiscover JSON EWS returned unexpected URL")
        if expected_service_host:
            require(
                url_host(ews_payload.get("Url", "")) == expected_service_host,
                f"Autodiscover JSON EWS did not return expected service host {expected_service_host}",
            )
    else:
        require(ews_response.status in {200, 404}, f"Autodiscover JSON EWS returned unexpected HTTP {ews_response.status}")

    mapi_url = join_url(base_url, f"/autodiscover/autodiscover.json/v1.0/{encoded_email}?Protocol=MapiHttp")
    mapi_response = request("GET", mapi_url, timeout=timeout, insecure_tls=insecure_tls)
    if expect_mapi:
        require(mapi_response.status == 200, f"Autodiscover JSON MAPI returned HTTP {mapi_response.status}: {mapi_response.text[:300]}")
        mapi_payload = json.loads(mapi_response.text)
        require(mapi_payload.get("Protocol") == "MapiHttp", "Autodiscover JSON MAPI returned wrong protocol")
        require("/mapi/emsmdb/" in mapi_payload.get("Url", ""), "Autodiscover JSON MAPI returned unexpected URL")
        if expected_service_host:
            require(
                url_host(mapi_payload.get("Url", "")) == expected_service_host,
                f"Autodiscover JSON MAPI did not return expected service host {expected_service_host}",
            )
    else:
        require(mapi_response.status in {200, 404}, f"Autodiscover JSON MAPI returned unexpected HTTP {mapi_response.status}")
    print("ok autodiscover_json")


def check_jmap_publication_headers(
    base_url: str,
    expected_service_host: str | None,
    insecure_tls: bool,
    timeout: int,
) -> None:
    well_known = request(
        "GET",
        join_url(base_url, "/.well-known/jmap"),
        timeout=timeout,
        insecure_tls=insecure_tls,
        follow_redirects=False,
    )
    require(
        well_known.status in {301, 302, 307, 308},
        f"/.well-known/jmap returned HTTP {well_known.status}; expected redirect to JMAP session",
    )
    location = header_value(well_known.headers, "location")
    require(location.endswith("/api/jmap/session"), f"/.well-known/jmap returned unexpected Location {location!r}")
    if expected_service_host:
        require(
            url_host(location) == expected_service_host,
            f"/.well-known/jmap did not redirect to expected service host {expected_service_host}",
        )

    anonymous = request(
        "GET",
        join_url(base_url, "/api/jmap/session"),
        headers={"Accept": "application/json"},
        timeout=timeout,
        insecure_tls=insecure_tls,
    )
    require(
        anonymous.status == 401,
        f"anonymous JMAP session returned HTTP {anonymous.status}; expected authentication challenge",
    )
    print("ok jmap_publication_headers")


def check_jmap_session(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> dict:
    login_body = json.dumps({"email": email, "password": password}).encode("utf-8")
    login = request(
        "POST",
        join_url(base_url, "/api/mail/auth/login"),
        login_body,
        {"Content-Type": "application/json", "Accept": "application/json"},
        timeout,
        insecure_tls=insecure_tls,
    )
    require(login.status == 200, f"mail login returned HTTP {login.status}: {login.text[:300]}")
    token = json.loads(login.text).get("token")
    require(isinstance(token, str) and token, "mail login did not return a bearer token")

    session = request(
        "GET",
        join_url(base_url, "/api/jmap/session"),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=timeout,
        insecure_tls=insecure_tls,
    )
    require(session.status == 200, f"JMAP session returned HTTP {session.status}: {session.text[:300]}")
    require("json" in content_type(session.headers), "JMAP session did not return JSON headers")
    payload = json.loads(session.text)
    require("urn:ietf:params:jmap:core" in payload.get("capabilities", {}), "JMAP session missing core capability")
    require("accounts" in payload and payload["accounts"], "JMAP session returned no accounts")
    print("ok jmap_session")
    return {"token": token, "session": payload}


def check_jmap_email_subject_absent(
    base_url: str,
    email: str,
    password: str,
    subject: str,
    insecure_tls: bool,
    timeout: int,
) -> None:
    login = check_jmap_session(base_url, email, password, insecure_tls, timeout)
    token = login["token"]
    session = login["session"]
    account_id = session.get("primaryAccounts", {}).get("urn:ietf:params:jmap:mail")
    if not account_id:
        accounts = session.get("accounts", {})
        account_id = next(iter(accounts.keys()), None)
    require(account_id, "JMAP session did not expose a mail account id")
    api_url = session.get("apiUrl") or "/api/jmap/api"
    body = json.dumps({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            ["Email/query", {"accountId": account_id, "filter": {"text": subject}, "limit": 10}, "q0"],
            ["Email/get", {"accountId": account_id, "#ids": {"resultOf": "q0", "name": "Email/query", "path": "/ids"}, "properties": ["subject"]}, "g0"],
        ],
    }).encode("utf-8")
    response = request(
        "POST",
        join_url(base_url, api_url),
        body,
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"},
        timeout,
        insecure_tls=insecure_tls,
    )
    require(response.status == 200, f"JMAP Email/query returned HTTP {response.status}: {response.text[:300]}")
    payload = json.loads(response.text)
    method_responses = payload.get("methodResponses", [])
    email_get = next((item[1] for item in method_responses if item and item[0] == "Email/get"), {})
    for item in email_get.get("list", []):
        require(item.get("subject") != subject, "JMAP still exposes the MAPI-emptied Deleted Items fixture")
    print("ok jmap_mapi_empty_deleted_items_absence")


def check_ews_basic(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> None:
    response = request(
        "POST",
        join_url(base_url, "/EWS/Exchange.asmx"),
        EWS_TIMEZONES_BODY.encode("utf-8"),
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "text/xml; charset=utf-8",
            "Accept": "text/xml",
            "User-Agent": "lpe-rca-connectivity-check/0.1",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(response.status == 200, f"EWS Basic probe returned HTTP {response.status}: {response.text[:500]}")
    require("xml" in content_type(response.headers), "EWS Basic probe did not return XML headers")
    require("<m:GetServerTimeZonesResponse>" in response.text, "EWS Basic probe did not return timezone response")
    require("<m:ResponseCode>NoError</m:ResponseCode>" in response.text, "EWS Basic probe did not authenticate successfully")
    print("ok ews_basic")


def check_ews_mailbox_access(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> None:
    payload = ews_call(
        base_url,
        email,
        password,
        "FindFolder",
        """
        <m:FindFolder Traversal="Shallow">
          <m:FolderShape><t:BaseShape>AllProperties</t:BaseShape></m:FolderShape>
          <m:ParentFolderIds><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderIds>
        </m:FindFolder>
        """,
        insecure_tls,
        timeout,
    )
    require_ews_no_error("EWS FindFolder", payload)
    for fragment in ["<t:ContactsFolder>", "<t:CalendarFolder>", "<t:TasksFolder>"]:
        require(fragment in payload, f"EWS FindFolder did not expose mailbox collaboration folder {fragment}")
    print("ok ews_mailbox_access")


def check_ews_send_sent(
    base_url: str,
    email: str,
    password: str,
    recipient: str,
    insecure_tls: bool,
    timeout: int,
    check_mapi: bool,
) -> None:
    marker = uuid.uuid4().hex[:12]
    subject = f"LPE RCA canonical send {marker}"
    body_text = f"RCA canonical Sent proof {marker}"
    message_id: str | None = None
    try:
        created = ews_call(
            base_url,
            email,
            password,
            "CreateItem SendAndSaveCopy",
            f"""
            <m:CreateItem MessageDisposition="SendAndSaveCopy">
              <m:Items>
                <t:Message>
                  <t:Subject>{xml_escape(subject)}</t:Subject>
                  <t:Body BodyType="Text">{xml_escape(body_text)}</t:Body>
                  <t:ToRecipients>
                    <t:Mailbox><t:EmailAddress>{xml_escape(recipient)}</t:EmailAddress></t:Mailbox>
                  </t:ToRecipients>
                </t:Message>
              </m:Items>
            </m:CreateItem>
            """,
            insecure_tls,
            timeout,
        )
        require_ews_no_error("EWS CreateItem SendAndSaveCopy", created)
        message_id = extract_ews_item_id(created, "message:", "EWS CreateItem SendAndSaveCopy")

        fetched = ews_call(
            base_url,
            email,
            password,
            "GetItem sent message",
            f"""
            <m:GetItem>
              <m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape>
              <m:ItemIds><t:ItemId Id="{xml_escape(message_id)}"/></m:ItemIds>
            </m:GetItem>
            """,
            insecure_tls,
            timeout,
        )
        require_ews_no_error("EWS GetItem sent message", fetched)
        require(subject in fetched, "EWS GetItem did not return the canonical sent message subject")

        sent = ews_call(
            base_url,
            email,
            password,
            "FindItem sentitems",
            """
            <m:FindItem Traversal="Shallow">
              <m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape>
              <m:ParentFolderIds><t:DistinguishedFolderId Id="sentitems"/></m:ParentFolderIds>
            </m:FindItem>
            """,
            insecure_tls,
            timeout,
        )
        require_ews_no_error("EWS FindItem sentitems", sent)
        require(message_id in sent or subject in sent, "EWS Sent folder did not expose the submitted canonical message")
        if check_mapi:
            check_mapi_emsmdb_sent_message(
                base_url,
                email,
                password,
                subject,
                insecure_tls,
                timeout,
            )
            check_mapi_emsmdb_sent_sync_manifest(
                base_url,
                email,
                password,
                subject,
                insecure_tls,
                timeout,
            )
    finally:
        if message_id:
            delete_ews_item(base_url, email, password, message_id, insecure_tls, timeout, required=False)
    print("ok ews_canonical_send_sent")


def check_ews_contact_calendar_and_mapi_fixture(
    base_url: str,
    email: str,
    password: str,
    insecure_tls: bool,
    timeout: int,
    check_mapi: bool,
) -> None:
    marker = uuid.uuid4().hex[:10]
    contact_name = f"LPE RCA Contact {marker}"
    contact_email = f"lpe-rca-{marker}@example.test"
    event_subject = f"LPE RCA Calendar {marker}"
    start = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=3)
    end = start + timedelta(minutes=30)
    contact_id: str | None = None
    event_id: str | None = None
    try:
        created_contact = ews_call(
            base_url,
            email,
            password,
            "CreateItem Contact",
            f"""
            <m:CreateItem>
              <m:SavedItemFolderId><t:DistinguishedFolderId Id="contacts"/></m:SavedItemFolderId>
              <m:Items>
                <t:Contact>
                  <t:DisplayName>{xml_escape(contact_name)}</t:DisplayName>
                  <t:GivenName>LPE</t:GivenName>
                  <t:Surname>RCA</t:Surname>
                  <t:EmailAddresses>
                    <t:Entry Key="EmailAddress1">{xml_escape(contact_email)}</t:Entry>
                  </t:EmailAddresses>
                  <t:Body BodyType="Text">Temporary RCA contact fixture</t:Body>
                </t:Contact>
              </m:Items>
            </m:CreateItem>
            """,
            insecure_tls,
            timeout,
        )
        require_ews_no_error("EWS CreateItem Contact", created_contact)
        contact_id = extract_ews_item_id(created_contact, "contact:", "EWS CreateItem Contact")

        fetched_contact = ews_call(
            base_url,
            email,
            password,
            "GetItem Contact",
            f"""
            <m:GetItem>
              <m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape>
              <m:ItemIds><t:ItemId Id="{xml_escape(contact_id)}"/></m:ItemIds>
            </m:GetItem>
            """,
            insecure_tls,
            timeout,
        )
        require_ews_no_error("EWS GetItem Contact", fetched_contact)
        require(contact_name in fetched_contact and contact_email in fetched_contact, "EWS contact read did not return the created fixture data")

        if check_mapi:
            check_mapi_nspi_address_book(
                base_url,
                email,
                password,
                insecure_tls,
                timeout,
                expected_name=contact_name,
                expected_email=contact_email,
            )

        created_event = ews_call(
            base_url,
            email,
            password,
            "CreateItem CalendarItem",
            f"""
            <m:CreateItem>
              <m:SavedItemFolderId><t:DistinguishedFolderId Id="calendar"/></m:SavedItemFolderId>
              <m:Items>
                <t:CalendarItem>
                  <t:Subject>{xml_escape(event_subject)}</t:Subject>
                  <t:Location>RCA fixture</t:Location>
                  <t:Start>{start.isoformat().replace("+00:00", "Z")}</t:Start>
                  <t:End>{end.isoformat().replace("+00:00", "Z")}</t:End>
                  <t:Body BodyType="Text">Temporary RCA calendar fixture</t:Body>
                </t:CalendarItem>
              </m:Items>
            </m:CreateItem>
            """,
            insecure_tls,
            timeout,
        )
        require_ews_no_error("EWS CreateItem CalendarItem", created_event)
        event_id = extract_ews_item_id(created_event, "event:", "EWS CreateItem CalendarItem")

        fetched_event = ews_call(
            base_url,
            email,
            password,
            "GetItem CalendarItem",
            f"""
            <m:GetItem>
              <m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape>
              <m:ItemIds><t:ItemId Id="{xml_escape(event_id)}"/></m:ItemIds>
            </m:GetItem>
            """,
            insecure_tls,
            timeout,
        )
        require_ews_no_error("EWS GetItem CalendarItem", fetched_event)
        require(event_subject in fetched_event and event_id in fetched_event, "EWS calendar read did not return the created fixture data")
    finally:
        if event_id:
            delete_ews_item(base_url, email, password, event_id, insecure_tls, timeout, required=False)
        if contact_id:
            delete_ews_item(base_url, email, password, contact_id, insecure_tls, timeout, required=False)

    require_deleted_ews_item(base_url, email, password, contact_id, "contact", insecure_tls, timeout)
    require_deleted_ews_item(base_url, email, password, event_id, "calendar", insecure_tls, timeout)
    print("ok ews_contact_calendar_live_fixtures")


def delete_ews_item(
    base_url: str,
    email: str,
    password: str,
    item_id: str,
    insecure_tls: bool,
    timeout: int,
    required: bool,
) -> None:
    payload = ews_call(
        base_url,
        email,
        password,
        "DeleteItem cleanup",
        f"""
        <m:DeleteItem DeleteType="HardDelete">
          <m:ItemIds><t:ItemId Id="{xml_escape(item_id)}"/></m:ItemIds>
        </m:DeleteItem>
        """,
        insecure_tls,
        timeout,
    )
    if required:
        require_ews_no_error("EWS DeleteItem", payload)


def require_deleted_ews_item(
    base_url: str,
    email: str,
    password: str,
    item_id: str | None,
    label: str,
    insecure_tls: bool,
    timeout: int,
) -> None:
    if not item_id:
        return
    payload = ews_call(
        base_url,
        email,
        password,
        f"GetItem deleted {label}",
        f"""
        <m:GetItem>
          <m:ItemShape><t:BaseShape>Default</t:BaseShape></m:ItemShape>
          <m:ItemIds><t:ItemId Id="{xml_escape(item_id)}"/></m:ItemIds>
        </m:GetItem>
        """,
        insecure_tls,
        timeout,
    )
    require("ErrorItemNotFound" in payload, f"EWS deleted {label} fixture was still readable: {payload[:800]}")


def check_mapi_ping(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> None:
    sessions = [
        (
            "/mapi/emsmdb",
            request(
                "POST",
                join_url(base_url, f"/mapi/emsmdb/?mailboxId={urllib.parse.quote(email, safe='@')}"),
                b"",
                {
                    "Authorization": basic_auth_header(email, password),
                    "Content-Type": "application/mapi-http",
                    "X-RequestType": "Connect",
                    "X-RequestId": mapi_request_id(),
                    "X-ClientInfo": mapi_client_info(),
                    "User-Agent": "MapiHttpClient",
                },
                timeout,
                insecure_tls=insecure_tls,
            ),
        ),
        (
            "/mapi/nspi",
            request(
                "POST",
                join_url(base_url, f"/mapi/nspi/?mailboxId={urllib.parse.quote(email, safe='@')}"),
                bytes(45),
                {
                    "Authorization": basic_auth_header(email, password),
                    "Content-Type": "application/octet-stream",
                    "X-RequestType": "Bind",
                    "X-RequestId": mapi_request_id(),
                    "X-ClientInfo": mapi_client_info(),
                    "User-Agent": "MapiHttpClient",
                },
                timeout,
                insecure_tls=insecure_tls,
            ),
        ),
    ]
    for path, session in sessions:
        require(session.status == 200, f"MAPI session setup {path} returned HTTP {session.status}: {session.text[:300]}")
        require("application/mapi-http" in content_type(session.headers), f"MAPI session setup {path} did not return MAPI content")
        session_response_code = header_value(session.headers, "x-responsecode")
        require(
            session_response_code == "0",
            f"MAPI session setup {path} returned X-ResponseCode {session_response_code!r}: {session.text[:300]}",
        )
        cookie = cookie_header(session)
        require("MapiContext=" in cookie, f"MAPI session setup {path} did not issue a MapiContext cookie")
        require("MapiSequence=" in cookie, f"MAPI session setup {path} did not issue a MapiSequence cookie")
        response = request(
            "POST",
            join_url(base_url, path),
            b"",
            {
                "Authorization": basic_auth_header(email, password),
                "Content-Type": "application/mapi-http",
                "Cookie": cookie,
                "X-RequestType": "PING",
                "X-RequestId": mapi_request_id(),
                "X-ClientInfo": mapi_client_info(),
            },
            timeout,
            insecure_tls=insecure_tls,
        )
        require(response.status == 200, f"MAPI PING {path} returned HTTP {response.status}: {response.text[:300]}")
        require("application/mapi-http" in content_type(response.headers), f"MAPI PING {path} did not return MAPI content")
        response_code = header_value(response.headers, "x-responsecode")
        require(response_code == "0", f"MAPI PING {path} returned X-ResponseCode {response_code!r}")
    print("ok mapi_ping")


def mapi_nspi_bind_cookie(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> str:
    response = request(
        "POST",
        join_url(base_url, f"/mapi/nspi/?mailboxId={urllib.parse.quote(email, safe='@')}"),
        bytes(45),
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "application/octet-stream",
            "X-RequestType": "Bind",
            "X-RequestId": mapi_request_id(1),
            "X-ClientInfo": mapi_client_info(),
            "User-Agent": "MapiHttpClient",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(response.status == 200, f"MAPI NSPI Bind returned HTTP {response.status}: {response.text[:300]}")
    require("application/mapi-http" in content_type(response.headers), "MAPI NSPI Bind did not return MAPI content")
    response_code = header_value(response.headers, "x-responsecode")
    require(response_code == "0", f"MAPI NSPI Bind returned X-ResponseCode {response_code!r}: {response.text[:300]}")
    expiration = header_value(response.headers, "x-expirationinfo")
    require(expiration.isdigit() and int(expiration) > 0, f"MAPI NSPI Bind returned invalid X-ExpirationInfo {expiration!r}")
    client_info = header_value(response.headers, "x-clientinfo")
    require_guid_counter_header(client_info, "MAPI NSPI Bind X-ClientInfo")
    cookie = cookie_header(response)
    require("MapiContext=" in cookie, "MAPI NSPI Bind did not issue a MapiContext cookie")
    require("MapiSequence=" in cookie, "MAPI NSPI Bind did not issue a MapiSequence cookie")
    return cookie


def check_mapi_nspi_bind_octet_stream(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> None:
    mapi_nspi_bind_cookie(base_url, email, password, insecure_tls, timeout)
    print("ok mapi_nspi_bind_octet_stream")


def check_mapi_nspi_address_book(
    base_url: str,
    email: str,
    password: str,
    insecure_tls: bool,
    timeout: int,
    expected_name: str | None = None,
    expected_email: str | None = None,
) -> None:
    path = f"/mapi/nspi/?mailboxId={urllib.parse.quote(email, safe='@')}"
    cookie = mapi_nspi_bind_cookie(base_url, email, password, insecure_tls, timeout)
    lookup = expected_email or email
    probe_bodies: list[tuple[str, bytes]] = [
        ("ResolveNames", resolve_names_request(lookup, [0x3003_001F, 0x3001_001F])),
        ("GetMatches", lookup.encode("utf-8")),
        ("QueryRows", lookup.encode("utf-8")),
    ]
    probe_assertions = {
        "ResolveNames": assert_nspi_resolve_names_payload,
        "GetMatches": assert_nspi_get_matches_payload,
        "QueryRows": assert_nspi_query_rows_payload,
        "GetProps": assert_nspi_get_props_payload,
    }
    minimal_id: int | None = None
    for request_type, body in probe_bodies:
        response = request(
            "POST",
            join_url(base_url, path),
            body,
            {
                "Authorization": basic_auth_header(email, password),
                "Content-Type": "application/octet-stream",
                "Cookie": cookie,
                "X-RequestType": request_type,
                "X-RequestId": mapi_request_id(request_type),
                "X-ClientInfo": mapi_client_info(),
                "User-Agent": "MapiHttpClient",
            },
            timeout,
            insecure_tls=insecure_tls,
        )
        require(response.status == 200, f"MAPI NSPI {request_type} returned HTTP {response.status}: {response.text[:300]}")
        require("application/mapi-http" in content_type(response.headers), f"MAPI NSPI {request_type} did not return MAPI content")
        response_code = header_value(response.headers, "x-responsecode")
        require(response_code == "0", f"MAPI NSPI {request_type} returned X-ResponseCode {response_code!r}: {response.text[:300]}")
        payload = mapi_http_binary_payload(response.body)
        probe_assertions[request_type](payload, request_type)
        if expected_name and expected_email:
            assert_nspi_fixture_payload(payload, request_type, expected_name, expected_email)
        if request_type == "GetMatches":
            minimal_id = nspi_first_minimal_id(payload, request_type)
        cookie = update_cookie_header(cookie, response)
    require(minimal_id is not None, "MAPI NSPI GetMatches did not return a MinimalId for GetProps")
    get_props_body = nspi_get_props_request(
        minimal_id,
        [0x3003_001F, 0x3001_001F, 0x39FE_001F, 0x3002_001F],
    )
    response = request(
        "POST",
        join_url(base_url, path),
        get_props_body,
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "application/octet-stream",
            "Cookie": cookie,
            "X-RequestType": "GetProps",
            "X-RequestId": mapi_request_id("GetProps"),
            "X-ClientInfo": mapi_client_info(),
            "User-Agent": "MapiHttpClient",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(response.status == 200, f"MAPI NSPI GetProps returned HTTP {response.status}: {response.text[:300]}")
    require("application/mapi-http" in content_type(response.headers), "MAPI NSPI GetProps did not return MAPI content")
    response_code = header_value(response.headers, "x-responsecode")
    require(response_code == "0", f"MAPI NSPI GetProps returned X-ResponseCode {response_code!r}: {response.text[:300]}")
    payload = mapi_http_binary_payload(response.body)
    assert_nspi_get_props_payload(payload, "GetProps")
    if expected_name and expected_email:
        assert_nspi_fixture_payload(payload, "GetProps", expected_name, expected_email)
    if expected_name and expected_email:
        print("ok mapi_nspi_address_book_fixture")
    else:
        print("ok mapi_nspi_address_book")


def check_mapi_nspi_resolve_authenticated_mailbox(
    base_url: str,
    email: str,
    password: str,
    insecure_tls: bool,
    timeout: int,
) -> None:
    path = f"/mapi/nspi/?mailboxId={urllib.parse.quote(email, safe='@')}"
    cookie = mapi_nspi_bind_cookie(base_url, email, password, insecure_tls, timeout)
    response = request(
        "POST",
        join_url(base_url, path),
        resolve_names_request(email, [0x3003_001F, 0x3001_001F]),
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "application/octet-stream",
            "Cookie": cookie,
            "X-RequestType": "ResolveNames",
            "X-RequestId": mapi_request_id("ResolveNamesSelf"),
            "X-ClientInfo": mapi_client_info(),
            "User-Agent": "MapiHttpClient",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(response.status == 200, f"MAPI NSPI ResolveNames self returned HTTP {response.status}: {response.text[:300]}")
    require("application/mapi-http" in content_type(response.headers), "MAPI NSPI ResolveNames self did not return MAPI content")
    response_code = header_value(response.headers, "x-responsecode")
    require(response_code == "0", f"MAPI NSPI ResolveNames self returned X-ResponseCode {response_code!r}: {response.text[:300]}")
    payload = mapi_http_binary_payload(response.body)
    assert_nspi_resolve_names_payload(payload, "ResolveNames")
    require(email.lower().encode("utf-16le") in payload.lower(), "MAPI NSPI ResolveNames self did not include the authenticated mailbox SMTP address")
    print("ok mapi_nspi_resolve_authenticated_mailbox")


def check_mapi_emsmdb_sent_message(
    base_url: str,
    email: str,
    password: str,
    expected_subject: str,
    insecure_tls: bool,
    timeout: int,
) -> None:
    connect = request(
        "POST",
        join_url(base_url, f"/mapi/emsmdb/?mailboxId={urllib.parse.quote(email, safe='@')}"),
        b"",
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "application/mapi-http",
            "X-RequestType": "Connect",
            "X-RequestId": mapi_request_id("Connect"),
            "X-ClientInfo": mapi_client_info(),
            "User-Agent": "MapiHttpClient",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(connect.status == 200, f"MAPI EMSMDB Connect returned HTTP {connect.status}: {connect.text[:300]}")
    require("application/mapi-http" in content_type(connect.headers), "MAPI EMSMDB Connect did not return MAPI content")
    require(header_value(connect.headers, "x-responsecode") == "0", "MAPI EMSMDB Connect did not return success")
    cookie = cookie_header(connect)
    require("MapiContext=" in cookie, "MAPI EMSMDB Connect did not issue an EMSMDB session cookie")
    require("MapiSequence=" in cookie, "MAPI EMSMDB Connect did not issue an EMSMDB sequence cookie")

    rops = mapi_sent_subject_table_rops()
    execute = request(
        "POST",
        join_url(base_url, f"/mapi/emsmdb/?mailboxId={urllib.parse.quote(email, safe='@')}"),
        mapi_execute_body(mapi_rop_buffer(rops, [1, 0xFFFF_FFFF, 0xFFFF_FFFF])),
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "application/mapi-http",
            "Cookie": cookie,
            "X-RequestType": "Execute",
            "X-RequestId": mapi_request_id("Execute"),
            "X-ClientInfo": mapi_client_info(),
            "User-Agent": "MapiHttpClient",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(execute.status == 200, f"MAPI EMSMDB Execute returned HTTP {execute.status}: {execute.text[:300]}")
    require("application/mapi-http" in content_type(execute.headers), "MAPI EMSMDB Execute did not return MAPI content")
    require(header_value(execute.headers, "x-responsecode") == "0", "MAPI EMSMDB Execute did not return success")
    payload = mapi_http_binary_payload(execute.body)
    response_rops = mapi_execute_response_rops(payload, "MAPI EMSMDB Execute")
    require(len(response_rops) > 20, "MAPI EMSMDB Execute returned an empty or static-sized ROP payload")
    require(response_rops[0] == 0x02, "MAPI EMSMDB Execute did not start with RopOpenFolder response")
    require(contains_bytes(response_rops, bytes([0x05, 0x02])), "MAPI EMSMDB Execute did not include RopGetContentsTable response")
    require(contains_bytes(response_rops, bytes([0x15, 0x02])), "MAPI EMSMDB Execute did not include RopQueryRows response")
    require(
        contains_bytes(response_rops, utf16z(expected_subject)),
        "MAPI EMSMDB Sent table did not expose the EWS-created canonical Sent message",
    )
    print("ok mapi_emsmdb_canonical_sent_message")


def check_mapi_emsmdb_sent_sync_manifest(
    base_url: str,
    email: str,
    password: str,
    expected_subject: str,
    insecure_tls: bool,
    timeout: int,
) -> None:
    connect = request(
        "POST",
        join_url(base_url, f"/mapi/emsmdb/?mailboxId={urllib.parse.quote(email, safe='@')}"),
        b"",
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "application/mapi-http",
            "X-RequestType": "Connect",
            "X-RequestId": mapi_request_id("ConnectSync"),
            "X-ClientInfo": mapi_client_info(),
            "User-Agent": "MapiHttpClient",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(connect.status == 200, f"MAPI EMSMDB sync Connect returned HTTP {connect.status}: {connect.text[:300]}")
    require("application/mapi-http" in content_type(connect.headers), "MAPI EMSMDB sync Connect did not return MAPI content")
    require(header_value(connect.headers, "x-responsecode") == "0", "MAPI EMSMDB sync Connect did not return success")
    cookie = cookie_header(connect)
    require("MapiContext=" in cookie, "MAPI EMSMDB sync Connect did not issue an EMSMDB session cookie")
    require("MapiSequence=" in cookie, "MAPI EMSMDB sync Connect did not issue an EMSMDB sequence cookie")

    execute = request(
        "POST",
        join_url(base_url, f"/mapi/emsmdb/?mailboxId={urllib.parse.quote(email, safe='@')}"),
        mapi_execute_body(mapi_rop_buffer(mapi_sent_content_sync_rops(), [1, 0xFFFF_FFFF, 0xFFFF_FFFF])),
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "application/mapi-http",
            "Cookie": cookie,
            "X-RequestType": "Execute",
            "X-RequestId": mapi_request_id("ExecuteSync"),
            "X-ClientInfo": mapi_client_info(),
            "User-Agent": "MapiHttpClient",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(execute.status == 200, f"MAPI EMSMDB sync Execute returned HTTP {execute.status}: {execute.text[:300]}")
    require("application/mapi-http" in content_type(execute.headers), "MAPI EMSMDB sync Execute did not return MAPI content")
    require(header_value(execute.headers, "x-responsecode") == "0", "MAPI EMSMDB sync Execute did not return success")
    response_rops = mapi_execute_response_rops(mapi_http_binary_payload(execute.body), "MAPI EMSMDB sync Execute")
    require(contains_bytes(response_rops, bytes([0x70, 0x02, 0, 0, 0, 0])), "MAPI EMSMDB sync did not configure a synchronization source")
    require(contains_bytes(response_rops, bytes([0x4E, 0x02, 0, 0, 0, 0])), "MAPI EMSMDB sync did not return a FastTransfer buffer")
    require(
        not contains_bytes(response_rops, b"LPE-MAPI-SYNC\0"),
        "MAPI EMSMDB sync returned the deprecated LPE-private sync manifest marker",
    )
    require(
        contains_bytes(response_rops, (0x4012_0003).to_bytes(4, "little")),
        "MAPI EMSMDB sync did not return an MS-OXCFXICS IncrSyncChg marker",
    )
    require(
        contains_bytes(response_rops, expected_subject.encode("utf-8")),
        "MAPI EMSMDB sync manifest did not expose the EWS-created canonical Sent message",
    )
    print("ok mapi_emsmdb_sent_sync_manifest")


def check_mapi_empty_deleted_items_fixture(
    base_url: str,
    email: str,
    password: str,
    insecure_tls: bool,
    timeout: int,
) -> None:
    marker = uuid.uuid4().hex[:12]
    subject = f"LPE RCA MAPI empty Deleted Items {marker}"
    created = ews_call(
        base_url,
        email,
        password,
        "CreateItem deleteditems",
        f"""
        <m:CreateItem>
          <m:SavedItemFolderId><t:DistinguishedFolderId Id="deleteditems"/></m:SavedItemFolderId>
          <m:Items>
            <t:Message>
              <t:Subject>{xml_escape(subject)}</t:Subject>
              <t:Body BodyType="Text">MAPI empty Deleted Items fixture {xml_escape(marker)}</t:Body>
            </t:Message>
          </m:Items>
        </m:CreateItem>
        """,
        insecure_tls,
        timeout,
    )
    require_ews_no_error("EWS CreateItem deleteditems", created)

    connect = request(
        "POST",
        join_url(base_url, f"/mapi/emsmdb/?mailboxId={urllib.parse.quote(email, safe='@')}"),
        b"",
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "application/mapi-http",
            "X-RequestType": "Connect",
            "X-RequestId": mapi_request_id("ConnectTrash"),
            "X-ClientInfo": mapi_client_info(),
            "User-Agent": "MapiHttpClient",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(connect.status == 200, f"MAPI EMSMDB Trash Connect returned HTTP {connect.status}: {connect.text[:300]}")
    require(header_value(connect.headers, "x-responsecode") == "0", "MAPI EMSMDB Trash Connect did not return success")
    cookie = cookie_header(connect)
    execute = request(
        "POST",
        join_url(base_url, f"/mapi/emsmdb/?mailboxId={urllib.parse.quote(email, safe='@')}"),
        mapi_execute_body(mapi_rop_buffer(mapi_empty_deleted_items_rops(), [1, 0xFFFF_FFFF])),
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "application/mapi-http",
            "Cookie": cookie,
            "X-RequestType": "Execute",
            "X-RequestId": mapi_request_id("EmptyTrash"),
            "X-ClientInfo": mapi_client_info(),
            "User-Agent": "MapiHttpClient",
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(execute.status == 200, f"MAPI EMSMDB Empty Deleted Items returned HTTP {execute.status}: {execute.text[:300]}")
    require(header_value(execute.headers, "x-responsecode") == "0", "MAPI EMSMDB Empty Deleted Items did not return success")
    response_rops = mapi_execute_response_rops(mapi_http_binary_payload(execute.body), "MAPI EMSMDB Empty Deleted Items")
    require(contains_bytes(response_rops, bytes([0x58, 0x01, 0, 0, 0, 0])), "MAPI EmptyFolder did not return success")

    deleted_items = ews_call(
        base_url,
        email,
        password,
        "FindItem deleteditems after MAPI empty",
        """
        <m:FindItem Traversal="Shallow">
          <m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape>
          <m:ParentFolderIds><t:DistinguishedFolderId Id="deleteditems"/></m:ParentFolderIds>
        </m:FindItem>
        """,
        insecure_tls,
        timeout,
    )
    require_ews_no_error("EWS FindItem deleteditems after MAPI empty", deleted_items)
    require(subject not in deleted_items, "EWS Deleted Items still exposes the MAPI-emptied fixture")
    check_jmap_email_subject_absent(base_url, email, password, subject, insecure_tls, timeout)
    print("skip imap_mapi_empty_deleted_items_absence no IMAP connection helper is configured in this harness")
    print("ok mapi_empty_deleted_items_fixture")


def check_rpc_proxy_auth(base_url: str, email: str, password: str | None, insecure_tls: bool, timeout: int) -> None:
    parsed = urllib.parse.urlparse(base_url)
    rpc_url = join_url(base_url, "/rpc/rpcproxy.dll")
    headers = {
        "Accept": "application/rpc",
        "User-Agent": "MSRPC",
    }

    anonymous = request("RPC_IN_DATA", rpc_url, b"", headers, timeout, insecure_tls=insecure_tls)
    require(
        anonymous.status == 401,
        f"anonymous RPC proxy probe returned HTTP {anonymous.status}; RCA requires anonymous to fail",
    )
    require(
        "basic" in header_value(anonymous.headers, "www-authenticate").lower(),
        "anonymous RPC proxy probe did not advertise Basic authentication",
    )

    if password is not None:
        authenticated_headers = dict(headers)
        authenticated_headers["Authorization"] = basic_auth_header(email, password)
        for method, body, expected_status, expected_length in [
            ("RPC_IN_DATA", b"", "echo", 20),
            ("RPC_OUT_DATA", rpc_rts_conn_a1_body(), "rts-connect", 72),
        ]:
            authenticated = request(
                method,
                rpc_url,
                body,
                authenticated_headers,
                timeout,
                read_limit=expected_length,
                insecure_tls=insecure_tls,
            )
            require(
                authenticated.status == 200,
                f"authenticated RPC proxy {method} probe returned HTTP {authenticated.status}: {authenticated.text[:300]}",
            )
            require(
                "application/rpc" in content_type(authenticated.headers),
                f"authenticated RPC proxy {method} probe did not return application/rpc",
            )
            require(
                header_value(authenticated.headers, "x-lpe-rpc-proxy-status") == expected_status,
                f"authenticated RPC proxy {method} returned unexpected compatibility status {header_value(authenticated.headers, 'x-lpe-rpc-proxy-status')!r}; expected {expected_status!r}",
            )
            require(
                len(authenticated.body) == expected_length,
                f"authenticated RPC proxy {method} returned unexpected body length {len(authenticated.body)}",
            )
    print("ok rpc_proxy_auth")


def check_rpc_proxy_mailstore_ping(
    base_url: str,
    email: str,
    password: str,
    insecure_tls: bool,
    timeout: int,
) -> None:
    parsed = urllib.parse.urlparse(base_url)
    rpc_host = parsed.hostname or parsed.netloc
    require(bool(rpc_host), "base URL must include a host for RPC proxy checks")
    rpc_url = join_url(base_url, f"/rpc/rpcproxy.dll?{rpc_host}:6001")
    headers = {
        "Accept": "application/rpc",
        "Authorization": basic_auth_header(email, password),
        "User-Agent": "MSRPC",
    }
    out_body = rpc_rts_conn_a1_body()
    in_response = request(
        "RPC_IN_DATA",
        rpc_url,
        rpc_rts_conn_b1_body(out_body[32:48]),
        headers,
        timeout,
        read_limit=0,
        insecure_tls=insecure_tls,
    )
    require(
        in_response.status == 200,
        f"RPC proxy mailstore IN ping returned HTTP {in_response.status}: {in_response.text[:300]}",
    )
    require(
        header_value(in_response.headers, "x-lpe-rpc-proxy-status") == "in-channel-open",
        f"RPC proxy mailstore IN ping returned compatibility status {header_value(in_response.headers, 'x-lpe-rpc-proxy-status')!r}; expected 'in-channel-open'",
    )
    time.sleep(0.2)

    response = request(
        "RPC_OUT_DATA",
        rpc_url,
        out_body,
        headers,
        timeout,
        read_limit=184,
        insecure_tls=insecure_tls,
    )
    require(
        response.status == 200,
        f"RPC proxy mailstore OUT ping returned HTTP {response.status}: {response.text[:300]}",
    )
    require(
        "application/rpc" in content_type(response.headers),
        "RPC proxy mailstore OUT ping did not return application/rpc",
    )
    require(
        header_value(response.headers, "x-lpe-rpc-proxy-status") == "endpoint-ping",
        f"RPC proxy mailstore OUT ping returned compatibility status {header_value(response.headers, 'x-lpe-rpc-proxy-status')!r}; expected 'endpoint-ping'",
    )
    require(len(response.body) >= 184, f"RPC proxy mailstore OUT ping returned only {len(response.body)} bytes")
    require(response.body[72] == 0x05 and response.body[74] == 0x0C, "mailstore ping did not include a DCE/RPC bind ACK")
    print("ok rpc_proxy_mailstore_ping")
























def xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def main() -> int:
    args = parse_args()

    require(args.base_url, "provide --base-url or set LPE_RCA_BASE_URL")
    require(args.email, "provide --email or set LPE_RCA_EMAIL")
    expect_ews = args.expect_ews or args.ews_readiness or args.outlook_rca_readiness
    expect_exch_provider = (
        args.expect_exchange_providers or args.expect_exch_provider or args.outlook_rca_readiness
    )
    expect_expr_provider = (
        args.expect_exchange_providers or args.expect_expr_provider or args.outlook_rca_readiness
    )
    expect_mapi = args.expect_mapi or args.outlook_rca_readiness
    run_ews_basic = args.check_ews_basic or args.ews_readiness or args.outlook_rca_readiness
    run_live_fixtures = args.check_live_fixtures or args.ews_readiness or args.outlook_rca_readiness
    run_live_mapi_proof = args.check_live_fixtures or args.outlook_rca_readiness
    run_mapi_ping = args.check_mapi_ping or args.outlook_rca_readiness
    run_mapi_nspi_bind = args.check_mapi_nspi_bind_octet_stream or args.outlook_rca_readiness
    run_mapi_nspi_address_book = args.check_mapi_nspi_address_book
    run_mapi_empty_deleted_items = args.check_mapi_empty_deleted_items
    run_rpc_proxy_auth = args.check_rpc_proxy_auth or args.outlook_rca_readiness
    run_rpc_proxy_mailstore_ping = args.check_rpc_proxy_mailstore_ping or args.outlook_rca_readiness

    if args.outlook_rca_readiness:
        require(args.password, "--outlook-rca-readiness requires --password or LPE_RCA_PASSWORD")
    if (
        run_ews_basic
        or run_mapi_ping
        or run_mapi_nspi_bind
        or run_mapi_nspi_address_book
        or run_mapi_empty_deleted_items
        or run_live_fixtures
        or run_rpc_proxy_mailstore_ping
    ):
        require(args.password, "requested authenticated checks require --password or LPE_RCA_PASSWORD")
    if run_live_fixtures or run_mapi_empty_deleted_items:
        require(
            args.allow_mutating_fixtures,
            "live readiness checks create and delete fixtures; pass --allow-mutating-fixtures to permit this",
        )
    if run_mapi_empty_deleted_items:
        require(
            args.dangerously_empty_deleted_items,
            "--check-mapi-empty-deleted-items empties the target mailbox Deleted Items folder; pass --dangerously-empty-deleted-items to acknowledge this",
        )

    base_url = args.base_url.rstrip("/")
    check_pox_autodiscover(
        base_url,
        args.email,
        expect_ews,
        expect_exch_provider,
        expect_expr_provider,
        expect_mapi,
        args.expected_service_host,
        args.insecure,
        args.timeout,
    )
    check_json_autodiscover(
        base_url,
        args.email,
        expect_ews,
        expect_mapi,
        args.expected_service_host,
        args.insecure,
        args.timeout,
    )
    check_jmap_publication_headers(base_url, args.expected_service_host, args.insecure, args.timeout)

    if args.password:
        check_jmap_session(base_url, args.email, args.password, args.insecure, args.timeout)
        if run_ews_basic:
            check_ews_basic(base_url, args.email, args.password, args.insecure, args.timeout)
        if run_live_fixtures:
            check_ews_mailbox_access(base_url, args.email, args.password, args.insecure, args.timeout)
            check_ews_send_sent(
                base_url,
                args.email,
                args.password,
                args.send_recipient or args.email,
                args.insecure,
                args.timeout,
                check_mapi=run_live_mapi_proof,
            )
            check_ews_contact_calendar_and_mapi_fixture(
                base_url,
                args.email,
                args.password,
                args.insecure,
                args.timeout,
                check_mapi=run_live_mapi_proof,
            )
        if run_mapi_ping:
            check_mapi_ping(base_url, args.email, args.password, args.insecure, args.timeout)
        if run_mapi_nspi_bind:
            check_mapi_nspi_bind_octet_stream(base_url, args.email, args.password, args.insecure, args.timeout)
        if args.outlook_rca_readiness:
            check_mapi_nspi_resolve_authenticated_mailbox(base_url, args.email, args.password, args.insecure, args.timeout)
        if run_mapi_nspi_address_book:
            check_mapi_nspi_address_book(base_url, args.email, args.password, args.insecure, args.timeout)
        if run_mapi_empty_deleted_items:
            check_mapi_empty_deleted_items_fixture(base_url, args.email, args.password, args.insecure, args.timeout)
    else:
        print("skip jmap_session password not provided")
        if (
            run_ews_basic
            or run_mapi_ping
            or run_mapi_nspi_bind
            or run_mapi_nspi_address_book
            or run_mapi_empty_deleted_items
            or run_live_fixtures
            or run_rpc_proxy_mailstore_ping
        ):
            raise RuntimeError("requested authenticated checks require --password or LPE_RCA_PASSWORD")
    if run_rpc_proxy_auth:
        check_rpc_proxy_auth(base_url, args.email, args.password, args.insecure, args.timeout)
    if run_rpc_proxy_mailstore_ping:
        require(args.password, "--check-rpc-proxy-mailstore-ping requires --password or LPE_RCA_PASSWORD")
        check_rpc_proxy_mailstore_ping(base_url, args.email, args.password, args.insecure, args.timeout)

    return 0


if __name__ == "__main__":
    sys.exit(main())
