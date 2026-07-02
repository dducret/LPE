from __future__ import annotations

import re
import textwrap

from .http import basic_auth_header, content_type, join_url, request, require


EWS_BODY_TEMPLATE = """\
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
            xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
  <s:Body>
{body}
  </s:Body>
</s:Envelope>
"""


def ews_envelope(body: str) -> bytes:
    indented = textwrap.indent(textwrap.dedent(body).strip(), "    ")
    return EWS_BODY_TEMPLATE.format(body=indented).encode("utf-8")

def ews_call(
    base_url: str,
    email: str,
    password: str,
    operation: str,
    body: str,
    insecure_tls: bool,
    timeout: int,
) -> str:
    response = request(
        "POST",
        join_url(base_url, "/EWS/Exchange.asmx"),
        ews_envelope(body),
        {
            "Authorization": basic_auth_header(email, password),
            "Content-Type": "text/xml; charset=utf-8",
            "Accept": "text/xml",
            "User-Agent": "lpe-rca-connectivity-check/0.1",
            "X-LPE-RCA-Operation": operation,
        },
        timeout,
        insecure_tls=insecure_tls,
    )
    require(response.status == 200, f"EWS {operation} returned HTTP {response.status}: {response.text[:500]}")
    require("xml" in content_type(response.headers), f"EWS {operation} did not return XML headers")
    return response.text

def require_ews_no_error(name: str, payload: str) -> None:
    require("<m:ResponseCode>NoError</m:ResponseCode>" in payload, f"{name} did not return EWS NoError: {payload[:800]}")

def extract_ews_item_id(payload: str, prefix: str, name: str) -> str:
    match = re.search(rf'Id="({re.escape(prefix)}[0-9a-fA-F-]{{36}})"', payload)
    require(match is not None, f"{name} did not return an {prefix} item id: {payload[:800]}")
    return match.group(1)
