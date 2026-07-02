from __future__ import annotations

import base64
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass
class HttpResponse:
    status: int
    headers: dict[str, str]
    body: bytes
    set_cookies: list[str]

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None

def request(
    method: str,
    url: str,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 20,
    read_limit: int | None = None,
    insecure_tls: bool = False,
    follow_redirects: bool = True,
) -> HttpResponse:
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    handlers: list[Any] = []
    if insecure_tls and urllib.parse.urlparse(url).scheme == "https":
        handlers.append(urllib.request.HTTPSHandler(context=ssl._create_unverified_context()))
    if not follow_redirects:
        handlers.append(NoRedirectHandler)
    try:
        if handlers:
            opener = urllib.request.build_opener(*handlers)
            response_context = opener.open(req, timeout=timeout)
        else:
            response_context = urllib.request.urlopen(req, timeout=timeout)
        with response_context as resp:
            response_body = resp.read(read_limit) if read_limit is not None else resp.read()
            return HttpResponse(
                resp.status,
                dict(resp.headers.items()),
                response_body,
                resp.headers.get_all("Set-Cookie") or [],
            )
    except urllib.error.HTTPError as error:
        response_body = error.read(read_limit) if read_limit is not None else error.read()
        return HttpResponse(
            error.code,
            dict(error.headers.items()),
            response_body,
            error.headers.get_all("Set-Cookie") or [],
        )

def join_url(base_url: str, path: str) -> str:
    if urllib.parse.urlparse(path).scheme:
        return path
    return base_url.rstrip("/") + "/" + path.lstrip("/")

def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)

def content_type(headers: dict[str, str]) -> str:
    for key, value in headers.items():
        if key.lower() == "content-type":
            return value.lower()
    return ""

def header_value(headers: dict[str, str], name: str) -> str:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return ""

def cookie_header(response: HttpResponse) -> str:
    cookies = response.set_cookies
    if not cookies:
        value = header_value(response.headers, "set-cookie")
        cookies = [value] if value else []
    return "; ".join(cookie.split(";", 1)[0] for cookie in cookies if cookie)

def update_cookie_header(current: str, response: HttpResponse) -> str:
    replacement = cookie_header(response)
    if not replacement:
        return current
    cookies: dict[str, str] = {}
    for value in current.split(";"):
        name, separator, cookie_value = value.strip().partition("=")
        if separator:
            cookies[name] = cookie_value
    for value in replacement.split(";"):
        name, separator, cookie_value = value.strip().partition("=")
        if separator:
            cookies[name] = cookie_value
    return "; ".join(f"{name}={value}" for name, value in cookies.items())

def require_guid_counter_header(value: str, label: str) -> None:
    require(
        re.fullmatch(r"\{[0-9A-Fa-f-]{36}\}:[0-9]+", value) is not None,
        f"{label} was not a {{GUID}}:counter value: {value!r}",
    )

def url_host(value: str) -> str:
    parsed = urllib.parse.urlparse(value)
    return parsed.hostname or ""

def basic_auth_header(email: str, password: str) -> str:
    token = base64.b64encode(f"{email}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"
