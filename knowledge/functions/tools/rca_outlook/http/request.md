---
type: Python Function
title: request
resource: tools/rca_outlook/http.py#L36-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/test_rca_outlook_trace_summary/FakePath/open
---

# Signature

`def request( method: str, url: str, body: bytes | None = None, headers: dict[str, str] | None = None, timeout: int = 20, read_limit: int | None = None, insecure_tls: bool = False, follow_redirects: bool = True, ) -> HttpResponse:`

# Calls

- [open](../../../../functions/tools/test_rca_outlook_trace_summary/FakePath/open.md)