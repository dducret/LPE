---
type: Python Function
title: check_json_autodiscover
resource: tools/rca_outlook_connectivity_check.py#L262-L308
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/require
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook/http/url_host
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_json_autodiscover( base_url: str, email: str, expect_ews: bool, expect_mapi: bool, expected_service_host: str | None, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [url_host](../../../functions/tools/rca_outlook/http/url_host.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)