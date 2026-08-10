---
type: Python Function
title: check_pox_autodiscover
resource: tools/rca_outlook_connectivity_check.py#L405-L508
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/content_type
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_pox_autodiscover( base_url: str, email: str, expect_ews: bool, expect_exch_provider: bool, expect_expr_provider: bool, expect_mapi: bool, expected_service_host: str | None, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)