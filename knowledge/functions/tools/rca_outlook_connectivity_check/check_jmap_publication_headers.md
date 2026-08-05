---
type: Python Function
title: check_jmap_publication_headers
resource: tools/rca_outlook_connectivity_check.py#L311-L347
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/url_host
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_jmap_publication_headers( base_url: str, expected_service_host: str | None, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [url_host](../../../functions/tools/rca_outlook/http/url_host.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)