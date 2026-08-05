---
type: Python Function
title: url_host
resource: tools/rca_outlook/http.py#L124-L126
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_json_autodiscover
  - functions/tools/rca_outlook_connectivity_check/check_jmap_publication_headers
---

# Signature

`def url_host(value: str) -> str:`

# Called by

- [check_json_autodiscover](../../../../functions/tools/rca_outlook_connectivity_check/check_json_autodiscover.md)
- [check_jmap_publication_headers](../../../../functions/tools/rca_outlook_connectivity_check/check_jmap_publication_headers.md)