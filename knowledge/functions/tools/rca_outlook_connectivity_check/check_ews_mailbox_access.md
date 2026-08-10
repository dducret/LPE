---
type: Python Function
title: check_ews_mailbox_access
resource: tools/rca_outlook_connectivity_check.py#L691-L709
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/ews/ews_call
  - functions/tools/rca_outlook/ews/require_ews_no_error
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_ews_mailbox_access(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> None:`

# Calls

- [ews_call](../../../functions/tools/rca_outlook/ews/ews_call.md)
- [require_ews_no_error](../../../functions/tools/rca_outlook/ews/require_ews_no_error.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)