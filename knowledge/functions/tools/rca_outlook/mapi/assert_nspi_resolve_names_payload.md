---
type: Python Function
title: assert_nspi_resolve_names_payload
resource: tools/rca_outlook/mapi.py#L261-L267
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/mapi/assert_nspi_common_success
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/mapi/le_u32
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox
---

# Signature

`def assert_nspi_resolve_names_payload(payload: bytes, request_type: str) -> None:`

# Calls

- [assert_nspi_common_success](../../../../functions/tools/rca_outlook/mapi/assert_nspi_common_success.md)
- [require](../../../../functions/tools/rca_outlook/http/require.md)
- [le_u32](../../../../functions/tools/rca_outlook/mapi/le_u32.md)

# Called by

- [check_mapi_nspi_resolve_authenticated_mailbox](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_resolve_authenticated_mailbox.md)