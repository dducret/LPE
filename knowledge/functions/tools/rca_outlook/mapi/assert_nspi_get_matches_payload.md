---
type: Python Function
title: assert_nspi_get_matches_payload
resource: tools/rca_outlook/mapi.py#L269-L275
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/mapi/assert_nspi_common_success
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/mapi/le_u32
  called_by:
  - functions/tools/rca_outlook/mapi/nspi_first_minimal_id
---

# Signature

`def assert_nspi_get_matches_payload(payload: bytes, request_type: str) -> None:`

# Calls

- [assert_nspi_common_success](../../../../functions/tools/rca_outlook/mapi/assert_nspi_common_success.md)
- [require](../../../../functions/tools/rca_outlook/http/require.md)
- [le_u32](../../../../functions/tools/rca_outlook/mapi/le_u32.md)

# Called by

- [nspi_first_minimal_id](../../../../functions/tools/rca_outlook/mapi/nspi_first_minimal_id.md)