---
type: Python Function
title: nspi_first_minimal_id
resource: tools/rca_outlook/mapi.py#L151-L155
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/mapi/assert_nspi_get_matches_payload
  - functions/tools/rca_outlook/mapi/le_u32
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book
---

# Signature

`def nspi_first_minimal_id(payload: bytes, request_type: str) -> int:`

# Calls

- [assert_nspi_get_matches_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_get_matches_payload.md)
- [le_u32](../../../../functions/tools/rca_outlook/mapi/le_u32.md)
- [require](../../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [check_mapi_nspi_address_book](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book.md)