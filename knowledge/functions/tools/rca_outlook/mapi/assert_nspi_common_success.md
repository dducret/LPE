---
type: Python Function
title: assert_nspi_common_success
resource: tools/rca_outlook/mapi.py#L164-L167
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/mapi/le_u32
  called_by:
  - functions/tools/rca_outlook/mapi/assert_nspi_resolve_names_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_get_matches_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_query_rows_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_get_props_payload
---

# Signature

`def assert_nspi_common_success(payload: bytes, request_type: str) -> None:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)
- [le_u32](../../../../functions/tools/rca_outlook/mapi/le_u32.md)

# Called by

- [assert_nspi_resolve_names_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_resolve_names_payload.md)
- [assert_nspi_get_matches_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_get_matches_payload.md)
- [assert_nspi_query_rows_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_query_rows_payload.md)
- [assert_nspi_get_props_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_get_props_payload.md)