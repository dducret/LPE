---
type: Python Function
title: le_u32
resource: tools/rca_outlook/mapi.py#L147-L149
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook/mapi/mapi_execute_response_rops
  - functions/tools/rca_outlook/mapi/nspi_first_minimal_id
  - functions/tools/rca_outlook/mapi/assert_nspi_common_success
  - functions/tools/rca_outlook/mapi/assert_nspi_resolve_names_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_get_matches_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_query_rows_payload
  - functions/tools/rca_outlook/mapi/assert_nspi_get_props_payload
---

# Signature

`def le_u32(payload: bytes, offset: int) -> int:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [mapi_execute_response_rops](../../../../functions/tools/rca_outlook/mapi/mapi_execute_response_rops.md)
- [nspi_first_minimal_id](../../../../functions/tools/rca_outlook/mapi/nspi_first_minimal_id.md)
- [assert_nspi_common_success](../../../../functions/tools/rca_outlook/mapi/assert_nspi_common_success.md)
- [assert_nspi_resolve_names_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_resolve_names_payload.md)
- [assert_nspi_get_matches_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_get_matches_payload.md)
- [assert_nspi_query_rows_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_query_rows_payload.md)
- [assert_nspi_get_props_payload](../../../../functions/tools/rca_outlook/mapi/assert_nspi_get_props_payload.md)