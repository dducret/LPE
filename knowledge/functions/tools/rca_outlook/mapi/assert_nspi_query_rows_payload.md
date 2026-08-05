---
type: Python Function
title: assert_nspi_query_rows_payload
resource: tools/rca_outlook/mapi.py#L185-L189
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/mapi/assert_nspi_common_success
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/mapi/le_u32
---

# Signature

`def assert_nspi_query_rows_payload(payload: bytes, request_type: str) -> None:`

# Calls

- [assert_nspi_common_success](../../../../functions/tools/rca_outlook/mapi/assert_nspi_common_success.md)
- [require](../../../../functions/tools/rca_outlook/http/require.md)
- [le_u32](../../../../functions/tools/rca_outlook/mapi/le_u32.md)