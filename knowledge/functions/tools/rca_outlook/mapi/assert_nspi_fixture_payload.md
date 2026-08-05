---
type: Python Function
title: assert_nspi_fixture_payload
resource: tools/rca_outlook/mapi.py#L198-L206
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book
---

# Signature

`def assert_nspi_fixture_payload(payload: bytes, request_type: str, expected_name: str, expected_email: str) -> None:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [check_mapi_nspi_address_book](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book.md)