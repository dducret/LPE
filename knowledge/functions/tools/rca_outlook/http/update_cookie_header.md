---
type: Python Function
title: update_cookie_header
resource: tools/rca_outlook/http.py#L103-L116
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/cookie_header
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book
---

# Signature

`def update_cookie_header(current: str, response: HttpResponse) -> str:`

# Calls

- [cookie_header](../../../../functions/tools/rca_outlook/http/cookie_header.md)

# Called by

- [check_mapi_nspi_address_book](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_nspi_address_book.md)