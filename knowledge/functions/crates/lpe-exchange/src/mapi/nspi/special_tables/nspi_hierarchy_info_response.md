---
type: Rust Function
title: nspi_hierarchy_info_response
resource: crates/lpe-exchange/src/mapi/nspi/special_tables.rs#L76-L88
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
  - functions/crates/lpe-exchange/src/mapi/nspi/tests/get_hierarchy_info_returns_successful_address_book_hierarchy
---

# Signature

`pub(in crate::mapi) fn nspi_hierarchy_info_response( principal: &AccountPrincipal, request: &[u8], request_id: &str, ) -> Response`

# Calls

- [nspi_hierarchy_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response.md)

# Called by

- [handle_nspi_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)
- [get_hierarchy_info_returns_successful_address_book_hierarchy](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/tests/get_hierarchy_info_returns_successful_address_book_hierarchy.md)