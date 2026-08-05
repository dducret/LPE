---
type: Rust Function
title: nspi_special_table_response
resource: crates/lpe-exchange/src/mapi/nspi/special_tables.rs#L62-L74
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) fn nspi_special_table_response( principal: &AccountPrincipal, request: &[u8], request_id: &str, ) -> Response`

# Calls

- [nspi_hierarchy_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response.md)

# Called by

- [handle_nspi_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)