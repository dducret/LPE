---
type: Rust Function
title: resolve_names_columns
resource: crates/lpe-exchange/src/mapi/nspi.rs#L294-L298
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_columns
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
---

# Signature

`pub(in crate::mapi) fn resolve_names_columns(request: &[u8]) -> Vec<u32>`

# Calls

- [parse_resolve_names_columns](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_columns.md)

# Called by

- [resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)