---
type: Rust Function
title: parse_nspi_get_prop_list_request
resource: crates/lpe-exchange/src/mapi/nspi.rs#L453-L461
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response
---

# Signature

`fn parse_nspi_get_prop_list_request(request: &[u8]) -> Option<NspiGetPropListRequest>`

# Calls

- [read_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [nspi_get_prop_list_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_get_prop_list_response.md)