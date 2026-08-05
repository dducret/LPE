---
type: Rust Module
title: request_reader
resource: crates/lpe-exchange/src/mapi/rop/request_reader.rs#L1-L1471
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-parse-tagged-property-rop-id-is-reserved-write-u16-prefixed-bytes-write-utf16z-cursor-roprequest
  - external/crate-mapi-properties-write-mapi-value
  - external/crate-mapi-wire-ropid
  - external/anyhow-anyhow-result
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [read_rop_request](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)
- [read_rop_request_with_logon_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)

# Imports

- `super::{
    parse_tagged_property, rop_id_is_reserved, write_u16_prefixed_bytes, write_utf16z, Cursor,
    RopRequest,
}`
- `crate::mapi::properties::write_mapi_value`
- `crate::mapi::wire::RopId`
- `anyhow::{anyhow, Result}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)