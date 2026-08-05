---
type: Rust Function
title: content_length_matches_body
resource: crates/lpe-exchange/src/mapi/transport/headers.rs#L132-L137
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
---

# Signature

`pub(in crate::mapi) fn content_length_matches_body(value: &str, body: &[u8]) -> bool`

# Called by

- [handle_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)