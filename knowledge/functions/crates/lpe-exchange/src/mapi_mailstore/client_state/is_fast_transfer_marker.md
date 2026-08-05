---
type: Rust Function
title: is_fast_transfer_marker
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1268-L1293
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_change
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property
---

# Signature

`fn is_fast_transfer_marker(tag: u32) -> bool`

# Called by

- [parse_change](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_change.md)
- [parse_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property.md)