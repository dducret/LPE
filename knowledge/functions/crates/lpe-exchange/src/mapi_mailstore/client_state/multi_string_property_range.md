---
type: Rust Function
title: multi_string_property_range
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1224-L1239
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property
---

# Signature

`fn multi_string_property_range( bytes: &[u8], value_start: usize, ) -> Result<(usize, usize, usize), String>`

# Called by

- [parse_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property.md)