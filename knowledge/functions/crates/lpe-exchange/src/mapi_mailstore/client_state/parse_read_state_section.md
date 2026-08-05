---
type: Rust Function
title: parse_read_state_section
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L1014-L1047
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_change_boundary
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replid_set
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/intersection
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest
---

# Signature

`fn parse_read_state_section( bytes: &[u8], start: usize, ) -> Result<(CounterSet, CounterSet, usize), String>`

# Calls

- [is_change_boundary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/is_change_boundary.md)
- [parse_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property.md)
- [decode_replid_set](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replid_set.md)
- [intersection](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/intersection.md)

# Called by

- [parse_manifest](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest.md)