---
type: Rust Function
title: parse_state
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L678-L757
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replguid_set
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/required_state_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_standalone_state
---

# Signature

`fn parse_state( bytes: &[u8], start: usize, sync_type: u8, label: &str, ) -> Result<(SyncStateSets, usize), String>`

# Calls

- [parse_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_property.md)
- [decode_replguid_set](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replguid_set.md)
- [required_state_value](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/required_state_value.md)

# Called by

- [parse_manifest](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest.md)
- [parse_standalone_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_standalone_state.md)