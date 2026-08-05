---
type: Rust Function
title: globcnt_suffix_range
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L931-L942
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_globset_range_prefix
---

# Signature

`fn globcnt_suffix_range(prefix: &[u8], low: u8, high: u8) -> Result<(u64, u64), String>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [global_counter_from_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [decode_globset_range_prefix](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_globset_range_prefix.md)