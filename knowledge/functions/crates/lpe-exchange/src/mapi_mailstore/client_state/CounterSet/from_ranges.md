---
type: Rust Method
title: from_ranges
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L167-L186
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/union_with
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replguid_set
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replid_set
---

# Signature

`fn from_ranges(mut ranges: Vec<(u64, u64)>) -> Result<Self, String>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [union_with](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/union_with.md)
- [decode_replguid_set](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replguid_set.md)
- [decode_replid_set](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replid_set.md)