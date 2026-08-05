---
type: Rust Function
title: decode_globset_range_prefix
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L821-L929
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/globcnt_suffix_range
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replguid_set
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replid_set
---

# Signature

`fn decode_globset_range_prefix( value: &[u8], mut offset: usize, ) -> Result<(Vec<(u64, u64)>, usize), String>`

# Calls

- [global_counter_from_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [globcnt_suffix_range](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/globcnt_suffix_range.md)

# Called by

- [decode_replguid_set](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replguid_set.md)
- [decode_replid_set](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_replid_set.md)