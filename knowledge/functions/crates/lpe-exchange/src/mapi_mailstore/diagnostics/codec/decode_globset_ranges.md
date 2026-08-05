---
type: Rust Function
title: decode_globset_ranges
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L1156-L1270
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/globcnt_slice_to_u64
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters
---

# Signature

`pub(super) fn decode_globset_ranges( value: &[u8], mut offset: usize, ) -> Result<Vec<(u64, u64)>, String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [globcnt_slice_to_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/globcnt_slice_to_u64.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [format_replguid_globset_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/format_replguid_globset_debug.md)
- [replguid_globset_counters](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/replguid_globset_counters.md)