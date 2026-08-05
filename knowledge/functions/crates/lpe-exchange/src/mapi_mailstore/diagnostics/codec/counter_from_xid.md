---
type: Rust Function
title: counter_from_xid
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L1289-L1294
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder
---

# Signature

`pub(super) fn counter_from_xid(value: &[u8]) -> Option<u64>`

# Calls

- [current_store_replica_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [finish_hierarchy_debug_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder.md)