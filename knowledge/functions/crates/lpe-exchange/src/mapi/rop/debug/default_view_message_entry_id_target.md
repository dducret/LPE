---
type: Rust Function
title: default_view_message_entry_id_target
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L956-L971
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_default_view_entry_id_decoding
---

# Signature

`pub(in crate::mapi) fn default_view_message_entry_id_target(entry_id: &[u8]) -> Option<(u64, u64)>`

# Calls

- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [format_common_view_descriptor_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)
- [format_default_view_entry_id_decoding](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_default_view_entry_id_decoding.md)