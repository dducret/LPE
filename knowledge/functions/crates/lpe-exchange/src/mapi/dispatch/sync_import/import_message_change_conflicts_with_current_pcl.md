---
type: Rust Function
title: import_message_change_conflicts_with_current_pcl
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L718-L741
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/parse_predecessor_change_list_entries
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(super) fn import_message_change_conflicts_with_current_pcl( properties: &[(u32, MapiValue)], current_change_number: u64, ) -> bool`

# Calls

- [parse_predecessor_change_list_entries](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/parse_predecessor_change_list_entries.md)
- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)