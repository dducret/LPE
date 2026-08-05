---
type: Rust Function
title: parse_predecessor_change_list_entries
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L748-L767
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_message_change_conflicts_with_current_pcl
---

# Signature

`fn parse_predecessor_change_list_entries( bytes: &[u8], ) -> Result<Vec<PredecessorChangeListEntry>, ()>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [import_message_change_conflicts_with_current_pcl](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_message_change_conflicts_with_current_pcl.md)