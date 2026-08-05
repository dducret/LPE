---
type: Rust Method
title: record_special_folder_alias
resource: crates/lpe-exchange/src/mapi/session.rs#L959-L964
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/replace_special_folder_aliases
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_resolves_learned_special_folder_aliases
---

# Signature

`pub(in crate::mapi) fn record_special_folder_alias(&mut self, alias_id: u64, folder_id: u64)`

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [replace_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/replace_special_folder_aliases.md)
- [access_plan_resolves_learned_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_resolves_learned_special_folder_aliases.md)