---
type: Rust Function
title: virtual_special_mailbox_id
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L303-L305
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_default_contacts_folder_properties_use_persisted_change_number
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_open_folder_defers_property_projection_until_getprops
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_folder_extended_flags_survive_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage
---

# Signature

`pub(crate) fn virtual_special_mailbox_id(folder_id: u64) -> Uuid`

# Called by

- [virtual_special_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [mapi_folder_identity_requests](../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests.md)
- [mapi_over_http_default_contacts_folder_properties_use_persisted_change_number](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_default_contacts_folder_properties_use_persisted_change_number.md)
- [mapi_over_http_open_folder_defers_property_projection_until_getprops](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_open_folder_defers_property_projection_until_getprops.md)
- [mapi_over_http_folder_extended_flags_survive_reconnect](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_folder_extended_flags_survive_reconnect.md)
- [mapi_identity_repair_removes_orphaned_checkpoint_and_config_state](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state.md)
- [postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage](../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_folder_hierarchy_commit_keeps_durable_trash_version_lineage.md)