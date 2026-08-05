---
type: Rust Function
title: imported_fai_identity
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_message.rs#L4-L40
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_contact_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(super) fn imported_fai_identity( properties: &HashMap<u32, MapiValue>, imported_message_id: u64, ) -> Result<MapiFaiImportedIdentity>`

# Calls

- [imported_message_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [imported_contact_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_contact_identity.md)
- [current_common_views_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)