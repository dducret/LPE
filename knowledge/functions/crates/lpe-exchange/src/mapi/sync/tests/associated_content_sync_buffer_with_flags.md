---
type: Rust Function
title: associated_content_sync_buffer_with_flags
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L77-L104
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer
  - functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_no_foreign_identifiers_uses_local_source_key
---

# Signature

`fn associated_content_sync_buffer_with_flags( account_id: Uuid, folder_id: u64, sync_flags: u16, objects: &[mapi_mailstore::SpecialMessageSyncFact], ) -> Vec<u8>`

# Calls

- [sync_manifest_buffer_with_special_objects_and_final_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state.md)

# Called by

- [associated_content_sync_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer.md)
- [outlook_inbox_fai_ics_omits_unsupported_message_identity_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties.md)
- [associated_config_fai_no_foreign_identifiers_uses_local_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_no_foreign_identifiers_uses_local_source_key.md)