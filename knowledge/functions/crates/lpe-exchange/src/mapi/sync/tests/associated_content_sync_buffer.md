---
type: Rust Function
title: associated_content_sync_buffer
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L69-L75
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer_with_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai
  - functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync
  - functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions
---

# Signature

`fn associated_content_sync_buffer( account_id: Uuid, folder_id: u64, objects: &[mapi_mailstore::SpecialMessageSyncFact], ) -> Vec<u8>`

# Calls

- [associated_content_sync_buffer_with_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer_with_flags.md)

# Called by

- [common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts.md)
- [inbox_fai_fasttransfer_boundaries_export_only_persisted_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai.md)
- [empty_persisted_inbox_named_view_is_exported_by_fai_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync.md)
- [calendar_fai_content_sync_preserves_imported_ics_identity_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties.md)
- [associated_config_fai_content_sync_emits_valid_property_definitions](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions.md)