---
type: Rust Function
title: fast_transfer_message_content_buffer_with_special_object
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L233-L252
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions
  - functions/crates/lpe-exchange/src/mapi/sync/tests/appointment_fast_transfer_named_lid_includes_property_definition
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/outlook_fai_copyto_generates_a_mapiuid_search_key
---

# Signature

`pub(crate) fn fast_transfer_message_content_buffer_with_special_object( entry_id: Option<&[u8]>, parent_entry_id: Option<&[u8]>, object: &SpecialMessageSyncFact, send_options: u8, property_filter: FastTransferDirectPropertyFilter<'_>, message_children: FastTransferMessageChildren, ) -> Vec<u8>`

# Calls

- [write_fast_transfer_special_message_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)

# Called by

- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [calendar_fai_content_sync_preserves_imported_ics_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties.md)
- [associated_config_fai_content_sync_emits_valid_property_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions.md)
- [appointment_fast_transfer_named_lid_includes_property_definition](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/appointment_fast_transfer_named_lid_includes_property_definition.md)
- [microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root.md)
- [outlook_fai_copyto_generates_a_mapiuid_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/outlook_fai_copyto_generates_a_mapiuid_search_key.md)