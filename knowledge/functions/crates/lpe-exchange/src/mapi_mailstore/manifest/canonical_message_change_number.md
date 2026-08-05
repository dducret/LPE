---
type: Rust Function
title: canonical_message_change_number
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L213-L215
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_read_state/append_synchronization_import_read_state_changes_response
  - functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/rss_email_named_property_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/message_change_number_excludes_bcc_recipients
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/message_change_number_tracks_per_folder_membership_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/canonical_change_numbers_fit_mapi_globcnt
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/unchanged_object_keeps_source_key_and_changed_object_advances_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/canonical_message_change_number_uses_membership_modseq_without_bcc_leakage
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_cached_mode_properties_include_canonical_change_keys
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state
---

# Signature

`pub(crate) fn canonical_message_change_number(email: &JmapEmail) -> u64`

# Calls

- [canonical_message_change_number_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments.md)

# Called by

- [append_synchronization_import_deletes_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [append_synchronization_import_read_state_changes_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_read_state/append_synchronization_import_read_state_changes_response.md)
- [email_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/email_property_value.md)
- [rss_email_named_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/rss_email_named_property_value.md)
- [message_change_number_excludes_bcc_recipients](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/message_change_number_excludes_bcc_recipients.md)
- [message_change_number_tracks_per_folder_membership_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/message_change_number_tracks_per_folder_membership_state.md)
- [canonical_change_numbers_fit_mapi_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/canonical_change_numbers_fit_mapi_globcnt.md)
- [unchanged_object_keeps_source_key_and_changed_object_advances_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/unchanged_object_keeps_source_key_and_changed_object_advances_change_number.md)
- [canonical_message_change_number_uses_membership_modseq_without_bcc_leakage](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/canonical_message_change_number_uses_membership_modseq_without_bcc_leakage.md)
- [mapi_over_http_cached_mode_properties_include_canonical_change_keys](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_cached_mode_properties_include_canonical_change_keys.md)
- [mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc.md)
- [mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts.md)
- [mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints.md)
- [mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state.md)
- [mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state.md)