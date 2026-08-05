---
type: Rust Function
title: assert_variable_property
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L4212-L4221
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_variable_strings_with_fast_transfer_lengths
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/direct_fast_transfer_uses_persisted_normal_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_calendar_includes_account_scoped_entry_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_inbox_includes_calendar_identification_entry_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_string8_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/final_sync_state_separates_object_idset_from_change_cnset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_state_keeps_normal_and_fai_cnsets_separate
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers
---

# Signature

`fn assert_variable_property(buffer: &[u8], property_tag: u32, value: &[u8])`

# Calls

- [position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [sync_manifest_serializes_variable_strings_with_fast_transfer_lengths](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_variable_strings_with_fast_transfer_lengths.md)
- [sync_manifest_serializes_content_message_header_in_fixed_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order.md)
- [content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync.md)
- [microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties.md)
- [fast_transfer_copy_properties_filters_message_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties.md)
- [direct_fast_transfer_uses_persisted_normal_message_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/direct_fast_transfer_uses_persisted_normal_message_identity_properties.md)
- [hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset.md)
- [hierarchy_download_no_deletions_keeps_missing_id_without_tombstone](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone.md)
- [hierarchy_download_emits_explicit_tombstone_absent_from_client_idset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset.md)
- [hierarchy_transfer_calendar_includes_account_scoped_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_calendar_includes_account_scoped_entry_id.md)
- [hierarchy_transfer_inbox_includes_calendar_identification_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_inbox_includes_calendar_identification_entry_id.md)
- [hierarchy_transfer_respects_default_post_message_class_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_exclusion.md)
- [hierarchy_transfer_respects_default_post_message_class_string8_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_string8_exclusion.md)
- [fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given.md)
- [normal_message_no_foreign_identifiers_uses_local_source_key_for_selection](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection.md)
- [content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag.md)
- [final_sync_state_separates_object_idset_from_change_cnset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/final_sync_state_separates_object_idset_from_change_cnset.md)
- [scoped_final_sync_state_uses_the_durable_inbox_counter](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter.md)
- [hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes.md)
- [content_sync_state_keeps_normal_and_fai_cnsets_separate](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_state_keeps_normal_and_fai_cnsets_separate.md)
- [special_message_headers_and_final_cnsets_share_durable_change_numbers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers.md)