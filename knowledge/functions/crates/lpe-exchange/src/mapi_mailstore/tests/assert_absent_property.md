---
type: Rust Function
title: assert_absent_property
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L4256-L4259
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_variable_strings_with_fast_transfer_lengths
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/direct_fast_transfer_uses_persisted_normal_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_entry_id_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_string8_exclusion
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag
---

# Signature

`fn assert_absent_property(buffer: &[u8], property_tag: u32)`

# Called by

- [sync_manifest_serializes_variable_strings_with_fast_transfer_lengths](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_variable_strings_with_fast_transfer_lengths.md)
- [microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties.md)
- [fast_transfer_copy_properties_filters_message_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties.md)
- [direct_fast_transfer_uses_persisted_normal_message_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/direct_fast_transfer_uses_persisted_normal_message_identity_properties.md)
- [hierarchy_download_no_deletions_keeps_missing_id_without_tombstone](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone.md)
- [hierarchy_transfer_respects_entry_id_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_entry_id_exclusion.md)
- [hierarchy_transfer_respects_default_post_message_class_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_exclusion.md)
- [hierarchy_transfer_respects_default_post_message_class_string8_exclusion](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_string8_exclusion.md)
- [content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag.md)