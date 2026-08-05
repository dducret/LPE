---
type: Rust Function
title: source_key_for_mailbox_folder
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L281-L285
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_hierarchy_parent_mailbox_id
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/folder/imported_hierarchy_existing_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_preserves_nested_folder_parent_keys
---

# Signature

`pub(crate) fn source_key_for_mailbox_folder(mailbox: &JmapMailbox) -> Vec<u8>`

# Calls

- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [mapi_folder_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)

# Called by

- [imported_hierarchy_parent_mailbox_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_hierarchy_parent_mailbox_id.md)
- [mailbox_property_value_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [imported_hierarchy_existing_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/imported_hierarchy_existing_mailbox.md)
- [fast_transfer_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments.md)
- [mapi_over_http_hierarchy_sync_preserves_nested_folder_parent_keys](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_preserves_nested_folder_parent_keys.md)