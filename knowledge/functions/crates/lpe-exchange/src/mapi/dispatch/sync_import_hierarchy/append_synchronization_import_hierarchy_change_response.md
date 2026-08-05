---
type: Rust Function
title: append_synchronization_import_hierarchy_change_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy.rs#L4-L357
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_hierarchy_values
  - functions/crates/lpe-exchange/src/mapi/properties/folder/hierarchy_display_name
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/imported_hierarchy_version
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/folder_version_for_snapshot
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/upsert_folder_version
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_hierarchy_change_with_change_number
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/persistable_import_source_key_global_counter
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_special_folder_aliases
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_special_folder_alias
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/properties/folder/system_folder_display_name
  - functions/crates/lpe-exchange/src/mapi/properties/folder/imported_hierarchy_existing_mailbox
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_hierarchy_parent_mailbox_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response
---

# Signature

`pub(super) async fn append_synchronization_import_hierarchy_change_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], snapshot: &mut MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [import_hierarchy_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_hierarchy_values.md)
- [hierarchy_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/hierarchy_display_name.md)
- [imported_hierarchy_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/imported_hierarchy_version.md)
- [resolve_special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [advertised_special_folder_id_for_create](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create.md)
- [identity_codec](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec.md)
- [actual_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)
- [commit_mapi_folder_hierarchy_change](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_folder_hierarchy_change.md)
- [folder_version_for_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/folder_version_for_snapshot.md)
- [upsert_folder_version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/upsert_folder_version.md)
- [record_sync_upload_hierarchy_change_with_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_hierarchy_change_with_change_number.md)
- [rop_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_import_hierarchy_change_response.md)
- [persistable_import_source_key_global_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/persistable_import_source_key_global_counter.md)
- [upsert_mapi_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_special_folder_aliases.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [record_special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_special_folder_alias.md)
- [sync_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)
- [system_folder_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/system_folder_display_name.md)
- [imported_hierarchy_existing_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/imported_hierarchy_existing_mailbox.md)
- [remember_created_mapi_identity_record](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record.md)
- [imported_hierarchy_parent_mailbox_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_hierarchy_parent_mailbox_id.md)

# Called by

- [append_sync_import_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response.md)