---
type: Rust Function
title: folder_properties_for_open_from_mailboxes
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1259-L1394
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/collaboration_folder_handle_properties
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/search_folder_handle_properties
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version
  - functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_message_counts_for_folder
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value
  - functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_keeps_loaded_inbox_counts_and_mapi_name
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_search_folder_mail_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_persisted_search_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_collaboration_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/journal_getprops_flags_absent_web_view_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_getprops_flags_absent_retention_identity_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_getprops_flags_binary_acl_member_name_as_absent
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_public_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_im_contact_list_default_post_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/advertised_special_folder_counts_snapshot_messages_when_mailbox_not_loaded
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_reports_inbox_associated_content_count
---

# Signature

`pub(super) fn folder_properties_for_open_from_mailboxes( principal: &AccountPrincipal, folder_id: u64, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, ) -> HashMap<u32, MapiValue>`

# Calls

- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [mailbox_property_value_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [collaboration_folder_handle_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/collaboration_folder_handle_properties.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [public_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)
- [search_folder_definition_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id.md)
- [search_folder_handle_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/search_folder_handle_properties.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [folder_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_change_number.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [special_folder_property_value_with_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)
- [folder_version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version.md)
- [folder_version_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value.md)
- [snapshot_message_counts_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_message_counts_for_folder.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [special_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value.md)
- [folder_local_commit_time_max](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max.md)
- [associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)

# Called by

- [folder_properties_for_open_keeps_loaded_inbox_counts_and_mapi_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_keeps_loaded_inbox_counts_and_mapi_name.md)
- [folder_properties_for_open_projects_search_folder_mail_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_search_folder_mail_class.md)
- [folder_properties_for_open_projects_persisted_search_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_persisted_search_folder_contract.md)
- [folder_properties_for_open_projects_collaboration_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_collaboration_folder_contract.md)
- [journal_getprops_flags_absent_web_view_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/journal_getprops_flags_absent_web_view_properties.md)
- [inbox_getprops_flags_absent_retention_identity_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_getprops_flags_absent_retention_identity_properties.md)
- [inbox_getprops_flags_binary_acl_member_name_as_absent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_getprops_flags_binary_acl_member_name_as_absent.md)
- [folder_properties_for_open_projects_public_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_public_folder_contract.md)
- [folder_properties_for_open_projects_im_contact_list_default_post_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_projects_im_contact_list_default_post_class.md)
- [advertised_special_folder_counts_snapshot_messages_when_mailbox_not_loaded](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/advertised_special_folder_counts_snapshot_messages_when_mailbox_not_loaded.md)
- [folder_properties_for_open_reports_inbox_associated_content_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_reports_inbox_associated_content_count.md)