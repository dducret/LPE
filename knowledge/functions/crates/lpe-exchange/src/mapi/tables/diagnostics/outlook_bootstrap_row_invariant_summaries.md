---
type: Rust Function
title: outlook_bootstrap_row_invariant_summaries
resource: crates/lpe-exchange/src/mapi/tables/diagnostics.rs#L4-L175
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/state/selected_row_indexes
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_parent_id
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/classify_outlook_bootstrap_row_invariants
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_expected_container_class
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/debug_folder_row_property_value
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version
  - functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/common_views_message_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_config
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/inbox_contents_row_invariant_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_invariant_uses_mailbox_guid_entry_id
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_contents_invariant_accepts_message_identity_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/empty_common_views_has_no_row_identity_invariant
---

# Signature

`pub(in crate::mapi) fn outlook_bootstrap_row_invariant_summaries( object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, forward_read: bool, requested_row_count: usize, ) -> Vec<String>`

# Calls

- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)
- [selected_row_indexes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/selected_row_indexes.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [hierarchy_row_parent_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_parent_id.md)
- [classify_outlook_bootstrap_row_invariants](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/classify_outlook_bootstrap_row_invariants.md)
- [hierarchy_row_expected_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_expected_container_class.md)
- [debug_folder_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/debug_folder_row_property_value.md)
- [folder_version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version.md)
- [folder_version_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value.md)
- [hierarchy_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value.md)
- [associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [restriction_matches_common_views_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message.md)
- [sort_common_views_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages.md)
- [common_views_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/common_views_message_id.md)
- [common_views_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value.md)
- [associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows.md)
- [associated_table_row_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_config.md)
- [sort_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_associated_table_rows.md)
- [associated_table_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_id.md)
- [associated_table_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value.md)
- [emails_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)
- [restriction_matches_email_in_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [inbox_contents_row_invariant_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/inbox_contents_row_invariant_property_value.md)

# Called by

- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [inbox_associated_invariant_uses_mailbox_guid_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_invariant_uses_mailbox_guid_entry_id.md)
- [inbox_contents_invariant_accepts_message_identity_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_contents_invariant_accepts_message_identity_columns.md)
- [empty_common_views_has_no_row_identity_invariant](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/empty_common_views_has_no_row_identity_invariant.md)