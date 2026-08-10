---
type: Rust Function
title: restriction_matches
resource: crates/lpe-exchange/src/mapi/properties.rs#L407-L467
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text
  - functions/crates/lpe-exchange/src/mapi/properties/content_restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/size
  - functions/crates/lpe-exchange/src/mapi/properties/compare_i64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_mailbox_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_collaboration_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_public_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_note
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_journal_entry
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/restriction_matches_event_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_hierarchy_row_matches
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/restriction_matches_public_folder_item
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(in crate::mapi) fn restriction_matches( restriction: Option<&MapiRestriction>, value_for: impl Copy + Fn(u32) -> Option<MapiValue>, ) -> bool`

# Calls

- [into_text](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text.md)
- [content_restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/content_restriction_matches.md)
- [compare_mapi_values](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values.md)
- [into_u32](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_u32.md)
- [size](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/size.md)
- [compare_i64](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_i64.md)

# Called by

- [restriction_matches_mailbox_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_mailbox_with_context_for_account.md)
- [restriction_matches_collaboration_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_collaboration_folder.md)
- [restriction_matches_public_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_public_folder.md)
- [restriction_matches_email_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)
- [restriction_matches_contact_in_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder.md)
- [restriction_matches_task](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task.md)
- [restriction_matches_note](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_note.md)
- [restriction_matches_journal_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_journal_entry.md)
- [restriction_matches_attachment](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_attachment.md)
- [restriction_matches_navigation_shortcut](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_navigation_shortcut.md)
- [restriction_matches_common_view_named_view](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view.md)
- [restriction_matches_associated_config](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config.md)
- [rop_find_row_response](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [restriction_matches_common_views_message](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message.md)
- [restriction_matches_event_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/restriction_matches_event_with_mailbox_guid.md)
- [restricted_associated_folder_message_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [special_hierarchy_row_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_hierarchy_row_matches.md)
- [restriction_matches_public_folder_item](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/restriction_matches_public_folder_item.md)
- [rop_query_rows_response_inner](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)