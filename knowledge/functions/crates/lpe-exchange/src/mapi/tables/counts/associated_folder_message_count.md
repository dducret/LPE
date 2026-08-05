---
type: Rust Function
title: associated_folder_message_count
resource: crates/lpe-exchange/src/mapi/tables/counts.rs#L136-L167
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/has_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/counts/contents_table_open_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object
---

# Signature

`pub(in crate::mapi) fn associated_folder_message_count( folder_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> u32`

# Calls

- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [conversation_action_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_table_messages.md)
- [delegate_freebusy_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages.md)
- [has_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/has_associated_table_rows.md)
- [associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)

# Called by

- [log_outlook_contents_table_find_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row.md)
- [folder_properties_for_open_from_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [serialize_session_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [contents_table_open_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/contents_table_open_row_count.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [serialize_hierarchy_row_from_backing_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object.md)