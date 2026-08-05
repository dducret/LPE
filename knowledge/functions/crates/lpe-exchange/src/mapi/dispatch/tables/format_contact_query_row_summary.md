---
type: Rust Function
title: format_contact_query_row_summary
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L721-L794
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_search_results
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_contacts
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_mapi_contact_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
---

# Signature

`fn format_contact_query_row_summary( folder_id: u64, position: usize, forward_read: bool, row_count: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, columns: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [contacts_search_results](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_search_results.md)
- [contacts_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder.md)
- [restriction_matches_contact_in_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder.md)
- [sort_contacts](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_contacts.md)
- [select_query_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)
- [serialize_mapi_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_mapi_contact_row.md)
- [standard_property_row_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes.md)
- [contact_property_value_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)
- [format_debug_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_mapi_value.md)

# Called by

- [format_normal_message_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)