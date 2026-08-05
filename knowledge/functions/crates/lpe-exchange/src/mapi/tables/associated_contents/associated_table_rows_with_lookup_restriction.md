---
type: Rust Function
title: associated_table_rows_with_lookup_restriction
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L202-L232
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_inbox_exact_rule_organizer_restriction
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_visible_in_table
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_for_find_row
---

# Signature

`fn associated_table_rows_with_lookup_restriction( folder_id: u64, snapshot: &MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, find_row_restriction: Option<&MapiRestriction>, _mailbox_guid: Uuid, ) -> Vec<AssociatedTableRow>`

# Calls

- [associated_config_messages_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder.md)
- [is_inbox_exact_rule_organizer_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_inbox_exact_rule_organizer_restriction.md)
- [outlook_inbox_exact_virtual_associated_config_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_message_class.md)
- [restriction_matches_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config.md)
- [associated_config_visible_in_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_visible_in_table.md)

# Called by

- [associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows.md)
- [associated_table_rows_for_find_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_for_find_row.md)