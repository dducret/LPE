---
type: Rust Function
title: assert_contact_folder_associated_find_row_does_not_return_config
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8598-L8611
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contact_folder_associated_find_row_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_does_not_invent_osc_contact_sync_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_does_not_invent_contact_link_timestamp_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/suggested_contacts_associated_find_row_does_not_return_empty_osc_contact_sync_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/quick_contacts_associated_find_row_does_not_invent_osc_contact_sync_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/im_contact_list_associated_find_row_does_not_invent_osc_contact_sync_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/mailbox_backed_quick_contacts_associated_find_row_does_not_invent_osc_contact_sync_config
---

# Signature

`fn assert_contact_folder_associated_find_row_does_not_return_config( folder_id: u64, message_class: &str, snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [contact_folder_associated_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contact_folder_associated_find_row_response.md)

# Called by

- [contacts_associated_find_row_does_not_invent_osc_contact_sync_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_does_not_invent_osc_contact_sync_config.md)
- [contacts_associated_find_row_does_not_invent_contact_link_timestamp_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_does_not_invent_contact_link_timestamp_config.md)
- [suggested_contacts_associated_find_row_does_not_return_empty_osc_contact_sync_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/suggested_contacts_associated_find_row_does_not_return_empty_osc_contact_sync_config.md)
- [quick_contacts_associated_find_row_does_not_invent_osc_contact_sync_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/quick_contacts_associated_find_row_does_not_invent_osc_contact_sync_config.md)
- [im_contact_list_associated_find_row_does_not_invent_osc_contact_sync_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/im_contact_list_associated_find_row_does_not_invent_osc_contact_sync_config.md)
- [dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config.md)
- [mailbox_backed_quick_contacts_associated_find_row_does_not_invent_osc_contact_sync_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/mailbox_backed_quick_contacts_associated_find_row_does_not_invent_osc_contact_sync_config.md)