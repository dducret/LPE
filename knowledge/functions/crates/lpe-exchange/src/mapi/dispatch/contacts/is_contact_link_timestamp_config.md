---
type: Rust Function
title: is_contact_link_timestamp_config
resource: crates/lpe-exchange/src/mapi/dispatch/contacts.rs#L13-L16
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contacts/mapi_folder_is_outlook_contacts_surface
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`pub(super) fn is_contact_link_timestamp_config(folder_id: u64, message_class: &str) -> bool`

# Calls

- [mapi_folder_is_outlook_contacts_surface](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contacts/mapi_folder_is_outlook_contacts_surface.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)