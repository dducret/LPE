---
type: Rust Function
title: raw_outlook_message_list_settings_entry_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L764-L769
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_with_provider_and_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/outlook_message_list_settings_entry_id
---

# Signature

`fn raw_outlook_message_list_settings_entry_id( mailbox_guid: Uuid, object_id: u64, ) -> Option<Vec<u8>>`

# Calls

- [folder_entry_id_with_provider_and_type](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_with_provider_and_type.md)

# Called by

- [outlook_message_list_settings_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/outlook_message_list_settings_entry_id.md)