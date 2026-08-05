---
type: Rust Function
title: folder_entry_id_with_provider_and_type
resource: crates/lpe-exchange/src/mapi/identity.rs#L779-L793
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/raw_outlook_message_list_settings_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_with_provider
---

# Signature

`fn folder_entry_id_with_provider_and_type( provider_uid: [u8; 16], object_id: u64, entry_type: u16, ) -> Option<Vec<u8>>`

# Calls

- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [raw_outlook_message_list_settings_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_outlook_message_list_settings_entry_id.md)
- [folder_entry_id_with_provider](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_with_provider.md)