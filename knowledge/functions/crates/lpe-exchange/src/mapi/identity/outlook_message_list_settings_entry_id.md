---
type: Rust Function
title: outlook_message_list_settings_entry_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L973-L981
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_outlook_message_list_settings_entry_id
---

# Signature

`pub(crate) fn outlook_message_list_settings_entry_id( mailbox_guid: Uuid, object_id: u64, ) -> Option<Vec<u8>>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_outlook_message_list_settings_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_outlook_message_list_settings_entry_id.md)