---
type: Rust Function
title: current_mapi_identity_codec
resource: crates/lpe-exchange/src/mapi/identity.rs#L43-L45
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid
  - functions/crates/lpe-exchange/src/mapi/identity/durable_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_trailing_replid_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/wire_id_bytes_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/long_term_id_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id
  - functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/outlook_message_list_settings_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/public_folder_entry_id_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_identifier_bytes
  - functions/crates/lpe-exchange/src/mapi/identity/message_entry_id_from_object_ids
  - functions/crates/lpe-exchange/src/mapi/identity/object_ids_from_message_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/source_key_for_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_source_key
  - functions/crates/lpe-exchange/src/mapi/identity/change_key_for_change_number
  - functions/crates/lpe-exchange/src/mapi/identity/instance_key_for_object_id
---

# Signature

`fn current_mapi_identity_codec<T>(mapper: impl FnOnce(&MapiIdentityCodec) -> T) -> Option<T>`

# Called by

- [current_store_replica_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_store_replica_guid.md)
- [durable_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/durable_object_id.md)
- [object_id_from_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_wire_id.md)
- [object_id_from_trailing_replid_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_trailing_replid_wire_id.md)
- [wire_id_bytes_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/wire_id_bytes_from_object_id.md)
- [long_term_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/long_term_id_from_object_id.md)
- [object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id.md)
- [folder_entry_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_from_object_id.md)
- [outlook_message_list_settings_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/outlook_message_list_settings_entry_id.md)
- [public_folder_entry_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/public_folder_entry_id_from_object_id.md)
- [object_id_from_folder_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_entry_id.md)
- [object_id_from_folder_identifier_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_identifier_bytes.md)
- [message_entry_id_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/message_entry_id_from_object_ids.md)
- [object_ids_from_message_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_ids_from_message_entry_id.md)
- [source_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/source_key_for_object_id.md)
- [object_id_from_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_source_key.md)
- [change_key_for_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/change_key_for_change_number.md)
- [instance_key_for_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/instance_key_for_object_id.md)