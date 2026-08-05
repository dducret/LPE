---
type: Rust Module
title: receive_folders
resource: crates/lpe-exchange/src/mapi/rop/receive_folders.rs#L1-L118
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-write-object-id-write-u32-roprequest
  - external/crate-mapi-identity-calendar-folder-id-inbox-folder-id
  - external/crate-mapi-properties-write-mapi-value-mapivalue-pid-tag-folder-id-pid-tag-last-modification-time-pid-tag-message-class-string8
  - external/crate-mapi-tables-write-standard-property-row
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rop_get_receive_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_response.md)
- [valid_receive_folder_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/valid_receive_folder_message_class.md)
- [ReceiveFolderEntry](../../../../../../classes/crates/lpe-exchange/src/mapi/rop/receive_folders/ReceiveFolderEntry.md)
- [receive_folder_entry_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_matches.md)
- [receive_folder_entry_for_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_for_message_class.md)
- [explicit_receive_folder_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/explicit_receive_folder_message_class.md)
- [receive_folder_id_for_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_id_for_message_class.md)
- [rop_get_receive_folder_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/rop_get_receive_folder_table_response.md)

# Imports

- `super::{write_object_id, write_u32, RopRequest}`
- `crate::mapi::identity::{CALENDAR_FOLDER_ID, INBOX_FOLDER_ID}`
- `crate::mapi::properties::{
    write_mapi_value, MapiValue, PID_TAG_FOLDER_ID, PID_TAG_LAST_MODIFICATION_TIME,
    PID_TAG_MESSAGE_CLASS_STRING8,
}`
- `crate::mapi::tables::write_standard_property_row`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)