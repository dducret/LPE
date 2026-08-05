---
type: Rust Method
title: import_property_values
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L381-L393
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_message_id
---

# Signature

`pub(in crate::mapi) fn import_property_values(&self) -> Result<Vec<(u32, MapiValue)>>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_u16](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_tagged_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property.md)

# Called by

- [append_synchronization_import_message_change_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
- [import_message_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_message_id.md)