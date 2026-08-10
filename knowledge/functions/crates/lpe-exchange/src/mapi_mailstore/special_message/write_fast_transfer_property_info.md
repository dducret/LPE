---
type: Rust Function
title: write_fast_transfer_property_info
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L489-L533
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/named/fast_transfer_named_property_for_message_tag
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property
---

# Signature

`fn write_fast_transfer_property_info( buffer: &mut Vec<u8>, object: &SpecialMessageSyncFact, property_tag: u32, ) -> bool`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [fast_transfer_named_property_for_message_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/fast_transfer_named_property_for_message_tag.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [write_special_message_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property.md)