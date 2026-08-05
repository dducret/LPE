---
type: Rust Function
title: associated_config_named_property_tags
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L858-L874
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
---

# Signature

`pub(in crate::mapi) fn associated_config_named_property_tags( message: &MapiAssociatedConfigMessage, ) -> Vec<u32>`

# Calls

- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [associated_config_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [rop_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)