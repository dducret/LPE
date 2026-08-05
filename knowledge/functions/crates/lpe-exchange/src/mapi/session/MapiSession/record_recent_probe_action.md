---
type: Rust Method
title: record_recent_probe_action
resource: crates/lpe-exchange/src/mapi/session.rs#L644-L651
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(in crate::mapi) fn record_recent_probe_action(&mut self, action: String)`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_save_changes_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)
- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [append_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)
- [append_read_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response.md)
- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)