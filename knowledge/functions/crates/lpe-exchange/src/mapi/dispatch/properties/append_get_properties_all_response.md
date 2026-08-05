---
type: Rust Function
title: append_get_properties_all_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L451-L494
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_identity_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response
---

# Signature

`pub(super) async fn append_get_properties_all_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [default_folder_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags.md)
- [default_folder_identity_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_identity_property_tags.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [attachment_overlay_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object.md)
- [rop_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)

# Called by

- [append_property_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response.md)