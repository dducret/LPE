---
type: Rust Function
title: append_save_changes_attachment_response
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L715-L975
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_flags_are_supported
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_upload
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/pending_embedded_message_attachment_upload
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/mapi_expected_attachment_kind
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_event_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values_from_map
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response
---

# Signature

`pub(super) async fn append_save_changes_attachment_response<S, V>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, validator: &Validator<V>, responses: &mut Vec<u8>, ) where S: ExchangeStore, V: Detector,`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [save_flags_are_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_flags_are_supported.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [folder_access_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)
- [pending_attachment_upload](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_upload.md)
- [pending_embedded_message_attachment_upload](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/pending_embedded_message_attachment_upload.md)
- [mapi_expected_attachment_kind](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/mapi_expected_attachment_kind.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [mapi_event_custom_property_values_from_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_event_custom_property_values_from_map.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)
- [upsert_custom_property_values_from_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/upsert_custom_property_values_from_map.md)

# Called by

- [append_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_attachment_response.md)