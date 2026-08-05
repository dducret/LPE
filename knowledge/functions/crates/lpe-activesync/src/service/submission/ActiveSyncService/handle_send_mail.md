---
type: Rust Method
title: handle_send_mail
resource: crates/lpe-activesync/src/service/submission.rs#L17-L107
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/response/is_message_rfc822
  - functions/crates/lpe-activesync/src/wbxml/decode_wbxml
  - functions/crates/lpe-activesync/src/service/command_status_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/message/parse_mime_message
  - functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_from_address
  - functions/crates/lpe-activesync/src/message/default_sender
  - functions/crates/lpe-activesync/src/response/empty_response
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn handle_send_mail( &self, principal: &AuthenticatedPrincipal, protocol_version: &str, headers: &HeaderMap, body: &[u8], ) -> Result<Response>`

# Calls

- [is_message_rfc822](../../../../../../../functions/crates/lpe-activesync/src/response/is_message_rfc822.md)
- [decode_wbxml](../../../../../../../functions/crates/lpe-activesync/src/wbxml/decode_wbxml.md)
- [command_status_response](../../../../../../../functions/crates/lpe-activesync/src/service/command_status_response.md)
- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [parse_mime_message](../../../../../../../functions/crates/lpe-activesync/src/message/parse_mime_message.md)
- [validate_mime_attachments](../../../../../../../functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments.md)
- [mailbox_access_for_from_address](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_from_address.md)
- [default_sender](../../../../../../../functions/crates/lpe-activesync/src/message/default_sender.md)
- [empty_response](../../../../../../../functions/crates/lpe-activesync/src/response/empty_response.md)
- [wbxml_response](../../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)