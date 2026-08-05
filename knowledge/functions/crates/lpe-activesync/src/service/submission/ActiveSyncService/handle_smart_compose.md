---
type: Rust Method
title: handle_smart_compose
resource: crates/lpe-activesync/src/service/submission.rs#L109-L231
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/command_status_response
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/resolve_source_message
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments
  - functions/crates/lpe-activesync/src/message/parse_mime_message
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_from_address
  - functions/crates/lpe-activesync/src/service/submission/reply_recipients
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/load_message_attachment_uploads
  - functions/crates/lpe-activesync/src/service/submission/default_reply_subject
  - functions/crates/lpe-activesync/src/service/submission/merge_smart_body
  - functions/crates/lpe-activesync/src/message/default_sender
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn handle_smart_compose( &self, principal: &AuthenticatedPrincipal, protocol_version: &str, request: &WbxmlNode, command: ActiveSyncCommand, ) -> Result<Response>`

# Calls

- [command_status_response](../../../../../../../functions/crates/lpe-activesync/src/service/command_status_response.md)
- [resolve_source_message](../../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/resolve_source_message.md)
- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [validate_mime_attachments](../../../../../../../functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments.md)
- [parse_mime_message](../../../../../../../functions/crates/lpe-activesync/src/message/parse_mime_message.md)
- [mailbox_access_for_from_address](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_from_address.md)
- [reply_recipients](../../../../../../../functions/crates/lpe-activesync/src/service/submission/reply_recipients.md)
- [load_message_attachment_uploads](../../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/load_message_attachment_uploads.md)
- [default_reply_subject](../../../../../../../functions/crates/lpe-activesync/src/service/submission/default_reply_subject.md)
- [merge_smart_body](../../../../../../../functions/crates/lpe-activesync/src/service/submission/merge_smart_body.md)
- [default_sender](../../../../../../../functions/crates/lpe-activesync/src/message/default_sender.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)