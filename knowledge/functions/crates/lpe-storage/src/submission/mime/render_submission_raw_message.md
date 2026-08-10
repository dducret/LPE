---
type: Rust Function
title: render_submission_raw_message
resource: crates/lpe-storage/src/submission/mime.rs#L4-L51
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
  - functions/crates/lpe-storage/src/submission/mime/calendar_invitation_renders_calendar_mime_without_bcc
---

# Signature

`pub(super) fn render_submission_raw_message( from_address: &str, input: &SubmitMessageInput, body_text: &str, attachments: &[AttachmentUploadInput], ) -> String`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)
- [calendar_invitation_renders_calendar_mime_without_bcc](../../../../../../functions/crates/lpe-storage/src/submission/mime/calendar_invitation_renders_calendar_mime_without_bcc.md)