---
type: Rust Function
title: read_smtp_reply
resource: LPE-CT/src/smtp/protocol.rs#L63-L87
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients
  - functions/LPE-CT/src/smtp/protocol/smtp_command_reply
  - functions/LPE-CT/src/smtp/protocol/expect_smtp
---

# Signature

`pub(in crate::smtp) async fn read_smtp_reply( reader: &mut BufReader<OwnedReadHalf>, ) -> Result<SmtpReply>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [relay_message_to_target_for_recipients](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients.md)
- [smtp_command_reply](../../../../../functions/LPE-CT/src/smtp/protocol/smtp_command_reply.md)
- [expect_smtp](../../../../../functions/LPE-CT/src/smtp/protocol/expect_smtp.md)