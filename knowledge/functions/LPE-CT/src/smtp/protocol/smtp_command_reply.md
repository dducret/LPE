---
type: Rust Function
title: smtp_command_reply
resource: LPE-CT/src/smtp/protocol.rs#L41-L49
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/protocol/read_smtp_reply
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients
---

# Signature

`pub(in crate::smtp) async fn smtp_command_reply( reader: &mut BufReader<OwnedReadHalf>, writer: &mut OwnedWriteHalf, command: &str, ) -> Result<SmtpReply>`

# Calls

- [read_smtp_reply](../../../../../functions/LPE-CT/src/smtp/protocol/read_smtp_reply.md)

# Called by

- [relay_message_to_target_for_recipients](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients.md)