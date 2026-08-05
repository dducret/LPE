---
type: Rust Function
title: expect_smtp
resource: LPE-CT/src/smtp/protocol.rs#L51-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/protocol/read_smtp_reply
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients
  - functions/LPE-CT/src/smtp/protocol/smtp_command
---

# Signature

`pub(in crate::smtp) async fn expect_smtp( reader: &mut BufReader<OwnedReadHalf>, expected: u16, ) -> Result<()>`

# Calls

- [read_smtp_reply](../../../../../functions/LPE-CT/src/smtp/protocol/read_smtp_reply.md)

# Called by

- [relay_message_to_target_for_recipients](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients.md)
- [smtp_command](../../../../../functions/LPE-CT/src/smtp/protocol/smtp_command.md)