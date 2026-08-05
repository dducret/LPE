---
type: Rust Function
title: smtp_command
resource: LPE-CT/src/smtp/protocol.rs#L30-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/protocol/expect_smtp
---

# Signature

`pub(in crate::smtp) async fn smtp_command( reader: &mut BufReader<OwnedReadHalf>, writer: &mut OwnedWriteHalf, command: &str, expected: u16, ) -> Result<()>`

# Calls

- [expect_smtp](../../../../../functions/LPE-CT/src/smtp/protocol/expect_smtp.md)