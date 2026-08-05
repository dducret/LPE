---
type: Rust Function
title: read_data
resource: LPE-CT/src/submission.rs#L602-L627
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/submission/max_message_size_bytes
  called_by:
  - functions/LPE-CT/src/submission/handle_submission_session
---

# Signature

`async fn read_data<R>(reader: &mut R) -> Result<Vec<u8>> where R: AsyncBufRead + Unpin,`

# Calls

- [max_message_size_bytes](../../../../functions/LPE-CT/src/submission/max_message_size_bytes.md)

# Called by

- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)