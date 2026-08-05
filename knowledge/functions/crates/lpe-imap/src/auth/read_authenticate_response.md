---
type: Rust Function
title: read_authenticate_response
resource: crates/lpe-imap/src/auth.rs#L142-L156
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/auth/Session/handle_authenticate
---

# Signature

`async fn read_authenticate_response<R>(reader: &mut BufReader<R>, context: &str) -> Result<String> where R: AsyncReadExt + Unpin,`

# Called by

- [handle_authenticate](../../../../../functions/crates/lpe-imap/src/auth/Session/handle_authenticate.md)