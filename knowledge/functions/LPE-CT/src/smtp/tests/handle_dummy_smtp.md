---
type: Rust Function
title: handle_dummy_smtp
resource: LPE-CT/src/smtp/tests.rs#L3407-L3457
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile
---

# Signature

`async fn handle_dummy_smtp(stream: TcpStream, profile: DummySmtpProfile)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [spawn_dummy_smtp_with_profile](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile.md)