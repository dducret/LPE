---
type: Rust Function
title: is_message_rfc822
resource: crates/lpe-activesync/src/response.rs#L106-L112
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
---

# Signature

`pub(crate) fn is_message_rfc822(headers: &HeaderMap) -> bool`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_send_mail](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)