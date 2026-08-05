---
type: Rust Function
title: default_reply_subject
resource: crates/lpe-activesync/src/service/submission.rs#L316-L331
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`fn default_reply_subject(command_name: &str, original_subject: &str) -> String`

# Called by

- [handle_smart_compose](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)