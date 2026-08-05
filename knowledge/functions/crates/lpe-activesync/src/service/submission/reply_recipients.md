---
type: Rust Function
title: reply_recipients
resource: crates/lpe-activesync/src/service/submission.rs#L291-L314
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`fn reply_recipients( principal_email: &str, source_message: &lpe_storage::JmapEmail, ) -> Vec<SubmittedRecipientInput>`

# Called by

- [handle_smart_compose](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)