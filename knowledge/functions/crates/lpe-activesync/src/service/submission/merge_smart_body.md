---
type: Rust Function
title: merge_smart_body
resource: crates/lpe-activesync/src/service/submission.rs#L333-L348
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`fn merge_smart_body(command_name: &str, composed: &str, original: &str) -> String`

# Called by

- [handle_smart_compose](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)