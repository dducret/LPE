---
type: Rust Function
title: submission_authorization_kind_sql
resource: crates/lpe-storage/src/submission/types.rs#L380-L386
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(super) fn submission_authorization_kind_sql(kind: SenderAuthorizationKind) -> &'static str`

# Called by

- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)