---
type: Rust Function
title: sender_authorization_kind_from_str
resource: crates/lpe-storage/src/submission/types.rs#L368-L374
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_email_submissions
---

# Signature

`pub(crate) fn sender_authorization_kind_from_str(value: &str) -> SenderAuthorizationKind`

# Called by

- [fetch_jmap_email_submissions](../../../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_email_submissions.md)