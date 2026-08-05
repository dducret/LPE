---
type: Rust Function
title: sender_identity_id
resource: crates/lpe-storage/src/submission/types.rs#L376-L378
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_email_submissions
  - functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_identities
---

# Signature

`pub(crate) fn sender_identity_id(kind: SenderAuthorizationKind, owner_account_id: Uuid) -> String`

# Called by

- [fetch_jmap_email_submissions](../../../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_email_submissions.md)
- [fetch_sender_identities](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_identities.md)