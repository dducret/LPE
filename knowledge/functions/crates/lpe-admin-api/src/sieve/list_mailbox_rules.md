---
type: Rust Function
title: list_mailbox_rules
resource: crates/lpe-admin-api/src/sieve.rs#L17-L27
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn list_mailbox_rules( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<Vec<MailboxRule>>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)