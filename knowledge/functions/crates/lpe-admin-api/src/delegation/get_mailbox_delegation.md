---
type: Rust Function
title: get_mailbox_delegation
resource: crates/lpe-admin-api/src/delegation.rs#L251-L275
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn get_mailbox_delegation( State(storage): State<Storage>, headers: HeaderMap, ) -> ApiResult<MailboxDelegationResponse>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)