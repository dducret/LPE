---
type: Rust Function
title: fake_grantee_account_id
resource: crates/lpe-imap/src/tests.rs#L4005-L4011
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/upsert_mailbox_delegation_grant
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/upsert_sender_delegation_grant
---

# Signature

`fn fake_grantee_account_id(email: &str) -> Uuid`

# Called by

- [upsert_mailbox_delegation_grant](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/upsert_mailbox_delegation_grant.md)
- [upsert_sender_delegation_grant](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/upsert_sender_delegation_grant.md)