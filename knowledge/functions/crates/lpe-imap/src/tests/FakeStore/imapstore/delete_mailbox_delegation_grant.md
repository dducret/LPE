---
type: Rust Method
title: delete_mailbox_delegation_grant
resource: crates/lpe-imap/src/tests.rs#L901-L915
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn delete_mailbox_delegation_grant<'a>( &'a self, owner_account_id: Uuid, grantee_account_id: Uuid, _audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`