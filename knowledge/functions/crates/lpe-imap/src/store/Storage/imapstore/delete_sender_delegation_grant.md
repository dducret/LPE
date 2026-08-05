---
type: Rust Method
title: delete_sender_delegation_grant
resource: crates/lpe-imap/src/store.rs#L386-L402
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn delete_sender_delegation_grant<'a>( &'a self, owner_account_id: Uuid, grantee_account_id: Uuid, sender_right: SenderDelegationRight, audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`