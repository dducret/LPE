---
type: Rust Method
title: fetch_outgoing_mailbox_delegation_grants
resource: crates/lpe-imap/src/store.rs#L338-L346
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_outgoing_mailbox_delegation_grants<'a>( &'a self, owner_account_id: Uuid, ) -> StoreFuture<'a, Vec<MailboxDelegationGrant>>`