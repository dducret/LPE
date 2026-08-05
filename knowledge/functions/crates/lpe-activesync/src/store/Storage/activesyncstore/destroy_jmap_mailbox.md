---
type: Rust Method
title: destroy_jmap_mailbox
resource: crates/lpe-activesync/src/store.rs#L261-L271
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn destroy_jmap_mailbox<'a>( &'a self, account_id: Uuid, mailbox_id: Uuid, audit: AuditEntryInput, ) -> StoreFuture<'a, ()>`