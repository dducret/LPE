---
type: Rust Method
title: fetch_imap_mailbox_state
resource: crates/lpe-imap/src/tests.rs#L289-L310
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn fetch_imap_mailbox_state<'a>( &'a self, _account_id: Uuid, mailbox_id: Uuid, ) -> StoreFuture<'a, ImapMailboxState>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)