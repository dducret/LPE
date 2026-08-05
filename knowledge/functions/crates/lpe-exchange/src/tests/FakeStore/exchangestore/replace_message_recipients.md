---
type: Rust Method
title: replace_message_recipients
resource: crates/lpe-exchange/src/tests/mod.rs#L11646-L11681
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn replace_message_recipients<'a>( &'a self, _account_id: Uuid, message_id: Uuid, to: &'a [SubmittedRecipientInput], cc: &'a [SubmittedRecipientInput], bcc: &'a [SubmittedRecipientInput], _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, ()>`