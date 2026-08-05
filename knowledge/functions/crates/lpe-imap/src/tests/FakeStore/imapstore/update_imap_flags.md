---
type: Rust Method
title: update_imap_flags
resource: crates/lpe-imap/src/tests.rs#L329-L379
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/tests/FakeStore/next_modseq
  - functions/crates/lpe-imap/src/tests/FakeStore/apply_post_flag_update_action
---

# Signature

`fn update_imap_flags<'a>( &'a self, _account_id: Uuid, mailbox_id: Uuid, message_ids: &'a [Uuid], unread: Option<bool>, flagged: Option<bool>, deleted: Option<bool>, unchanged_since: Option<u64>, ) -> StoreFuture<'a, Vec<Uuid>>`

# Calls

- [next_modseq](../../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/next_modseq.md)
- [apply_post_flag_update_action](../../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/apply_post_flag_update_action.md)