---
type: Rust Method
title: expunge_selected_indices
resource: crates/lpe-imap/src/mailboxes.rs#L605-L624
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/Session/delete_selected_indices
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_expunge
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge
---

# Signature

`async fn expunge_selected_indices<W>( &mut self, selected: &SelectedMailbox, indices: &[usize], writer: &mut W, ) -> Result<()> where W: AsyncWriteExt + Unpin,`

# Calls

- [delete_selected_indices](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/delete_selected_indices.md)

# Called by

- [handle_expunge](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_expunge.md)
- [handle_uid_expunge](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge.md)