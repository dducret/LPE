---
type: Rust Method
title: delete_selected_indices
resource: crates/lpe-imap/src/mailboxes.rs#L626-L656
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_close
  - functions/crates/lpe-imap/src/mailboxes/Session/expunge_selected_indices
---

# Signature

`async fn delete_selected_indices( &mut self, selected: &SelectedMailbox, indices: &[usize], ) -> Result<()>`

# Called by

- [handle_close](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_close.md)
- [expunge_selected_indices](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/expunge_selected_indices.md)