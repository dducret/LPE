---
type: Rust Method
title: next_modseq
resource: crates/lpe-imap/src/tests.rs#L173-L177
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/update_imap_flags
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/copy_imap_email
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/move_imap_email
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/save_draft_message
  - functions/crates/lpe-imap/src/tests/FakeStore/imapstore/import_imap_email
  - functions/crates/lpe-imap/src/tests/condstore_store_reports_modified_and_keeps_fresh_messages
  - functions/crates/lpe-imap/src/tests/noop_and_check_emit_selected_mailbox_refresh_updates
  - functions/crates/lpe-imap/src/tests/reconnect_select_refreshes_from_canonical_mailbox_state
  - functions/crates/lpe-imap/src/tests/idle_reports_selected_mailbox_flag_changes
  - functions/crates/lpe-imap/src/tests/idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change
---

# Signature

`fn next_modseq(&self) -> u64`

# Called by

- [update_imap_flags](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/update_imap_flags.md)
- [copy_imap_email](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/copy_imap_email.md)
- [move_imap_email](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/move_imap_email.md)
- [save_draft_message](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/save_draft_message.md)
- [import_imap_email](../../../../../../functions/crates/lpe-imap/src/tests/FakeStore/imapstore/import_imap_email.md)
- [condstore_store_reports_modified_and_keeps_fresh_messages](../../../../../../functions/crates/lpe-imap/src/tests/condstore_store_reports_modified_and_keeps_fresh_messages.md)
- [noop_and_check_emit_selected_mailbox_refresh_updates](../../../../../../functions/crates/lpe-imap/src/tests/noop_and_check_emit_selected_mailbox_refresh_updates.md)
- [reconnect_select_refreshes_from_canonical_mailbox_state](../../../../../../functions/crates/lpe-imap/src/tests/reconnect_select_refreshes_from_canonical_mailbox_state.md)
- [idle_reports_selected_mailbox_flag_changes](../../../../../../functions/crates/lpe-imap/src/tests/idle_reports_selected_mailbox_flag_changes.md)
- [idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change](../../../../../../functions/crates/lpe-imap/src/tests/idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change.md)