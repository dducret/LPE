---
type: Rust Function
title: read_response_with_timeout
resource: crates/lpe-imap/src/tests.rs#L4052-L4080
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/tests/idle_reports_selected_mailbox_flag_changes
  - functions/crates/lpe-imap/src/tests/idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change
  - functions/crates/lpe-imap/src/tests/read_response
---

# Signature

`async fn read_response_with_timeout( stream: &mut TcpStream, tag: Option<&str>, timeout_ms: u64, ) -> String`

# Called by

- [idle_reports_selected_mailbox_flag_changes](../../../../../functions/crates/lpe-imap/src/tests/idle_reports_selected_mailbox_flag_changes.md)
- [idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change](../../../../../functions/crates/lpe-imap/src/tests/idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)