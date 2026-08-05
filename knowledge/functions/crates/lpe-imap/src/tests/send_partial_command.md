---
type: Rust Function
title: send_partial_command
resource: crates/lpe-imap/src/tests.rs#L4013-L4017
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/tests/read_response
  called_by:
  - functions/crates/lpe-imap/src/tests/login_list_select_fetch_store_search_and_append_work
  - functions/crates/lpe-imap/src/tests/utf8_accept_enables_utf8_mailbox_response_quoting
  - functions/crates/lpe-imap/src/tests/malformed_utf8_command_literals_are_rejected_before_mailbox_validation
  - functions/crates/lpe-imap/src/tests/append_message_literals_remain_byte_oriented
  - functions/crates/lpe-imap/src/tests/thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy
  - functions/crates/lpe-imap/src/tests/append_copy_move_and_expunge_preserve_canonical_uid_state
  - functions/crates/lpe-imap/src/tests/idle_reports_selected_mailbox_flag_changes
  - functions/crates/lpe-imap/src/tests/idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change
  - functions/crates/lpe-imap/src/tests/idle_without_selected_mailbox_is_noop_for_outlook
  - functions/crates/lpe-imap/src/tests/plain_authenticate_challenge_response_is_accepted
  - functions/crates/lpe-imap/src/tests/login_authenticate_challenge_response_is_accepted
  - functions/crates/lpe-imap/src/tests/login_authenticate_with_initial_username_is_accepted
  - functions/crates/lpe-imap/src/tests/legacy_auth_login_alias_is_accepted
  - functions/crates/lpe-imap/src/tests/login_accepts_username_and_password_literals
  - functions/crates/lpe-imap/src/tests/authenticate_login_accepts_initial_username_literal
---

# Signature

`async fn send_partial_command(stream: &mut TcpStream, value: &str) -> String`

# Calls

- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)

# Called by

- [login_list_select_fetch_store_search_and_append_work](../../../../../functions/crates/lpe-imap/src/tests/login_list_select_fetch_store_search_and_append_work.md)
- [utf8_accept_enables_utf8_mailbox_response_quoting](../../../../../functions/crates/lpe-imap/src/tests/utf8_accept_enables_utf8_mailbox_response_quoting.md)
- [malformed_utf8_command_literals_are_rejected_before_mailbox_validation](../../../../../functions/crates/lpe-imap/src/tests/malformed_utf8_command_literals_are_rejected_before_mailbox_validation.md)
- [append_message_literals_remain_byte_oriented](../../../../../functions/crates/lpe-imap/src/tests/append_message_literals_remain_byte_oriented.md)
- [thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy](../../../../../functions/crates/lpe-imap/src/tests/thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy.md)
- [append_copy_move_and_expunge_preserve_canonical_uid_state](../../../../../functions/crates/lpe-imap/src/tests/append_copy_move_and_expunge_preserve_canonical_uid_state.md)
- [idle_reports_selected_mailbox_flag_changes](../../../../../functions/crates/lpe-imap/src/tests/idle_reports_selected_mailbox_flag_changes.md)
- [idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change](../../../../../functions/crates/lpe-imap/src/tests/idle_reports_replacement_when_selected_mailbox_membership_changes_without_count_change.md)
- [idle_without_selected_mailbox_is_noop_for_outlook](../../../../../functions/crates/lpe-imap/src/tests/idle_without_selected_mailbox_is_noop_for_outlook.md)
- [plain_authenticate_challenge_response_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/plain_authenticate_challenge_response_is_accepted.md)
- [login_authenticate_challenge_response_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/login_authenticate_challenge_response_is_accepted.md)
- [login_authenticate_with_initial_username_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/login_authenticate_with_initial_username_is_accepted.md)
- [legacy_auth_login_alias_is_accepted](../../../../../functions/crates/lpe-imap/src/tests/legacy_auth_login_alias_is_accepted.md)
- [login_accepts_username_and_password_literals](../../../../../functions/crates/lpe-imap/src/tests/login_accepts_username_and_password_literals.md)
- [authenticate_login_accepts_initial_username_literal](../../../../../functions/crates/lpe-imap/src/tests/authenticate_login_accepts_initial_username_literal.md)