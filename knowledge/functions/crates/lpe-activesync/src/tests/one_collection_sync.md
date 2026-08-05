---
type: Rust Function
title: one_collection_sync
resource: crates/lpe-activesync/src/tests.rs#L2625-L2634
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/tests/base64_sync_request_dispatches
  - functions/crates/lpe-activesync/src/tests/move_items_moves_message_between_canonical_mail_folders
  - functions/crates/lpe-activesync/src/tests/sync_delete_moves_message_to_trash_by_default
  - functions/crates/lpe-activesync/src/tests/sync_change_updates_read_state_and_round_trips
  - functions/crates/lpe-activesync/src/tests/sync_change_updates_followup_flag_state
  - functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime
  - functions/crates/lpe-activesync/src/tests/sync_projects_email_followup_flag_state
---

# Signature

`fn one_collection_sync(collection_id: &str, sync_key: &str) -> WbxmlNode`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [base64_sync_request_dispatches](../../../../../functions/crates/lpe-activesync/src/tests/base64_sync_request_dispatches.md)
- [move_items_moves_message_between_canonical_mail_folders](../../../../../functions/crates/lpe-activesync/src/tests/move_items_moves_message_between_canonical_mail_folders.md)
- [sync_delete_moves_message_to_trash_by_default](../../../../../functions/crates/lpe-activesync/src/tests/sync_delete_moves_message_to_trash_by_default.md)
- [sync_change_updates_read_state_and_round_trips](../../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_read_state_and_round_trips.md)
- [sync_change_updates_followup_flag_state](../../../../../functions/crates/lpe-activesync/src/tests/sync_change_updates_followup_flag_state.md)
- [sync_respects_body_preference_for_html_text_and_mime](../../../../../functions/crates/lpe-activesync/src/tests/sync_respects_body_preference_for_html_text_and_mime.md)
- [sync_projects_email_followup_flag_state](../../../../../functions/crates/lpe-activesync/src/tests/sync_projects_email_followup_flag_state.md)