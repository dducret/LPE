---
type: Rust Method
title: handle_note_set
resource: crates/lpe-jmap/src/notes_journal.rs#L170-L252
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  - functions/crates/lpe-jmap/src/notes_journal/parse_note_input
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_import_or_copy
---

# Signature

`pub(crate) async fn handle_note_set( &self, account: &lpe_storage::AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)
- [parse_note_input](../../../../../../functions/crates/lpe-jmap/src/notes_journal/parse_note_input.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [handle_canonical_import_or_copy](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_import_or_copy.md)