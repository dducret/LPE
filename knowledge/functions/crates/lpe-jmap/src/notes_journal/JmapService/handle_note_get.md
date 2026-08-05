---
type: Rust Method
title: handle_note_get
resource: crates/lpe-jmap/src/notes_journal.rs#L26-L62
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/notes_journal/note_properties
  - functions/crates/lpe-jmap/src/parse/parse_uuid_list
  - functions/crates/lpe-jmap/src/notes_journal/note_to_value
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_note_get( &self, account: &lpe_storage::AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [note_properties](../../../../../../functions/crates/lpe-jmap/src/notes_journal/note_properties.md)
- [parse_uuid_list](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid_list.md)
- [note_to_value](../../../../../../functions/crates/lpe-jmap/src/notes_journal/note_to_value.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)