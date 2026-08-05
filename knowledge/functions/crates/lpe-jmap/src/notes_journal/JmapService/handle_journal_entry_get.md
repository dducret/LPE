---
type: Rust Method
title: handle_journal_entry_get
resource: crates/lpe-jmap/src/notes_journal.rs#L254-L292
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/notes_journal/journal_entry_properties
  - functions/crates/lpe-jmap/src/parse/parse_uuid_list
  - functions/crates/lpe-jmap/src/notes_journal/journal_entry_to_value
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_journal_entry_get( &self, account: &lpe_storage::AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [journal_entry_properties](../../../../../../functions/crates/lpe-jmap/src/notes_journal/journal_entry_properties.md)
- [parse_uuid_list](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid_list.md)
- [journal_entry_to_value](../../../../../../functions/crates/lpe-jmap/src/notes_journal/journal_entry_to_value.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)