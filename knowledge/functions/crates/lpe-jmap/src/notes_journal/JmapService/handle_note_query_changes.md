---
type: Rust Method
title: handle_note_query_changes
resource: crates/lpe-jmap/src/notes_journal.rs#L118-L150
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/notes_journal/note_matches_filter
  - functions/crates/lpe-jmap/src/state/query_changes_response
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_note_query_changes( &self, account: &lpe_storage::AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [note_matches_filter](../../../../../../functions/crates/lpe-jmap/src/notes_journal/note_matches_filter.md)
- [query_changes_response](../../../../../../functions/crates/lpe-jmap/src/state/query_changes_response.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)