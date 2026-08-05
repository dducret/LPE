---
type: Rust Method
title: handle_journal_entry_query
resource: crates/lpe-jmap/src/notes_journal.rs#L294-L353
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/notes_journal/journal_entry_matches_filter
  - functions/crates/lpe-jmap/src/state/query_position
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_journal_entry_query( &self, account: &lpe_storage::AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [journal_entry_matches_filter](../../../../../../functions/crates/lpe-jmap/src/notes_journal/journal_entry_matches_filter.md)
- [query_position](../../../../../../functions/crates/lpe-jmap/src/state/query_position.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)