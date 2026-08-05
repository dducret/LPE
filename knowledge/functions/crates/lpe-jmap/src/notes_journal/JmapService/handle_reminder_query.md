---
type: Rust Method
title: handle_reminder_query
resource: crates/lpe-jmap/src/notes_journal.rs#L506-L579
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/state/query_position
  - functions/crates/lpe-jmap/src/state/encode_query_state_reference
  - functions/crates/lpe-jmap/src/state/encode_query_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_reminder_query( &self, account: &lpe_storage::AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [query_position](../../../../../../functions/crates/lpe-jmap/src/state/query_position.md)
- [encode_query_state_reference](../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state_reference.md)
- [encode_query_state](../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)