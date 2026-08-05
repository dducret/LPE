---
type: Rust Function
title: query_client_reminders_with_store
resource: crates/lpe-admin-api/src/workspace.rs#L1262-L1277
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/require_account_from_store
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/query_client_reminders
  - functions/crates/lpe-admin-api/src/workspace/tests/reminder_api_helper_preserves_include_inactive_query
---

# Signature

`async fn query_client_reminders_with_store<S: ClientOutlookStore>( storage: &S, headers: &HeaderMap, request: ReminderQueryRequest, ) -> std::result::Result<Vec<ClientReminder>, (StatusCode, String)>`

# Calls

- [require_account_from_store](../../../../../functions/crates/lpe-admin-api/src/workspace/require_account_from_store.md)

# Called by

- [query_client_reminders](../../../../../functions/crates/lpe-admin-api/src/workspace/query_client_reminders.md)
- [reminder_api_helper_preserves_include_inactive_query](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/reminder_api_helper_preserves_include_inactive_query.md)