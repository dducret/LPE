---
type: Rust Function
title: query_client_reminders
resource: crates/lpe-admin-api/src/workspace.rs#L1098-L1106
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/workspace/query_client_reminders_with_store
---

# Signature

`pub(crate) async fn query_client_reminders( State(storage): State<Storage>, headers: HeaderMap, Query(request): Query<ReminderQueryRequest>, ) -> ApiResult<Vec<ClientReminder>>`

# Calls

- [query_client_reminders_with_store](../../../../../functions/crates/lpe-admin-api/src/workspace/query_client_reminders_with_store.md)