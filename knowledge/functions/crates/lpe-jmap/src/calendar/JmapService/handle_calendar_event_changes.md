---
type: Rust Method
title: handle_calendar_event_changes
resource: crates/lpe-jmap/src/calendar.rs#L459-L477
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_calendar_event_changes( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)
- [object_changes_response](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)