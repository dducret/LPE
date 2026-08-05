---
type: Rust Method
title: handle_calendar_get
resource: crates/lpe-jmap/src/calendar.rs#L32-L70
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/calendar/calendar_properties
  - functions/crates/lpe-jmap/src/calendar/calendar_to_value
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_calendar_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [calendar_properties](../../../../../../functions/crates/lpe-jmap/src/calendar/calendar_properties.md)
- [calendar_to_value](../../../../../../functions/crates/lpe-jmap/src/calendar/calendar_to_value.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)