---
type: Rust Method
title: handle_calendar_event_query
resource: crates/lpe-jmap/src/calendar.rs#L377-L428
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/validation/validate_entity_sort
  - functions/crates/lpe-jmap/src/validation/validate_calendar_event_filter
  - functions/crates/lpe-jmap/src/calendar/event_matches_filter
  - functions/crates/lpe-jmap/src/state/query_position
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_calendar_event_query( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [validate_entity_sort](../../../../../../functions/crates/lpe-jmap/src/validation/validate_entity_sort.md)
- [validate_calendar_event_filter](../../../../../../functions/crates/lpe-jmap/src/validation/validate_calendar_event_filter.md)
- [event_matches_filter](../../../../../../functions/crates/lpe-jmap/src/calendar/event_matches_filter.md)
- [query_position](../../../../../../functions/crates/lpe-jmap/src/state/query_position.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)