---
type: Rust Method
title: handle_calendar_event_get
resource: crates/lpe-jmap/src/calendar.rs#L327-L375
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/calendar/calendar_event_properties
  - functions/crates/lpe-jmap/src/parse/parse_uuid_list
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_attachments_by_event
  - functions/crates/lpe-jmap/src/calendar/calendar_event_to_value
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_calendar_event_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [calendar_event_properties](../../../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_properties.md)
- [parse_uuid_list](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid_list.md)
- [calendar_attachments_by_event](../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_attachments_by_event.md)
- [calendar_event_to_value](../../../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_to_value.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)