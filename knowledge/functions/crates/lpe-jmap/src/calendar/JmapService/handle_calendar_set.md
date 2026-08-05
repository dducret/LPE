---
type: Rust Method
title: handle_calendar_set
resource: crates/lpe-jmap/src/calendar.rs#L181-L278
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_collection_name
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_import_or_copy
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_calendar_set( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)
- [parse_calendar_collection_name](../../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_collection_name.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [calendar_update_name](../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_calendar_import_or_copy](../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_import_or_copy.md)
- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)