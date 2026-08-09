---
type: Rust Function
title: with_default_scoped_mapi_identity
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L17-L23
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_scoped_mapi_identity
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox
---

# Signature

`async fn with_default_scoped_mapi_identity<T>(operation: impl FnOnce() -> T) -> T`

# Calls

- [with_scoped_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_scoped_mapi_identity.md)

# Called by

- [mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar.md)
- [mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox.md)