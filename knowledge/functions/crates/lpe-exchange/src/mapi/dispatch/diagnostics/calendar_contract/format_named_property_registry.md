---
type: Rust Function
title: format_named_property_registry
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract.rs#L233-L261
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_named_registry_entry
  - functions/crates/lpe-exchange/src/mapi/properties/named/is_calendar_named_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
---

# Signature

`fn format_named_property_registry(session: &MapiSession) -> String`

# Calls

- [format_named_registry_entry](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_named_registry_entry.md)
- [is_calendar_named_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/is_calendar_named_property.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)