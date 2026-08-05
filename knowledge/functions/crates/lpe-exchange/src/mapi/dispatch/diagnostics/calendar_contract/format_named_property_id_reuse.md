---
type: Rust Function
title: format_named_property_id_reuse
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract.rs#L275-L317
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
---

# Signature

`fn format_named_property_id_reuse(session: &MapiSession) -> String`

# Calls

- [entry](../../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)