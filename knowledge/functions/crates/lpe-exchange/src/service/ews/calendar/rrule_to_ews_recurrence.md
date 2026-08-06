---
type: Rust Function
title: rrule_to_ews_recurrence
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L330-L428
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_recurrence_xml
---

# Signature

`fn rrule_to_ews_recurrence(rrule: &str, start_date: &str) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [ews_recurrence_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_recurrence_xml.md)