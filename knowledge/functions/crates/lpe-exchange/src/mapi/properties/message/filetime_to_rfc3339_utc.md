---
type: Rust Function
title: filetime_to_rfc3339_utc
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L1062-L1064
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_property_clause
  - functions/crates/lpe-exchange/src/mapi/properties/message/ical_utc_filetime
---

# Signature

`pub(in crate::mapi) fn filetime_to_rfc3339_utc(filetime: i64) -> Option<String>`

# Calls

- [filetime_to_date_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time.md)

# Called by

- [bounded_search_property_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_property_clause.md)
- [ical_utc_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/ical_utc_filetime.md)