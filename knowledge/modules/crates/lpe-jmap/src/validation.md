---
type: Rust Module
title: validation
resource: crates/lpe-jmap/src/validation.rs#L1-L98
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/crate-parse-parse-local-datetime-parse-uuid
  - external/crate-protocol-calendareventqueryfilter-contactcardqueryfilter-emailquerysort-entityquerysort-taskqueryfilter-taskquerysort
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [validate_query_sort](../../../../functions/crates/lpe-jmap/src/validation/validate_query_sort.md)
- [validate_entity_sort](../../../../functions/crates/lpe-jmap/src/validation/validate_entity_sort.md)
- [validate_contact_filter](../../../../functions/crates/lpe-jmap/src/validation/validate_contact_filter.md)
- [validate_calendar_event_filter](../../../../functions/crates/lpe-jmap/src/validation/validate_calendar_event_filter.md)
- [validate_task_sort](../../../../functions/crates/lpe-jmap/src/validation/validate_task_sort.md)
- [validate_task_filter](../../../../functions/crates/lpe-jmap/src/validation/validate_task_filter.md)
- [require_collection_id](../../../../functions/crates/lpe-jmap/src/validation/require_collection_id.md)
- [validate_task_status_value](../../../../functions/crates/lpe-jmap/src/validation/validate_task_status_value.md)

# Imports

- `anyhow::{bail, Result}`
- `crate::parse::{parse_local_datetime, parse_uuid}`
- `crate::protocol::{
    CalendarEventQueryFilter, ContactCardQueryFilter, EmailQuerySort, EntityQuerySort,
    TaskQueryFilter, TaskQuerySort,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)