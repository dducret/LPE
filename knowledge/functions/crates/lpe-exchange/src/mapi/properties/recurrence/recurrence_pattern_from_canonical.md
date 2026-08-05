---
type: Rust Function
title: recurrence_pattern_from_canonical
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L117-L210
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/parse_canonical_recurrence_rule
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_rule_value
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/until_date
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_first_date_minutes
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_deleted_dates_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_modified_exceptions_from_json
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob
---

# Signature

`fn recurrence_pattern_from_canonical( event: &AccessibleEvent, ) -> Result<CanonicalRecurrencePattern>`

# Calls

- [parse_canonical_recurrence_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/parse_canonical_recurrence_rule.md)
- [recurrence_rule_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_rule_value.md)
- [recurrence_minutes_since_1601](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)
- [until_date](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/until_date.md)
- [recurrence_first_date_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_first_date_minutes.md)
- [recurrence_deleted_dates_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_deleted_dates_from_json.md)
- [recurrence_modified_exceptions_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_modified_exceptions_from_json.md)

# Called by

- [calendar_recurrence_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/calendar_recurrence_blob.md)