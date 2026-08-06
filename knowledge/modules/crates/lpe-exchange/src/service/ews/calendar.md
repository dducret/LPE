---
type: Rust Module
title: calendar
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L1-L709
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-super
  - external/crate-ews-types-ewsmonth-ewsresponsetype-ewsweekday
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [calendar_change_key](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/calendar_change_key.md)
- [calendar_item_summary_xml_with_change_key](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_summary_xml_with_change_key.md)
- [calendar_item_xml_with_change_key](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_xml_with_change_key.md)
- [create_event_success_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/create_event_success_response.md)
- [ews_datetime](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime.md)
- [event_end_datetime](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/event_end_datetime.md)
- [parse_create_event_input](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input.md)
- [parse_update_event_input](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)
- [ews_attendees_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_attendees_xml.md)
- [ews_attendee_collection_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_attendee_collection_xml.md)
- [ews_attendee_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_attendee_xml.md)
- [partstat_to_ews_response_type](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/partstat_to_ews_response_type.md)
- [ews_recurrence_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_recurrence_xml.md)
- [rrule_to_ews_recurrence](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/rrule_to_ews_recurrence.md)
- [rrule_fields](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/rrule_fields.md)
- [rrule_weekday_to_ews](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/rrule_weekday_to_ews.md)
- [rrule_month_number_to_ews](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/rrule_month_number_to_ews.md)
- [rrule_until_to_ews_date](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/rrule_until_to_ews_date.md)
- [parse_ews_recurrence](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_ews_recurrence.md)
- [push_interval_part](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/push_interval_part.md)
- [parse_positive_number](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_positive_number.md)
- [ews_weekday_to_rrule](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_weekday_to_rrule.md)
- [ews_month_to_number](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_month_to_number.md)
- [rrule_date](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/rrule_date.md)
- [parse_event_participants](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_event_participants.md)
- [parse_attendee](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_attendee.md)
- [ews_response_type_to_partstat](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_response_type_to_partstat.md)
- [requested_time_zone](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/requested_time_zone.md)
- [ews_datetime_parts](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_parts.md)
- [ews_duration_minutes](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_duration_minutes.md)
- [time_minutes](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/time_minutes.md)
- [ews_datetime_minutes](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_minutes.md)
- [ews_date_after_days](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_date_after_days.md)
- [ews_date_parts](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_date_parts.md)
- [days_in_month](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/days_in_month.md)
- [days_from_civil](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/days_from_civil.md)
- [civil_from_days](../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/civil_from_days.md)

# Imports

- `super::super::*`
- `crate::ews_types::{EwsMonth, EwsResponseType, EwsWeekday}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)