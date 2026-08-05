---
type: Rust Method
title: as_bool
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L490-L511
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_property_restriction
  - functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_upload
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_deleted_dates_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_modified_exceptions_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-jmap/src/calendar/validate_calendar_ids
  - functions/crates/lpe-jmap/src/contacts/validate_address_book_ids
  - functions/crates/lpe-jmap/src/drafts/parse_draft_keywords
  - functions/crates/lpe-jmap/src/drafts/parse_email_copy
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import
---

# Signature

`pub(in crate::mapi) fn as_bool(&self) -> Option<bool>`

# Called by

- [rop_property_restriction](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_property_restriction.md)
- [compare_mapi_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/compare_mapi_values.md)
- [pending_attachment_upload](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_upload.md)
- [message_followup_update_from_mapi_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values.md)
- [recurrence_deleted_dates_from_json](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_deleted_dates_from_json.md)
- [recurrence_modified_exceptions_from_json](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_modified_exceptions_from_json.md)
- [split_reminder_property_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values.md)
- [mapi_value_from_json](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json.md)
- [write_mapi_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [validate_calendar_ids](../../../../../../../../functions/crates/lpe-jmap/src/calendar/validate_calendar_ids.md)
- [validate_address_book_ids](../../../../../../../../functions/crates/lpe-jmap/src/contacts/validate_address_book_ids.md)
- [parse_draft_keywords](../../../../../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_keywords.md)
- [parse_email_copy](../../../../../../../../functions/crates/lpe-jmap/src/drafts/parse_email_copy.md)
- [parse_email_import](../../../../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import.md)