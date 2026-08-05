---
type: Rust Module
title: calendar
resource: crates/lpe-jmap/src/calendar.rs#L1-L1420
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-magika-ingresscontext-policydecision-validationrequest
  - external/lpe-storage-calendar-attendee-labels-normalize-calendar-email-normalize-calendar-participation-status-parse-calendar-participants-metadata-serialize-calendar-participants-metadata-accessibleevent-attachmentuploadinput-authenticatedaccount-calendareventattachment-calendarorganizermetadata-calendarparticipantmetadata-calendarparticipantsmetadata-collaborationcollection-upsertclienteventinput
  - external/serde-json-json-map-value
  - external/std-collections-hashmap-hashset
  - external/uuid-uuid
  - external/crate-convert-apply-jmap-property-patch-has-jmap-property-patch-insert-if-error-set-error-parse-parse-first-property-object-string-parse-local-datetime-parse-local-datetime-value-parse-optional-string-parse-required-string-parse-uuid-parse-uuid-list-protocol-calendareventgetarguments-calendareventqueryarguments-calendareventqueryfilter-calendareventsetarguments-calendargetarguments-calendarqueryarguments-calendarsetarguments-changesarguments-entityquerysort-querychangesarguments-state-query-changes-response-query-position-stateentry-validation-validate-calendar-event-filter-validate-entity-sort-jmapservice-default-get-limit-max-query-limit
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [handle_calendar_get](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_get.md)
- [handle_calendar_query](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query.md)
- [handle_calendar_query_changes](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query_changes.md)
- [handle_calendar_changes](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_changes.md)
- [handle_calendar_set](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set.md)
- [handle_calendar_import_or_copy](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_import_or_copy.md)
- [calendar_update_name](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name.md)
- [handle_calendar_event_get](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get.md)
- [handle_calendar_event_query](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query.md)
- [handle_calendar_event_query_changes](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes.md)
- [handle_calendar_event_changes](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_changes.md)
- [handle_calendar_event_set](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set.md)
- [calendar_event_update_input](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input.md)
- [calendar_attachments_by_event](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_attachments_by_event.md)
- [attach_calendar_uploads](../../../../functions/crates/lpe-jmap/src/calendar/JmapService/attach_calendar_uploads.md)
- [parse_calendar_collection_name](../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_collection_name.md)
- [calendar_properties](../../../../functions/crates/lpe-jmap/src/calendar/calendar_properties.md)
- [calendar_to_value](../../../../functions/crates/lpe-jmap/src/calendar/calendar_to_value.md)
- [calendar_event_properties](../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_properties.md)
- [calendar_event_to_value](../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_to_value.md)
- [calendar_attachment_links](../../../../functions/crates/lpe-jmap/src/calendar/calendar_attachment_links.md)
- [insert_json_if](../../../../functions/crates/lpe-jmap/src/calendar/insert_json_if.md)
- [participants_from_event](../../../../functions/crates/lpe-jmap/src/calendar/participants_from_event.md)
- [participants_from_attendees](../../../../functions/crates/lpe-jmap/src/calendar/participants_from_attendees.md)
- [participant_value](../../../../functions/crates/lpe-jmap/src/calendar/participant_value.md)
- [event_matches_filter](../../../../functions/crates/lpe-jmap/src/calendar/event_matches_filter.md)
- [calendar_event_sort_key](../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_sort_key.md)
- [calendar_event_start](../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_start.md)
- [collection_sort_key](../../../../functions/crates/lpe-jmap/src/calendar/collection_sort_key.md)
- [serialize_entity_query_sort](../../../../functions/crates/lpe-jmap/src/calendar/serialize_entity_query_sort.md)
- [reject_collection_query_constraints](../../../../functions/crates/lpe-jmap/src/calendar/reject_collection_query_constraints.md)
- [parse_calendar_event_input](../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
- [CalendarAttachmentInput](../../../../classes/crates/lpe-jmap/src/calendar/CalendarAttachmentInput.md)
- [parse_calendar_attachment_inputs](../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_attachment_inputs.md)
- [reject_unknown_calendar_event_properties](../../../../functions/crates/lpe-jmap/src/calendar/reject_unknown_calendar_event_properties.md)
- [validate_calendar_ids](../../../../functions/crates/lpe-jmap/src/calendar/validate_calendar_ids.md)
- [parse_calendar_location](../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_location.md)
- [parse_calendar_participants](../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_participants.md)
- [parse_calendar_participants_json](../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_participants_json.md)
- [parse_jmap_calendar_participants](../../../../functions/crates/lpe-jmap/src/calendar/parse_jmap_calendar_participants.md)
- [participant_email](../../../../functions/crates/lpe-jmap/src/calendar/participant_email.md)
- [parse_calendar_duration](../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_duration.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_magika::{IngressContext, PolicyDecision, ValidationRequest}`
- `lpe_storage::{
    calendar_attendee_labels, normalize_calendar_email, normalize_calendar_participation_status,
    parse_calendar_participants_metadata, serialize_calendar_participants_metadata,
    AccessibleEvent, AttachmentUploadInput, AuthenticatedAccount, CalendarEventAttachment,
    CalendarOrganizerMetadata, CalendarParticipantMetadata, CalendarParticipantsMetadata,
    CollaborationCollection, UpsertClientEventInput,
}`
- `serde_json::{json, Map, Value}`
- `std::collections::{HashMap, HashSet}`
- `uuid::Uuid`
- `crate::{
    convert::{apply_jmap_property_patch, has_jmap_property_patch, insert_if},
    error::set_error,
    parse::{
        parse_first_property_object_string, parse_local_datetime, parse_local_datetime_value,
        parse_optional_string, parse_required_string, parse_uuid, parse_uuid_list,
    },
    protocol::{
        CalendarEventGetArguments, CalendarEventQueryArguments, CalendarEventQueryFilter,
        CalendarEventSetArguments, CalendarGetArguments, CalendarQueryArguments,
        CalendarSetArguments, ChangesArguments, EntityQuerySort, QueryChangesArguments,
    },
    state::{query_changes_response, query_position, StateEntry},
    validation::{validate_calendar_event_filter, validate_entity_sort},
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)