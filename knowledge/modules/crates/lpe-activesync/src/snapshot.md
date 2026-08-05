---
type: Rust Module
title: snapshot
resource: crates/lpe-activesync/src/snapshot.rs#L1-L627
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-domain-civil-from-days-days-from-civil
  - external/lpe-storage-parse-calendar-participants-metadata-activesyncattachment-calendarparticipantmetadata-clientcontact-clientevent-jmapemail-jmapuploadblob
  - external/serde-json-json-value
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/crate-constants-mail-class-message-activesync-timestamp-format-email-address-split-name-protocol-bodypreferencetype-types-collectiondefinition-collectionstateentry-snapshotchange-snapshotentry-wbxml-wbxmlnode
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [BodyPreference](../../../../classes/crates/lpe-activesync/src/snapshot/BodyPreference.md)
- [default](../../../../functions/crates/lpe-activesync/src/snapshot/BodyPreference/default/default.md)
- [email_application_data](../../../../functions/crates/lpe-activesync/src/snapshot/email_application_data.md)
- [email_flag_value](../../../../functions/crates/lpe-activesync/src/snapshot/email_flag_value.md)
- [email_body_value](../../../../functions/crates/lpe-activesync/src/snapshot/email_body_value.md)
- [truncate_body_bytes](../../../../functions/crates/lpe-activesync/src/snapshot/truncate_body_bytes.md)
- [contact_application_data](../../../../functions/crates/lpe-activesync/src/snapshot/contact_application_data.md)
- [calendar_application_data](../../../../functions/crates/lpe-activesync/src/snapshot/calendar_application_data.md)
- [push_text](../../../../functions/crates/lpe-activesync/src/snapshot/push_text.md)
- [push_body](../../../../functions/crates/lpe-activesync/src/snapshot/push_body.md)
- [push_attendees](../../../../functions/crates/lpe-activesync/src/snapshot/push_attendees.md)
- [attendee_status](../../../../functions/crates/lpe-activesync/src/snapshot/attendee_status.md)
- [compact_datetime](../../../../functions/crates/lpe-activesync/src/snapshot/compact_datetime.md)
- [add_minutes_to_compact](../../../../functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact.md)
- [parse_date](../../../../functions/crates/lpe-activesync/src/snapshot/parse_date.md)
- [parse_time](../../../../functions/crates/lpe-activesync/src/snapshot/parse_time.md)
- [recurrence_application_data](../../../../functions/crates/lpe-activesync/src/snapshot/recurrence_application_data.md)
- [rrule_fields](../../../../functions/crates/lpe-activesync/src/snapshot/rrule_fields.md)
- [rrule_weekday_mask](../../../../functions/crates/lpe-activesync/src/snapshot/rrule_weekday_mask.md)
- [rrule_until_to_compact](../../../../functions/crates/lpe-activesync/src/snapshot/rrule_until_to_compact.md)
- [snapshot_to_value](../../../../functions/crates/lpe-activesync/src/snapshot/snapshot_to_value.md)
- [diff_snapshots](../../../../functions/crates/lpe-activesync/src/snapshot/diff_snapshots.md)
- [diff_collection_states](../../../../functions/crates/lpe-activesync/src/snapshot/diff_collection_states.md)
- [snapshot_fingerprints](../../../../functions/crates/lpe-activesync/src/snapshot/snapshot_fingerprints.md)
- [value_to_node](../../../../functions/crates/lpe-activesync/src/snapshot/value_to_node.md)
- [collection_window_size](../../../../functions/crates/lpe-activesync/src/snapshot/collection_window_size.md)
- [require_collection_id](../../../../functions/crates/lpe-activesync/src/snapshot/require_collection_id.md)
- [require_sync_collections](../../../../functions/crates/lpe-activesync/src/snapshot/require_sync_collections.md)
- [mail_collection](../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)
- [drafts_collection](../../../../functions/crates/lpe-activesync/src/snapshot/drafts_collection.md)
- [parse_collection_mailbox_id](../../../../functions/crates/lpe-activesync/src/snapshot/parse_collection_mailbox_id.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_domain::{civil_from_days, days_from_civil}`
- `lpe_storage::{
    parse_calendar_participants_metadata, ActiveSyncAttachment, CalendarParticipantMetadata,
    ClientContact, ClientEvent, JmapEmail, JmapUploadBlob,
}`
- `serde_json::{json, Value}`
- `std::collections::HashMap`
- `uuid::Uuid`
- `crate::{
    constants::MAIL_CLASS,
    message::{activesync_timestamp, format_email_address, split_name},
    protocol::BodyPreferenceType,
    types::{CollectionDefinition, CollectionStateEntry, SnapshotChange, SnapshotEntry},
    wbxml::WbxmlNode,
}`

# Member of

- [lpe-activesync](../../../../packages/crates/lpe-activesync.md)