use super::*;

pub(super) fn calendar_mapi_attachments(
    attachments: &[CalendarEventAttachment],
) -> Vec<MapiAttachment> {
    attachments
        .iter()
        .enumerate()
        .map(|(index, attachment)| MapiAttachment {
            attach_num: index as u32,
            canonical_id: attachment.id,
            file_reference: attachment.file_reference.clone(),
            file_name: attachment.file_name.clone(),
            media_type: attachment.media_type.clone(),
            disposition: None,
            content_id: None,
            size_octets: attachment.size_octets,
        })
        .collect()
}

pub(super) fn fallback_event_version(event: &AccessibleEvent, event_id: u64) -> MapiEventVersion {
    let change_number = mapi_mailstore::change_number_for_store_id(event_id);
    let timestamp = format!("{}T{}:00Z", event.date, event.time);
    MapiEventVersion {
        event_id: event.id,
        canonical_modseq: 1,
        change_number,
        search_key: None,
        change_key: mapi_mailstore::change_key_for_change_number(change_number),
        predecessor_change_list: mapi_mailstore::predecessor_change_list(change_number),
        last_modification_time: mapi_mailstore::filetime_from_rfc3339_utc(&timestamp),
        created_at: timestamp.clone(),
        updated_at: timestamp,
    }
}

impl MapiMailStoreSnapshot {
    pub(crate) fn with_calendar_recipient_response_times(
        mut self,
        values: Vec<crate::store::MapiCalendarRecipientResponseTime>,
    ) -> Self {
        let mut by_event = HashMap::<Uuid, HashMap<String, u64>>::new();
        let mut ambiguous = HashSet::<(Uuid, String)>::new();
        for value in values {
            let attendee_email = lpe_storage::normalize_calendar_email(&value.attendee_email);
            let response_time = mapi_mailstore::filetime_from_rfc3339_utc(&value.response_sent_at);
            if attendee_email.is_empty() || response_time == 0 {
                continue;
            }
            let ambiguity_key = (value.event_id, attendee_email.clone());
            if ambiguous.contains(&ambiguity_key) {
                continue;
            }
            let event_values = by_event.entry(value.event_id).or_default();
            if event_values
                .insert(attendee_email.clone(), response_time)
                .is_some()
            {
                event_values.remove(&attendee_email);
                ambiguous.insert(ambiguity_key);
            }
        }
        for event in &mut self.events {
            event.recipient_response_times =
                by_event.remove(&event.canonical_id).unwrap_or_default();
        }
        self
    }

    pub(crate) fn with_calendar_property_values(
        mut self,
        values: Vec<crate::store::MapiCalendarPropertyValue>,
    ) -> Self {
        let mut by_event = HashMap::<Uuid, Vec<MapiCustomPropertyValue>>::new();
        for value in values {
            by_event
                .entry(value.event_id)
                .or_default()
                .push(MapiCustomPropertyValue {
                    property_tag: value.property_tag,
                    property_type: value.property_type,
                    property_value: value.property_value,
                });
        }
        for event in &mut self.events {
            event.stored_properties = by_event.remove(&event.canonical_id).unwrap_or_default();
        }
        self
    }

    pub(crate) fn remember_event_custom_property_changes(
        &mut self,
        event_id: Uuid,
        upserts: &[lpe_storage::MapiEventCustomPropertyValue],
        deletes: &[u32],
    ) {
        let Some(event) = self
            .events
            .iter_mut()
            .find(|event| event.canonical_id == event_id)
        else {
            return;
        };
        event
            .stored_properties
            .retain(|value| !deletes.contains(&value.property_tag));
        for upsert in upserts {
            event
                .stored_properties
                .retain(|value| value.property_tag != upsert.property_tag);
            if upsert.property_tag != 0x300B_0102 {
                event.stored_properties.push(MapiCustomPropertyValue {
                    property_tag: upsert.property_tag,
                    property_type: upsert.property_type,
                    property_value: upsert.property_value.clone(),
                });
            }
        }
        event
            .stored_properties
            .sort_by_key(|value| (value.property_tag, value.property_type));
    }
}
