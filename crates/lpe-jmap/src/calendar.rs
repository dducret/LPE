use anyhow::{anyhow, bail, Result};
use lpe_storage::{
    calendar_attendee_labels, normalize_calendar_email, normalize_calendar_participation_status,
    parse_calendar_participants_metadata, serialize_calendar_participants_metadata,
    AccessibleEvent, AuthenticatedAccount, CalendarOrganizerMetadata, CalendarParticipantMetadata,
    CalendarParticipantsMetadata, CollaborationCollection, UpsertClientEventInput,
};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    convert::insert_if,
    error::set_error,
    parse::{
        parse_first_property_object_string, parse_local_datetime, parse_local_datetime_value,
        parse_optional_string, parse_required_string, parse_uuid, parse_uuid_list,
    },
    protocol::{
        CalendarEventGetArguments, CalendarEventQueryArguments, CalendarEventQueryFilter,
        CalendarEventSetArguments, CalendarGetArguments, CalendarQueryArguments, ChangesArguments,
        EntityQuerySort, QueryChangesArguments,
    },
    state::{changes_response, query_changes_response, StateEntry},
    validation::{validate_calendar_event_filter, validate_entity_sort},
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
};

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_calendar_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarGetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = calendar_properties(arguments.properties);
        let requested_ids = arguments.ids.unwrap_or_default();
        let mut collections = self
            .store
            .fetch_accessible_calendar_collections(account_id)
            .await?;
        collections.sort_by_key(collection_sort_key);
        let list = collections
            .iter()
            .filter(|collection| requested_ids.is_empty() || requested_ids.contains(&collection.id))
            .map(|collection| calendar_to_value(collection, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .into_iter()
            .filter(|id| !collections.iter().any(|collection| collection.id == *id))
            .map(Value::String)
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Calendar").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_calendar_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarQueryArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut collections = self
            .store
            .fetch_accessible_calendar_collections(account_id)
            .await?;
        collections.sort_by_key(collection_sort_key);
        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = collections
            .iter()
            .map(|collection| collection.id.clone())
            .skip(position)
            .take(limit)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": crate::encode_query_state(
                account_id,
                "Calendar/query",
                None,
                None,
                collections.iter().map(|collection| collection.id.clone()).collect(),
            )?,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": collections.len(),
        }))
    }

    pub(crate) async fn handle_calendar_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        reject_collection_query_constraints(
            arguments.filter.as_ref(),
            arguments.sort.as_ref(),
            "Calendar/query",
        )?;
        let collections = self
            .store
            .fetch_accessible_calendar_collections(account_id)
            .await?;
        query_changes_response(
            account_id,
            "Calendar/query",
            arguments.since_query_state,
            None,
            None,
            collections
                .iter()
                .map(|collection| collection.id.clone())
                .collect(),
            collections.len() as u64,
            arguments.max_changes,
        )
    }

    pub(crate) async fn handle_calendar_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let collections = self
            .store
            .fetch_accessible_calendar_collections(account_id)
            .await?;
        let entries = collections
            .into_iter()
            .map(|collection| StateEntry {
                id: collection.id.clone(),
                fingerprint: super::collection_state_fingerprint(&collection),
            })
            .collect::<Vec<_>>();
        changes_response(
            account_id,
            "Calendar",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
    }

    pub(crate) async fn handle_calendar_event_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarEventGetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = calendar_event_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let events = if let Some(ids) = requested_ids.as_ref() {
            self.store
                .fetch_accessible_events_by_ids(account_id, ids)
                .await?
        } else {
            self.store.fetch_accessible_events(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();
        let list = events
            .iter()
            .filter(|event| requested_ids.is_none() || requested_set.contains(&event.id))
            .map(|event| calendar_event_to_value(event, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !events.iter().any(|event| event.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "CalendarEvent").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_calendar_event_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarEventQueryArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_entity_sort(arguments.sort.as_deref(), "start", true)?;
        validate_calendar_event_filter(arguments.filter.as_ref())?;

        let mut events = self.store.fetch_accessible_events(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            events.retain(|event| event_matches_filter(event, filter));
        }
        events.sort_by_key(calendar_event_sort_key);

        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = events
            .iter()
            .skip(position)
            .take(limit)
            .map(|event| event.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": crate::encode_query_state(
                account_id,
                "CalendarEvent",
                arguments.filter.map(serde_json::to_value).transpose()?,
                serialize_entity_query_sort(arguments.sort)?,
                events.iter().map(|event| event.id.to_string()).collect(),
            )?,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": events.len(),
        }))
    }

    pub(crate) async fn handle_calendar_event_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<CalendarEventQueryFilter, EntityQuerySort> =
            serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_entity_sort(arguments.sort.as_deref(), "start", true)?;
        validate_calendar_event_filter(arguments.filter.as_ref())?;

        let mut events = self.store.fetch_accessible_events(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            events.retain(|event| event_matches_filter(event, filter));
        }
        events.sort_by_key(calendar_event_sort_key);

        query_changes_response(
            account_id,
            "CalendarEvent",
            arguments.since_query_state,
            arguments.filter.map(serde_json::to_value).transpose()?,
            serialize_entity_query_sort(arguments.sort)?,
            events.iter().map(|event| event.id.to_string()).collect(),
            events.len() as u64,
            arguments.max_changes,
        )
    }

    pub(crate) async fn handle_calendar_event_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self
            .object_state_entries(account_id, "CalendarEvent")
            .await?;
        changes_response(
            account_id,
            "CalendarEvent",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
    }

    pub(crate) async fn handle_calendar_event_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: CalendarEventSetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "CalendarEvent").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_calendar_event_input(None, account_id, value) {
                    Ok((collection_id, input)) => match self
                        .store
                        .create_accessible_event(account_id, collection_id.as_deref(), input)
                        .await
                    {
                        Ok(event) => {
                            created_ids.insert(creation_id.clone(), event.id.to_string());
                            created.insert(creation_id, json!({ "id": event.id.to_string() }));
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match parse_uuid(&id).and_then(|event_id| {
                    parse_calendar_event_input(Some(event_id), account_id, value)
                        .map(|(_, input)| (event_id, input))
                }) {
                    Ok((event_id, input)) => match self
                        .store
                        .update_accessible_event(account_id, event_id, input)
                        .await
                    {
                        Ok(_) => {
                            updated.insert(id, Value::Object(Map::new()));
                        }
                        Err(error) => {
                            not_updated.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(event_id) => match self
                        .store
                        .delete_accessible_event(account_id, event_id)
                        .await
                    {
                        Ok(()) => destroyed.push(Value::String(id)),
                        Err(error) => {
                            not_destroyed.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        let new_state = self.object_state(account_id, "CalendarEvent").await?;
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": new_state,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }
}

fn calendar_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "sortOrder".to_string(),
                "isSubscribed".to_string(),
                "isVisible".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn calendar_to_value(collection: &CollaborationCollection, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", collection.id.clone());
    insert_if(
        properties,
        &mut object,
        "name",
        collection.display_name.clone(),
    );
    insert_if(properties, &mut object, "sortOrder", 0);
    insert_if(properties, &mut object, "isSubscribed", true);
    insert_if(properties, &mut object, "isVisible", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayRead": collection.rights.may_read,
                "mayAddItems": collection.rights.may_write,
                "mayModifyItems": collection.rights.may_write,
                "mayRemoveItems": collection.rights.may_delete,
                "mayRename": false,
                "mayDelete": false,
                "mayAdmin": collection.rights.may_share,
            }),
        );
    }
    Value::Object(object)
}

fn calendar_event_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "uid".to_string(),
                "@type".to_string(),
                "title".to_string(),
                "start".to_string(),
                "duration".to_string(),
                "timeZone".to_string(),
                "locations".to_string(),
                "participants".to_string(),
                "description".to_string(),
                "calendarIds".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn calendar_event_to_value(event: &AccessibleEvent, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", event.id.to_string());
    insert_if(properties, &mut object, "uid", event.id.to_string());
    insert_if(properties, &mut object, "@type", "Event");
    insert_if(properties, &mut object, "title", event.title.clone());
    insert_if(
        properties,
        &mut object,
        "start",
        format!("{}T{}:00", event.date, event.time),
    );
    insert_if(
        properties,
        &mut object,
        "duration",
        if event.duration_minutes <= 0 {
            "PT0S".to_string()
        } else {
            format!("PT{}M", event.duration_minutes)
        },
    );
    if properties.contains("timeZone") {
        if event.time_zone.trim().is_empty() {
            object.insert("timeZone".to_string(), Value::Null);
        } else {
            object.insert(
                "timeZone".to_string(),
                Value::String(event.time_zone.clone()),
            );
        }
    }
    if properties.contains("locations") && !event.location.trim().is_empty() {
        object.insert(
            "locations".to_string(),
            json!({
                "main": {
                    "@type": "Location",
                    "name": event.location,
                }
            }),
        );
    }
    if properties.contains("participants") {
        let participants = participants_from_event(event);
        if participants
            .as_object()
            .map(|entries| !entries.is_empty())
            .unwrap_or(false)
        {
            object.insert("participants".to_string(), participants);
        }
    }
    insert_if(properties, &mut object, "description", event.notes.clone());
    if properties.contains("calendarIds") {
        object.insert(
            "calendarIds".to_string(),
            json!({event.collection_id.clone(): true}),
        );
    }
    Value::Object(object)
}

fn participants_from_event(event: &AccessibleEvent) -> Value {
    let metadata = parse_calendar_participants_metadata(&event.attendees_json);
    if metadata.organizer.is_some() || !metadata.attendees.is_empty() {
        let mut participants = Map::new();
        if let Some(organizer) = metadata.organizer {
            participants.insert(
                "owner".to_string(),
                participant_value(
                    &organizer.common_name,
                    &organizer.email,
                    json!({"owner": true}),
                    None,
                    false,
                ),
            );
        }
        for (index, attendee) in metadata.attendees.iter().enumerate() {
            let mut roles = Map::new();
            roles.insert("attendee".to_string(), Value::Bool(true));
            if attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT") {
                roles.insert("optional".to_string(), Value::Bool(true));
            }
            participants.insert(
                format!("p{}", index + 1),
                participant_value(
                    &attendee.common_name,
                    &attendee.email,
                    Value::Object(roles),
                    Some(&attendee.partstat),
                    attendee.rsvp,
                ),
            );
        }
        return Value::Object(participants);
    }
    participants_from_attendees(&event.attendees)
}

fn participants_from_attendees(attendees: &str) -> Value {
    let participants = attendees
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .enumerate()
        .map(|(index, value)| {
            let key = format!("p{}", index + 1);
            let participant = if value.contains('@') {
                json!({
                    "@type": "Participant",
                    "name": value,
                    "email": value,
                    "roles": {"attendee": true},
                })
            } else {
                json!({
                    "@type": "Participant",
                    "name": value,
                    "roles": {"attendee": true},
                })
            };
            (key, participant)
        })
        .collect::<Map<String, Value>>();
    Value::Object(participants)
}

fn participant_value(
    name: &str,
    email: &str,
    roles: Value,
    participation_status: Option<&str>,
    expect_reply: bool,
) -> Value {
    let mut participant = Map::new();
    participant.insert(
        "@type".to_string(),
        Value::String("Participant".to_string()),
    );
    if !name.trim().is_empty() {
        participant.insert("name".to_string(), Value::String(name.trim().to_string()));
    }
    if !email.trim().is_empty() {
        participant.insert("email".to_string(), Value::String(email.trim().to_string()));
        participant.insert(
            "sendTo".to_string(),
            json!({"imip": format!("mailto:{}", email.trim())}),
        );
    }
    participant.insert("roles".to_string(), roles);
    if let Some(status) = participation_status {
        participant.insert(
            "participationStatus".to_string(),
            Value::String(normalize_calendar_participation_status(status)),
        );
    }
    if expect_reply {
        participant.insert("expectReply".to_string(), Value::Bool(true));
    }
    Value::Object(participant)
}

fn event_matches_filter(event: &AccessibleEvent, filter: &CalendarEventQueryFilter) -> bool {
    if let Some(calendar_id) = filter.in_calendar.as_deref() {
        if calendar_id != event.collection_id {
            return false;
        }
    }
    if let Some(after) = filter.after.as_deref() {
        let start = calendar_event_start(event);
        if let Ok(after) = parse_local_datetime_value(after) {
            if start < after {
                return false;
            }
        } else {
            return false;
        }
    }
    if let Some(before) = filter.before.as_deref() {
        let start = calendar_event_start(event);
        if let Ok(before) = parse_local_datetime_value(before) {
            if start >= before {
                return false;
            }
        } else {
            return false;
        }
    }
    if let Some(text) = filter.text.as_deref() {
        let needle = text.trim().to_lowercase();
        if !needle.is_empty() {
            let haystack = format!(
                "{} {} {} {}",
                event.title, event.location, event.attendees, event.notes
            )
            .to_lowercase();
            if !haystack.contains(&needle) {
                return false;
            }
        }
    }
    true
}

fn calendar_event_sort_key(event: &AccessibleEvent) -> (String, String) {
    (calendar_event_start(event), event.id.to_string())
}

fn calendar_event_start(event: &AccessibleEvent) -> String {
    format!("{}T{}:00", event.date, event.time)
}

fn collection_sort_key(collection: &CollaborationCollection) -> (String, String) {
    (
        collection.display_name.to_lowercase(),
        collection.id.to_lowercase(),
    )
}

fn serialize_entity_query_sort(sort: Option<Vec<EntityQuerySort>>) -> Result<Option<Vec<Value>>> {
    sort.map(|sort| {
        sort.into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()
    })
    .transpose()
    .map_err(Into::into)
}

fn reject_collection_query_constraints(
    filter: Option<&Value>,
    sort: Option<&Vec<Value>>,
    method: &str,
) -> Result<()> {
    if filter.is_some() || sort.is_some() {
        bail!("{method} does not support filter or sort");
    }
    Ok(())
}

fn parse_calendar_event_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<(Option<String>, UpsertClientEventInput)> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("calendar event arguments must be an object"))?;
    reject_unknown_calendar_event_properties(object)?;
    let collection_id = validate_calendar_ids(object.get("calendarIds"))?;

    let event_type = object
        .get("@type")
        .and_then(Value::as_str)
        .unwrap_or("Event");
    if event_type != "Event" {
        bail!("only @type=Event is supported");
    }
    if let Some(uid) = object.get("uid").and_then(Value::as_str) {
        if uid.trim().is_empty() {
            bail!("uid must not be empty");
        }
    }

    let (date, time) = parse_local_datetime(
        object
            .get("start")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("start is required"))?,
    )?;

    Ok((
        collection_id,
        UpsertClientEventInput {
            id,
            account_id,
            date,
            time,
            time_zone: parse_optional_string(object.get("timeZone"))?.unwrap_or_default(),
            duration_minutes: parse_calendar_duration(object.get("duration"))?,
            recurrence_rule: String::new(),
            title: parse_required_string(object.get("title"), "title")?,
            location: parse_calendar_location(object.get("locations"))?,
            attendees: parse_calendar_participants(object.get("participants"))?,
            attendees_json: parse_calendar_participants_json(object.get("participants"))?,
            notes: parse_optional_string(object.get("description"))?.unwrap_or_default(),
        },
    ))
}

fn reject_unknown_calendar_event_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "@type" | "uid" | "title" | "start" | "duration" | "timeZone" | "locations"
            | "participants" | "description" | "calendarIds" => {}
            _ => bail!("unsupported calendar event property: {key}"),
        }
    }
    Ok(())
}

fn validate_calendar_ids(value: Option<&Value>) -> Result<Option<String>> {
    if let Some(value) = value {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("calendarIds must be an object"))?;
        if object.len() != 1 {
            bail!("exactly one calendarId must be provided");
        }
        let (collection_id, enabled) = object.iter().next().unwrap();
        if enabled.as_bool() != Some(true) {
            bail!("calendarIds entries must be true");
        }
        return Ok(Some(collection_id.clone()));
    }
    Ok(None)
}

fn parse_calendar_location(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "locations", "name")
}

fn parse_calendar_participants(value: Option<&Value>) -> Result<String> {
    Ok(calendar_attendee_labels(&parse_jmap_calendar_participants(
        value,
    )?))
}

fn parse_calendar_participants_json(value: Option<&Value>) -> Result<String> {
    Ok(serialize_calendar_participants_metadata(
        &parse_jmap_calendar_participants(value)?,
    ))
}

fn parse_jmap_calendar_participants(value: Option<&Value>) -> Result<CalendarParticipantsMetadata> {
    let Some(value) = value else {
        return Ok(CalendarParticipantsMetadata::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("participants must be an object"))?;
    let mut metadata = CalendarParticipantsMetadata::default();
    for participant in object.values() {
        let participant = participant
            .as_object()
            .ok_or_else(|| anyhow!("participants entries must be objects"))?;
        let common_name = participant
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        let roles = participant.get("roles").and_then(Value::as_object);
        let is_owner = roles
            .and_then(|roles| roles.get("owner"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let email = participant_email(participant, is_owner)?;
        if email.is_empty() && common_name.is_empty() {
            bail!("participant name or email is required");
        }
        if is_owner {
            if metadata.organizer.is_some() {
                bail!("only one organizer participant is supported");
            }
            metadata.organizer = Some(CalendarOrganizerMetadata { email, common_name });
            continue;
        }
        metadata.attendees.push(CalendarParticipantMetadata {
            email,
            common_name,
            role: if roles
                .and_then(|roles| roles.get("optional"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "OPT-PARTICIPANT".to_string()
            } else {
                "REQ-PARTICIPANT".to_string()
            },
            partstat: normalize_calendar_participation_status(
                participant
                    .get("participationStatus")
                    .and_then(Value::as_str)
                    .unwrap_or("needs-action"),
            ),
            rsvp: participant
                .get("expectReply")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        });
    }
    Ok(metadata)
}

fn participant_email(participant: &Map<String, Value>, owner: bool) -> Result<String> {
    if let Some(email) = participant.get("email").and_then(Value::as_str) {
        let normalized = normalize_calendar_email(email);
        if !normalized.is_empty() {
            return Ok(normalized);
        }
    }
    if let Some(send_to) = participant.get("sendTo").and_then(Value::as_object) {
        if let Some(email) = send_to.get("imip").and_then(Value::as_str) {
            let normalized = normalize_calendar_email(email);
            if !normalized.is_empty() {
                return Ok(normalized);
            }
        }
    }
    if owner {
        bail!("organizer participant email is required");
    }
    Ok(String::new())
}

fn parse_calendar_duration(value: Option<&Value>) -> Result<i32> {
    let Some(value) = value.and_then(Value::as_str) else {
        return Ok(0);
    };
    if value == "PT0S" {
        return Ok(0);
    }
    let Some(value) = value.strip_prefix("PT") else {
        bail!("duration must use PT...");
    };
    if let Some(hours) = value.strip_suffix('H') {
        return hours
            .parse::<i32>()
            .map(|value| value.max(0) * 60)
            .map_err(|_| anyhow!("invalid duration"));
    }
    if let Some(minutes) = value.strip_suffix('M') {
        return minutes
            .parse::<i32>()
            .map(|value| value.max(0))
            .map_err(|_| anyhow!("invalid duration"));
    }
    bail!("invalid duration")
}
