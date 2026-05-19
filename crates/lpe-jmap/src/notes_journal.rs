use anyhow::{anyhow, bail, Result};
use lpe_storage::{
    ClientNote, ClientReminder, JournalEntry, ReminderQuery, UpsertClientNoteInput,
    UpsertJournalEntryInput,
};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    convert::insert_if,
    error::set_error,
    parse::{parse_optional_string, parse_required_string, parse_uuid, parse_uuid_list},
    protocol::{
        ChangesArguments, JournalEntryGetArguments, JournalEntryQueryArguments,
        JournalEntryQueryFilter, JournalEntrySetArguments, NoteGetArguments, NoteQueryArguments,
        NoteQueryFilter, NoteSetArguments, QueryChangesArguments, ReminderQueryArguments,
    },
    state::{query_changes_response, query_position},
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
};

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_note_get(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: NoteGetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = note_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let notes = if let Some(ids) = requested_ids.as_ref() {
            self.store.fetch_jmap_notes_by_ids(account_id, ids).await?
        } else {
            self.store.fetch_jmap_notes(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();
        let list = notes
            .iter()
            .filter(|note| requested_ids.is_none() || requested_set.contains(&note.id))
            .map(|note| note_to_value(note, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !notes.iter().any(|note| note.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": self.object_state(account_id, "Note").await?,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_note_query(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: NoteQueryArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut notes = self.store.fetch_jmap_notes(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            notes.retain(|note| note_matches_filter(note, filter));
        }
        notes.sort_by_key(|note| {
            (
                std::cmp::Reverse(note.updated_at.clone()),
                note.id.to_string(),
            )
        });
        let all_ids = notes
            .iter()
            .map(|note| note.id.to_string())
            .collect::<Vec<_>>();
        let position = query_position(
            &all_ids,
            arguments.position,
            arguments.anchor.as_deref(),
            arguments.anchor_offset,
        )?;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = all_ids
            .iter()
            .skip(position)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": crate::encode_query_state(
                account_id,
                "Note",
                arguments.filter.map(serde_json::to_value).transpose()?,
                None,
                all_ids,
            )?,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": notes.len(),
        }))
    }

    pub(crate) async fn handle_note_query_changes(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<NoteQueryFilter, Value> =
            serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        if arguments.sort.is_some() {
            bail!("Note/query does not support sort");
        }
        let mut notes = self.store.fetch_jmap_notes(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            notes.retain(|note| note_matches_filter(note, filter));
        }
        notes.sort_by_key(|note| {
            (
                std::cmp::Reverse(note.updated_at.clone()),
                note.id.to_string(),
            )
        });

        query_changes_response(
            account_id,
            "Note",
            arguments.since_query_state,
            arguments.filter.map(serde_json::to_value).transpose()?,
            None,
            notes.iter().map(|note| note.id.to_string()).collect(),
            notes.len() as u64,
            arguments.max_changes,
        )
    }

    pub(crate) async fn handle_note_changes(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self.object_state_entries(account_id, "Note").await?;
        self.object_changes_response(
            account_id,
            "Note",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
        .await
    }

    pub(crate) async fn handle_note_set(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: NoteSetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "Note").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_note_input(None, account_id, value) {
                    Ok(input) => match self.store.upsert_jmap_note(input).await {
                        Ok(note) => {
                            created_ids.insert(creation_id.clone(), note.id.to_string());
                            created.insert(creation_id, json!({ "id": note.id.to_string() }));
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
                match parse_uuid(&id)
                    .and_then(|note_id| parse_note_input(Some(note_id), account_id, value))
                {
                    Ok(input) => match self.store.upsert_jmap_note(input).await {
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
                    Ok(note_id) => match self.store.delete_jmap_note(account_id, note_id).await {
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

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": self.object_state(account_id, "Note").await?,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    pub(crate) async fn handle_journal_entry_get(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: JournalEntryGetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = journal_entry_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let entries = if let Some(ids) = requested_ids.as_ref() {
            self.store
                .fetch_jmap_journal_entries_by_ids(account_id, ids)
                .await?
        } else {
            self.store.fetch_jmap_journal_entries(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();
        let list = entries
            .iter()
            .filter(|entry| requested_ids.is_none() || requested_set.contains(&entry.id))
            .map(|entry| journal_entry_to_value(entry, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !entries.iter().any(|entry| entry.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": self.object_state(account_id, "JournalEntry").await?,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_journal_entry_query(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: JournalEntryQueryArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut entries = self.store.fetch_jmap_journal_entries(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            entries.retain(|entry| journal_entry_matches_filter(entry, filter));
        }
        entries.sort_by_key(|entry| {
            (
                std::cmp::Reverse(
                    entry
                        .starts_at
                        .as_ref()
                        .or(entry.occurred_at.as_ref())
                        .unwrap_or(&entry.updated_at)
                        .clone(),
                ),
                entry.id.to_string(),
            )
        });
        let all_ids = entries
            .iter()
            .map(|entry| entry.id.to_string())
            .collect::<Vec<_>>();
        let position = query_position(
            &all_ids,
            arguments.position,
            arguments.anchor.as_deref(),
            arguments.anchor_offset,
        )?;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = all_ids
            .iter()
            .skip(position)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": crate::encode_query_state(
                account_id,
                "JournalEntry",
                arguments.filter.map(serde_json::to_value).transpose()?,
                None,
                all_ids,
            )?,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": entries.len(),
        }))
    }

    pub(crate) async fn handle_journal_entry_query_changes(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<JournalEntryQueryFilter, Value> =
            serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        if arguments.sort.is_some() {
            bail!("JournalEntry/query does not support sort");
        }
        let mut entries = self.store.fetch_jmap_journal_entries(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            entries.retain(|entry| journal_entry_matches_filter(entry, filter));
        }
        entries.sort_by_key(|entry| {
            (
                std::cmp::Reverse(
                    entry
                        .starts_at
                        .as_ref()
                        .or(entry.occurred_at.as_ref())
                        .unwrap_or(&entry.updated_at)
                        .clone(),
                ),
                entry.id.to_string(),
            )
        });

        query_changes_response(
            account_id,
            "JournalEntry",
            arguments.since_query_state,
            arguments.filter.map(serde_json::to_value).transpose()?,
            None,
            entries.iter().map(|entry| entry.id.to_string()).collect(),
            entries.len() as u64,
            arguments.max_changes,
        )
    }

    pub(crate) async fn handle_journal_entry_changes(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self
            .object_state_entries(account_id, "JournalEntry")
            .await?;
        self.object_changes_response(
            account_id,
            "JournalEntry",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
        .await
    }

    pub(crate) async fn handle_journal_entry_set(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: JournalEntrySetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "JournalEntry").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_journal_entry_input(None, account_id, value) {
                    Ok(input) => match self.store.upsert_jmap_journal_entry(input).await {
                        Ok(entry) => {
                            created_ids.insert(creation_id.clone(), entry.id.to_string());
                            created.insert(creation_id, json!({ "id": entry.id.to_string() }));
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
                match parse_uuid(&id).and_then(|entry_id| {
                    parse_journal_entry_input(Some(entry_id), account_id, value)
                }) {
                    Ok(input) => match self.store.upsert_jmap_journal_entry(input).await {
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
                    Ok(entry_id) => {
                        match self
                            .store
                            .delete_jmap_journal_entry(account_id, entry_id)
                            .await
                        {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": self.object_state(account_id, "JournalEntry").await?,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    pub(crate) async fn handle_reminder_query(
        &self,
        account: &lpe_storage::AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ReminderQueryArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let reminders = self
            .store
            .query_jmap_reminders(
                account_id,
                ReminderQuery {
                    include_inactive: arguments.include_inactive.unwrap_or(false),
                },
            )
            .await?;
        let all_ids = reminders.iter().map(reminder_id).collect::<Vec<_>>();
        let position = query_position(
            &all_ids,
            arguments.position,
            arguments.anchor.as_deref(),
            arguments.anchor_offset,
        )?;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let list = reminders
            .iter()
            .skip(position)
            .take(limit)
            .map(reminder_to_value)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": crate::encode_query_state(
                account_id,
                "Reminder",
                Some(json!({"includeInactive": arguments.include_inactive.unwrap_or(false)})),
                None,
                all_ids,
            )?,
            "canCalculateChanges": false,
            "position": position,
            "list": list,
            "total": reminders.len(),
        }))
    }
}

fn note_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "@type".to_string(),
                "title".to_string(),
                "bodyText".to_string(),
                "color".to_string(),
                "categoriesJson".to_string(),
                "created".to_string(),
                "updated".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn journal_entry_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "@type".to_string(),
                "subject".to_string(),
                "bodyText".to_string(),
                "entryType".to_string(),
                "messageClass".to_string(),
                "startsAt".to_string(),
                "endsAt".to_string(),
                "occurredAt".to_string(),
                "companiesJson".to_string(),
                "contactsJson".to_string(),
                "created".to_string(),
                "updated".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn note_to_value(note: &ClientNote, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", note.id.to_string());
    insert_if(properties, &mut object, "@type", "Note");
    insert_if(properties, &mut object, "title", note.title.clone());
    insert_if(properties, &mut object, "bodyText", note.body_text.clone());
    insert_if(properties, &mut object, "color", note.color.clone());
    insert_if(
        properties,
        &mut object,
        "categoriesJson",
        note.categories_json.clone(),
    );
    insert_if(properties, &mut object, "created", note.created_at.clone());
    insert_if(properties, &mut object, "updated", note.updated_at.clone());
    Value::Object(object)
}

fn journal_entry_to_value(entry: &JournalEntry, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", entry.id.to_string());
    insert_if(properties, &mut object, "@type", "JournalEntry");
    insert_if(properties, &mut object, "subject", entry.subject.clone());
    insert_if(properties, &mut object, "bodyText", entry.body_text.clone());
    insert_if(
        properties,
        &mut object,
        "entryType",
        entry.entry_type.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "messageClass",
        entry.message_class.clone(),
    );
    insert_if(properties, &mut object, "startsAt", entry.starts_at.clone());
    insert_if(properties, &mut object, "endsAt", entry.ends_at.clone());
    insert_if(
        properties,
        &mut object,
        "occurredAt",
        entry.occurred_at.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "companiesJson",
        entry.companies_json.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "contactsJson",
        entry.contacts_json.clone(),
    );
    insert_if(properties, &mut object, "created", entry.created_at.clone());
    insert_if(properties, &mut object, "updated", entry.updated_at.clone());
    Value::Object(object)
}

pub(crate) fn note_state_fingerprint(note: &ClientNote) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        note.title,
        note.body_text,
        note.color,
        note.categories_json,
        note.created_at,
        note.updated_at
    )
}

pub(crate) fn journal_entry_state_fingerprint(entry: &JournalEntry) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        entry.subject,
        entry.body_text,
        entry.entry_type,
        entry.message_class,
        entry.starts_at.as_deref().unwrap_or_default(),
        entry.ends_at.as_deref().unwrap_or_default(),
        entry.occurred_at.as_deref().unwrap_or_default(),
        entry.companies_json,
        entry.contacts_json,
        entry.updated_at
    )
}

pub(crate) fn reminder_state_fingerprint(reminder: &ClientReminder) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        reminder.source_type,
        reminder.title,
        reminder.due_at.as_deref().unwrap_or_default(),
        reminder.reminder_at,
        reminder.dismissed_at.as_deref().unwrap_or_default(),
        reminder.completed_at.as_deref().unwrap_or_default(),
        reminder.status
    )
}

fn reminder_to_value(reminder: &ClientReminder) -> Value {
    json!({
        "id": reminder_id(reminder),
        "@type": "Reminder",
        "sourceType": reminder.source_type,
        "sourceId": reminder.source_id.to_string(),
        "title": reminder.title,
        "dueAt": reminder.due_at,
        "reminderAt": reminder.reminder_at,
        "dismissedAt": reminder.dismissed_at,
        "completedAt": reminder.completed_at,
        "status": reminder.status,
    })
}

fn reminder_id(reminder: &ClientReminder) -> String {
    format!("{}:{}", reminder.source_type, reminder.source_id)
}

fn note_matches_filter(note: &ClientNote, filter: &NoteQueryFilter) -> bool {
    if let Some(text) = filter.text.as_deref() {
        let text = text.trim().to_ascii_lowercase();
        if !text.is_empty()
            && !note.title.to_ascii_lowercase().contains(&text)
            && !note.body_text.to_ascii_lowercase().contains(&text)
        {
            return false;
        }
    }
    true
}

fn journal_entry_matches_filter(entry: &JournalEntry, filter: &JournalEntryQueryFilter) -> bool {
    if let Some(entry_type) = filter.entry_type.as_deref() {
        if entry.entry_type != entry_type.trim().to_ascii_lowercase() {
            return false;
        }
    }
    if let Some(text) = filter.text.as_deref() {
        let text = text.trim().to_ascii_lowercase();
        if !text.is_empty()
            && !entry.subject.to_ascii_lowercase().contains(&text)
            && !entry.body_text.to_ascii_lowercase().contains(&text)
        {
            return false;
        }
    }
    true
}

fn parse_note_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<UpsertClientNoteInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("note arguments must be an object"))?;
    Ok(UpsertClientNoteInput {
        id,
        account_id,
        title: parse_required_string(object.get("title"), "title")?,
        body_text: parse_optional_string(object.get("bodyText"))?.unwrap_or_default(),
        color: parse_optional_string(object.get("color"))?.unwrap_or_default(),
        categories_json: parse_optional_string(object.get("categoriesJson"))?
            .unwrap_or_else(|| "[]".to_string()),
    })
}

fn parse_journal_entry_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<UpsertJournalEntryInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("journal entry arguments must be an object"))?;
    Ok(UpsertJournalEntryInput {
        id,
        account_id,
        subject: parse_required_string(object.get("subject"), "subject")?,
        body_text: parse_optional_string(object.get("bodyText"))?.unwrap_or_default(),
        entry_type: parse_optional_string(object.get("entryType"))?.unwrap_or_default(),
        message_class: parse_optional_string(object.get("messageClass"))?
            .unwrap_or_else(|| "IPM.Activity".to_string()),
        starts_at: parse_optional_string(object.get("startsAt"))?,
        ends_at: parse_optional_string(object.get("endsAt"))?,
        occurred_at: parse_optional_string(object.get("occurredAt"))?,
        companies_json: parse_optional_string(object.get("companiesJson"))?
            .unwrap_or_else(|| "[]".to_string()),
        contacts_json: parse_optional_string(object.get("contactsJson"))?
            .unwrap_or_else(|| "[]".to_string()),
    })
}
