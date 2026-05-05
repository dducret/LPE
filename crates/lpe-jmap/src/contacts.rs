use anyhow::{anyhow, bail, Result};
use lpe_storage::{
    AccessibleContact, AuthenticatedAccount, CollaborationCollection, UpsertClientContactInput,
};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    convert::insert_if,
    error::set_error,
    parse::{parse_first_property_object_string, parse_uuid, parse_uuid_list},
    protocol::{
        AddressBookGetArguments, AddressBookQueryArguments, ChangesArguments,
        ContactCardGetArguments, ContactCardQueryArguments, ContactCardQueryFilter,
        ContactCardSetArguments, EntityQuerySort, QueryChangesArguments,
    },
    state::{changes_response, query_changes_response, StateEntry},
    validation::{validate_contact_filter, validate_entity_sort},
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT, SESSION_STATE,
};

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_address_book_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: AddressBookGetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = address_book_properties(arguments.properties);
        let requested_ids = arguments.ids.unwrap_or_default();
        let collections = self
            .store
            .fetch_accessible_contact_collections(account_id)
            .await?;
        let list = collections
            .iter()
            .filter(|collection| requested_ids.is_empty() || requested_ids.contains(&collection.id))
            .map(|collection| address_book_to_value(collection, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .into_iter()
            .filter(|id| !collections.iter().any(|collection| collection.id == *id))
            .map(Value::String)
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "AddressBook").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_address_book_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: AddressBookQueryArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let collections = self
            .store
            .fetch_accessible_contact_collections(account_id)
            .await?;
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
            "queryState": SESSION_STATE,
            "canCalculateChanges": false,
            "position": position,
            "ids": ids,
            "total": collections.len(),
        }))
    }

    pub(crate) async fn handle_address_book_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let collections = self
            .store
            .fetch_accessible_contact_collections(account_id)
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
            "AddressBook",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
    }

    pub(crate) async fn handle_contact_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ContactCardGetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = contact_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let contacts = if let Some(ids) = requested_ids.as_ref() {
            self.store
                .fetch_accessible_contacts_by_ids(account_id, &ids)
                .await?
        } else {
            self.store.fetch_accessible_contacts(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();

        let list = contacts
            .iter()
            .filter(|contact| requested_ids.is_none() || requested_set.contains(&contact.id))
            .map(|contact| contact_to_value(contact, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !contacts.iter().any(|contact| contact.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "ContactCard").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_contact_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ContactCardQueryArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_entity_sort(arguments.sort.as_deref(), "name", true)?;
        validate_contact_filter(arguments.filter.as_ref())?;

        let mut contacts = self.store.fetch_accessible_contacts(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            contacts.retain(|contact| contact_matches_filter(contact, filter));
        }
        contacts.sort_by_key(|contact| contact.name.to_lowercase());

        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = contacts
            .iter()
            .skip(position)
            .take(limit)
            .map(|contact| contact.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": crate::encode_query_state(
                account_id,
                "ContactCard",
                arguments.filter.map(serde_json::to_value).transpose()?,
                serialize_entity_query_sort(arguments.sort)?,
                contacts.iter().map(|contact| contact.id.to_string()).collect(),
            )?,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": contacts.len(),
        }))
    }

    pub(crate) async fn handle_contact_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<ContactCardQueryFilter, EntityQuerySort> =
            serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_entity_sort(arguments.sort.as_deref(), "name", true)?;
        validate_contact_filter(arguments.filter.as_ref())?;

        let mut contacts = self.store.fetch_accessible_contacts(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            contacts.retain(|contact| contact_matches_filter(contact, filter));
        }
        contacts.sort_by_key(|contact| contact.name.to_lowercase());

        query_changes_response(
            account_id,
            "ContactCard",
            arguments.since_query_state,
            arguments.filter.map(serde_json::to_value).transpose()?,
            serialize_entity_query_sort(arguments.sort)?,
            contacts
                .iter()
                .map(|contact| contact.id.to_string())
                .collect(),
            contacts.len() as u64,
            arguments.max_changes,
        )
    }

    pub(crate) async fn handle_contact_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self.object_state_entries(account_id, "ContactCard").await?;
        changes_response(
            account_id,
            "ContactCard",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
    }

    pub(crate) async fn handle_contact_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: ContactCardSetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "ContactCard").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_contact_input(None, account_id, value) {
                    Ok((collection_id, input)) => match self
                        .store
                        .create_accessible_contact(account_id, collection_id.as_deref(), input)
                        .await
                    {
                        Ok(contact) => {
                            created_ids.insert(creation_id.clone(), contact.id.to_string());
                            created.insert(creation_id, json!({ "id": contact.id.to_string() }));
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
                match parse_uuid(&id).and_then(|contact_id| {
                    parse_contact_input(Some(contact_id), account_id, value)
                        .map(|(_, input)| (contact_id, input))
                }) {
                    Ok((contact_id, input)) => match self
                        .store
                        .update_accessible_contact(account_id, contact_id, input)
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
                    Ok(contact_id) => match self
                        .store
                        .delete_accessible_contact(account_id, contact_id)
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

        let new_state = self.object_state(account_id, "ContactCard").await?;
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

fn address_book_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "sortOrder".to_string(),
                "isSubscribed".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn address_book_to_value(
    collection: &CollaborationCollection,
    properties: &HashSet<String>,
) -> Value {
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

fn contact_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "uid".to_string(),
                "kind".to_string(),
                "name".to_string(),
                "emails".to_string(),
                "phones".to_string(),
                "organizations".to_string(),
                "titles".to_string(),
                "notes".to_string(),
                "addressBookIds".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn contact_to_value(contact: &AccessibleContact, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", contact.id.to_string());
    insert_if(properties, &mut object, "uid", contact.id.to_string());
    insert_if(properties, &mut object, "kind", "individual");
    if properties.contains("name") {
        object.insert(
            "name".to_string(),
            json!({
                "@type": "Name",
                "full": contact.name,
            }),
        );
    }
    if properties.contains("emails") && !contact.email.trim().is_empty() {
        object.insert(
            "emails".to_string(),
            json!({
                "main": {
                    "@type": "EmailAddress",
                    "address": contact.email,
                    "contexts": {"work": true},
                    "pref": 1,
                }
            }),
        );
    }
    if properties.contains("phones") && !contact.phone.trim().is_empty() {
        object.insert(
            "phones".to_string(),
            json!({
                "main": {
                    "@type": "Phone",
                    "number": contact.phone,
                    "contexts": {"work": true},
                }
            }),
        );
    }
    if properties.contains("organizations") && !contact.team.trim().is_empty() {
        object.insert(
            "organizations".to_string(),
            json!({
                "main": {
                    "@type": "Organization",
                    "name": contact.team,
                    "contexts": {"work": true},
                }
            }),
        );
    }
    if properties.contains("titles") && !contact.role.trim().is_empty() {
        object.insert(
            "titles".to_string(),
            json!({
                "main": {
                    "@type": "Title",
                    "kind": "title",
                    "name": contact.role,
                }
            }),
        );
    }
    if properties.contains("notes") && !contact.notes.trim().is_empty() {
        object.insert(
            "notes".to_string(),
            json!({
                "main": {
                    "@type": "Note",
                    "note": contact.notes,
                }
            }),
        );
    }
    if properties.contains("addressBookIds") {
        object.insert(
            "addressBookIds".to_string(),
            json!({contact.collection_id.clone(): true}),
        );
    }
    Value::Object(object)
}

fn contact_matches_filter(contact: &AccessibleContact, filter: &ContactCardQueryFilter) -> bool {
    if let Some(address_book_id) = filter.in_address_book.as_deref() {
        if address_book_id != contact.collection_id {
            return false;
        }
    }
    if let Some(text) = filter.text.as_deref() {
        let needle = text.trim().to_lowercase();
        if !needle.is_empty() {
            let haystack = format!(
                "{} {} {} {} {} {}",
                contact.name,
                contact.email,
                contact.role,
                contact.phone,
                contact.team,
                contact.notes
            )
            .to_lowercase();
            if !haystack.contains(&needle) {
                return false;
            }
        }
    }
    true
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

fn parse_contact_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<(Option<String>, UpsertClientContactInput)> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("contact card arguments must be an object"))?;
    reject_unknown_contact_properties(object)?;
    let collection_id = validate_address_book_ids(object.get("addressBookIds"))?;

    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("individual");
    if kind != "individual" {
        bail!("only kind=individual is supported");
    }

    Ok((
        collection_id,
        UpsertClientContactInput {
            id,
            account_id,
            name: parse_contact_name(object.get("name"))?,
            role: parse_contact_title(object.get("titles"))?,
            email: parse_contact_email(object.get("emails"))?,
            phone: parse_contact_phone(object.get("phones"))?,
            team: parse_contact_organization(object.get("organizations"))?,
            notes: parse_contact_note(object.get("notes"))?,
        },
    ))
}

fn reject_unknown_contact_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "uid" | "kind" | "name" | "emails" | "phones" | "organizations" | "titles"
            | "notes" | "addressBookIds" => {}
            _ => bail!("unsupported contact card property: {key}"),
        }
    }
    Ok(())
}

fn validate_address_book_ids(value: Option<&Value>) -> Result<Option<String>> {
    if let Some(value) = value {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("addressBookIds must be an object"))?;
        if object.len() != 1 {
            bail!("exactly one addressBookId must be provided");
        }
        let (collection_id, enabled) = object.iter().next().unwrap();
        if enabled.as_bool() != Some(true) {
            bail!("addressBookIds entries must be true");
        }
        return Ok(Some(collection_id.clone()));
    }
    Ok(None)
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}

fn parse_contact_name(value: Option<&Value>) -> Result<String> {
    let value = value.ok_or_else(|| anyhow!("name is required"))?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("name must be an object"))?;
    if let Some(full) = object.get("full").and_then(Value::as_str) {
        let full = full.trim();
        if full.is_empty() {
            bail!("name.full is required");
        }
        return Ok(full.to_string());
    }
    bail!("name.full is required")
}

fn parse_contact_email(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "emails", "address")
        .map(|email| normalize_email(&email))
}

fn parse_contact_phone(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "phones", "number")
}

fn parse_contact_organization(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "organizations", "name")
}

fn parse_contact_title(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "titles", "name")
}

fn parse_contact_note(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "notes", "note")
}
