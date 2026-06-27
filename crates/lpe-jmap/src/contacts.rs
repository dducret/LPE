use anyhow::{anyhow, bail, Result};
use lpe_domain::normalization;
use lpe_storage::{
    AccessibleContact, AuthenticatedAccount, CollaborationCollection, ContactNameFields,
    ContactSourceFields, RecipientSuggestion, UpsertClientContactInput,
};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    convert::{apply_jmap_property_patch, has_jmap_property_patch, insert_if},
    error::set_error,
    parse::{parse_uuid, parse_uuid_list},
    protocol::{
        AddressBookGetArguments, AddressBookQueryArguments, ChangesArguments,
        ContactCardGetArguments, ContactCardQueryArguments, ContactCardQueryFilter,
        ContactCardSetArguments, EntityQuerySort, QueryChangesArguments,
        RecipientSuggestionQueryArguments,
    },
    state::{query_changes_response, query_position, StateEntry},
    validation::{validate_contact_filter, validate_entity_sort},
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
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
        let requested_ids = arguments.ids;
        let mut collections = self
            .store
            .fetch_accessible_contact_collections(account_id)
            .await?;
        collections.sort_by_key(collection_sort_key);
        let list = collections
            .iter()
            .filter(|collection| {
                requested_ids
                    .as_ref()
                    .map(|ids| ids.contains(&collection.id))
                    .unwrap_or(true)
            })
            .map(|collection| address_book_to_value(collection, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
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
        let mut collections = self
            .store
            .fetch_accessible_contact_collections(account_id)
            .await?;
        collections.sort_by_key(collection_sort_key);
        let all_ids = collections
            .iter()
            .map(|collection| collection.id.clone())
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
                "AddressBook/query",
                None,
                None,
                all_ids,
            )?,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": collections.len(),
        }))
    }

    pub(crate) async fn handle_address_book_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        reject_collection_query_constraints(
            arguments.filter.as_ref(),
            arguments.sort.as_ref(),
            "AddressBook/query",
        )?;
        let mut collections = self
            .store
            .fetch_accessible_contact_collections(account_id)
            .await?;
        collections.sort_by_key(collection_sort_key);
        query_changes_response(
            account_id,
            "AddressBook/query",
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
        self.object_changes_response(
            account_id,
            "AddressBook",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
        .await
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
        contacts.sort_by_key(|contact| (contact.name.to_lowercase(), contact.id.to_string()));

        let all_ids = contacts
            .iter()
            .map(|contact| contact.id.to_string())
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
                "ContactCard",
                arguments.filter.map(serde_json::to_value).transpose()?,
                serialize_entity_query_sort(arguments.sort)?,
                all_ids,
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
        contacts.sort_by_key(|contact| (contact.name.to_lowercase(), contact.id.to_string()));

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
        self.object_changes_response(
            account_id,
            "ContactCard",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        )
        .await
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
                match parse_uuid(&id) {
                    Ok(contact_id) => match self
                        .contact_update_input(account_id, contact_id, value)
                        .await
                        .map(|input| (contact_id, input))
                    {
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

    pub(crate) async fn handle_recipient_suggestion_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: RecipientSuggestionQueryArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let suggestions = self
            .store
            .query_recipient_suggestions(account_id, arguments.query.as_deref())
            .await?;
        let all_ids = suggestions
            .iter()
            .map(|suggestion| suggestion.id.to_string())
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

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": crate::encode_query_state(
                account_id,
                "RecipientSuggestion",
                arguments.query.map(Value::String),
                None,
                all_ids.clone(),
            )?,
            "canCalculateChanges": false,
            "position": position,
            "ids": all_ids.iter().skip(position).take(limit).cloned().collect::<Vec<_>>(),
            "total": suggestions.len(),
            "list": suggestions
                .iter()
                .skip(position)
                .take(limit)
                .map(recipient_suggestion_to_value)
                .collect::<Vec<_>>(),
        }))
    }

    async fn contact_update_input(
        &self,
        account_id: Uuid,
        contact_id: Uuid,
        value: Value,
    ) -> Result<UpsertClientContactInput> {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("contact card arguments must be an object"))?;
        if !has_jmap_property_patch(object) {
            return parse_contact_input(Some(contact_id), account_id, value)
                .map(|(_, input)| input);
        }

        let existing = self
            .store
            .fetch_accessible_contacts_by_ids(account_id, &[contact_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("contact not found"))?;
        let mut patched = contact_to_value(&existing, &contact_properties(None));
        apply_jmap_property_patch(&mut patched, object)?;
        parse_contact_input(Some(contact_id), account_id, patched).map(|(_, input)| input)
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
                "addresses".to_string(),
                "onlineServices".to_string(),
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
                "prefix": contact.structured_name.prefix,
                "given": contact.structured_name.given,
                "middle": contact.structured_name.middle,
                "family": contact.structured_name.family,
                "suffix": contact.structured_name.suffix,
                "nickname": contact.structured_name.nickname,
                "phoneticGiven": contact.structured_name.phonetic_given,
                "phoneticFamily": contact.structured_name.phonetic_family,
            }),
        );
    }
    if properties.contains("emails") {
        insert_non_empty_object(
            &mut object,
            "emails",
            contact_array_to_named_object(&contact.emails_json, "email", "address"),
        );
    }
    if properties.contains("phones") {
        insert_non_empty_object(
            &mut object,
            "phones",
            contact_array_to_named_object(&contact.phones_json, "phone", "number"),
        );
    }
    if properties.contains("addresses") {
        insert_non_empty_object(
            &mut object,
            "addresses",
            contact_array_to_named_object(&contact.addresses_json, "address", "full"),
        );
    }
    if properties.contains("onlineServices") {
        insert_non_empty_object(
            &mut object,
            "onlineServices",
            contact_array_to_named_object(&contact.urls_json, "url", "uri"),
        );
    }
    if properties.contains("organizations")
        && (!contact.team.trim().is_empty() || !contact.organization_name.trim().is_empty())
    {
        object.insert(
            "organizations".to_string(),
            json!({
                "main": {
                    "@type": "Organization",
                    "name": if contact.organization_name.trim().is_empty() {
                        contact.team.clone()
                    } else {
                        contact.organization_name.clone()
                    },
                    "unit": contact.team,
                    "contexts": {"work": true},
                }
            }),
        );
    }
    if properties.contains("titles")
        && (!contact.role.trim().is_empty() || !contact.job_title.trim().is_empty())
    {
        object.insert(
            "titles".to_string(),
            json!({
                "main": {
                    "@type": "Title",
                    "kind": "title",
                    "name": if contact.job_title.trim().is_empty() {
                        contact.role.clone()
                    } else {
                        contact.job_title.clone()
                    },
                    "role": contact.role,
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

fn recipient_suggestion_to_value(suggestion: &RecipientSuggestion) -> Value {
    json!({
        "id": suggestion.id.to_string(),
        "@type": "RecipientSuggestion",
        "email": suggestion.email,
        "displayName": suggestion.display_name,
        "sourceKind": suggestion.source_kind,
        "useCount": suggestion.use_count,
        "lastUsedAt": suggestion.last_used_at,
        "contactId": suggestion.contact_id.map(|id| id.to_string()),
    })
}

fn insert_non_empty_object(object: &mut Map<String, Value>, key: &str, value: Value) {
    if value.as_object().is_some_and(|object| !object.is_empty()) {
        object.insert(key.to_string(), value);
    }
}

fn contact_array_to_named_object(value: &Value, source_key: &str, target_key: &str) -> Value {
    let mut object = Map::new();
    let Some(items) = value.as_array() else {
        return Value::Object(object);
    };
    for (index, item) in items.iter().enumerate() {
        let Some(source) = item.as_object() else {
            continue;
        };
        let mut entry = source.clone();
        if let Some(value) = entry.remove(source_key) {
            entry.insert(target_key.to_string(), value);
        }
        entry.entry("@type".to_string()).or_insert_with(|| {
            Value::String(
                match source_key {
                    "email" => "EmailAddress",
                    "phone" => "Phone",
                    "url" => "OnlineService",
                    _ => "Address",
                }
                .to_string(),
            )
        });
        let key = if index == 0 {
            "main".to_string()
        } else {
            format!("item{}", index + 1)
        };
        object.insert(key, Value::Object(entry));
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
                "{} {} {} {} {} {} {} {} {} {} {} {}",
                contact.name,
                contact.email,
                contact.role,
                contact.phone,
                contact.team,
                contact.notes,
                contact.structured_name.nickname,
                contact.emails_json,
                contact.phones_json,
                contact.addresses_json,
                contact.urls_json,
                contact.organization_name,
            )
            .to_lowercase();
            if !haystack.contains(&needle) {
                return false;
            }
        }
    }
    true
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
            structured_name: parse_contact_name_fields(object.get("name"))?,
            emails_json: Some(parse_contact_property_array(
                object.get("emails"),
                "address",
                "email",
            )?),
            phones_json: Some(parse_contact_property_array(
                object.get("phones"),
                "number",
                "phone",
            )?),
            addresses_json: Some(parse_contact_property_array(
                object.get("addresses"),
                "full",
                "address",
            )?),
            urls_json: Some(parse_contact_property_array(
                object.get("onlineServices").or_else(|| object.get("urls")),
                "uri",
                "url",
            )?),
            organization_name: parse_contact_organization_name(object.get("organizations"))?,
            job_title: parse_contact_job_title(object.get("titles"))?,
            raw_vcard: object
                .get("rawVCard")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            raw_vcard_is_explicit: object.contains_key("rawVCard"),
            source: ContactSourceFields {
                import_source: "jmap".to_string(),
                source_uid: object
                    .get("uid")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                source_etag: None,
                source_payload_json: object
                    .get("source")
                    .cloned()
                    .unwrap_or_else(|| Value::Object(Map::new())),
            },
            source_is_explicit: object.contains_key("source") || object.contains_key("uid"),
        },
    ))
}

fn reject_unknown_contact_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "id" | "uid" | "kind" | "name" | "emails" | "phones" | "addresses"
            | "onlineServices" | "urls" | "organizations" | "titles" | "notes"
            | "addressBookIds" | "rawVCard" | "source" => {}
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

fn parse_contact_name_fields(value: Option<&Value>) -> Result<ContactNameFields> {
    let Some(value) = value else {
        return Ok(ContactNameFields::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("name must be an object"))?;
    let family = {
        let family = contact_object_string(object, "family");
        if family.is_empty() {
            contact_object_string(object, "surname")
        } else {
            family
        }
    };
    Ok(ContactNameFields {
        prefix: contact_object_string(object, "prefix"),
        given: contact_object_string(object, "given"),
        middle: contact_object_string(object, "middle"),
        family,
        suffix: contact_object_string(object, "suffix"),
        nickname: contact_object_string(object, "nickname"),
        phonetic_given: contact_object_string(object, "phoneticGiven"),
        phonetic_family: contact_object_string(object, "phoneticFamily"),
    })
}

fn contact_object_string(object: &Map<String, Value>, key: &str) -> String {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_string()
}

fn parse_contact_email(value: Option<&Value>) -> Result<String> {
    parse_contact_property_string(value, "emails", "address")
        .map(|email| normalization::normalize_trimmed_lowercase(&email))
}

fn parse_contact_phone(value: Option<&Value>) -> Result<String> {
    parse_contact_property_string(value, "phones", "number")
}

fn parse_contact_organization(value: Option<&Value>) -> Result<String> {
    parse_contact_property_string(value, "organizations", "unit").and_then(|unit| {
        if unit.is_empty() {
            parse_contact_property_string(value, "organizations", "name")
        } else {
            Ok(unit)
        }
    })
}

fn parse_contact_title(value: Option<&Value>) -> Result<String> {
    parse_contact_property_string(value, "titles", "role").and_then(|role| {
        if role.is_empty() {
            parse_contact_property_string(value, "titles", "name")
        } else {
            Ok(role)
        }
    })
}

fn parse_contact_note(value: Option<&Value>) -> Result<String> {
    parse_contact_property_string(value, "notes", "note")
}

fn parse_contact_organization_name(value: Option<&Value>) -> Result<String> {
    parse_contact_property_string(value, "organizations", "name")
}

fn parse_contact_job_title(value: Option<&Value>) -> Result<String> {
    parse_contact_property_string(value, "titles", "name")
}

fn parse_contact_property_string(
    value: Option<&Value>,
    property_name: &str,
    field_name: &str,
) -> Result<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("{property_name} must be an object"))?;
    let first = object
        .get("main")
        .or_else(|| object.values().next())
        .cloned();
    let Some(first) = first else {
        return Ok(String::new());
    };
    let first = first
        .as_object()
        .ok_or_else(|| anyhow!("{property_name} entries must be objects"))?;
    Ok(first
        .get(field_name)
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_string())
}

fn parse_contact_property_array(
    value: Option<&Value>,
    source_key: &str,
    target_key: &str,
) -> Result<Value> {
    let Some(value) = value else {
        return Ok(Value::Array(Vec::new()));
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("contact property must be an object"))?;
    let mut items = Vec::new();
    if let Some(entry) = object.get("main") {
        items.push(parse_contact_property_entry(entry, source_key, target_key)?);
    }
    for (key, entry) in object {
        if key == "main" {
            continue;
        }
        items.push(parse_contact_property_entry(entry, source_key, target_key)?);
    }
    Ok(Value::Array(items))
}

fn parse_contact_property_entry(
    entry: &Value,
    source_key: &str,
    target_key: &str,
) -> Result<Value> {
    let entry = entry
        .as_object()
        .ok_or_else(|| anyhow!("contact property entries must be objects"))?;
    let mut output = entry.clone();
    if let Some(value) = output.remove(source_key) {
        output.insert(target_key.to_string(), value);
    }
    Ok(Value::Object(output))
}
