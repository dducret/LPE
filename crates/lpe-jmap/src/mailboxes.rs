use anyhow::{anyhow, bail, Result};
use lpe_domain::{MailboxNamePolicy, MailboxSegment};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use lpe_storage::{
    AuditEntryInput, AuthenticatedAccount, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, MailboxAccountAccess,
};

use crate::{
    convert::insert_if,
    error::set_error,
    parse::{parse_uuid, parse_uuid_list},
    protocol::{
        ChangesArguments, MailboxCreateInput, MailboxGetArguments, MailboxQueryArguments,
        MailboxSetArguments, MailboxUpdateInput, QueryChangesArguments,
    },
    state::{
        changes_response_from_durable_with_cursor, changes_response_with_cursor,
        decode_query_state, encode_query_state, encode_query_state_reference,
        query_changes_response_from_diff, query_diff_for_kind, query_position, state_cursor,
        validate_query_state_token, DurableObjectChange,
    },
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
};

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_mailbox_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: MailboxGetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let properties = mailbox_properties(arguments.properties);
        let mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        let mailbox_ids = mailboxes
            .iter()
            .map(|mailbox| mailbox.id)
            .collect::<HashSet<_>>();

        let requested_ids = parse_uuid_list(arguments.ids)?;
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();

        let list = mailboxes
            .iter()
            .filter(|mailbox| requested_ids.is_none() || requested_set.contains(&mailbox.id))
            .map(|mailbox| mailbox_to_value(mailbox, &account_access, &properties))
            .collect::<Vec<_>>();

        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !mailbox_ids.contains(id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.mailbox_object_state(&account_access).await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_mailbox_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: MailboxQueryArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let filter_state = arguments.filter.clone();
        let sort_state = arguments.sort.clone();
        let mut mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        mailboxes = filter_mailboxes(mailboxes, filter_state.as_ref())?;
        mailboxes.sort_by_key(|mailbox| {
            (
                mailbox.sort_order,
                mailbox.name.to_lowercase(),
                mailbox.id.to_string(),
            )
        });
        let all_ids = mailboxes
            .iter()
            .map(|mailbox| mailbox.id.to_string())
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
        let cursor = self
            .store
            .fetch_jmap_mail_change_cursor(account_id)
            .await?
            .unwrap_or(0);
        let query_state = match self
            .store
            .save_jmap_query_state(
                account_id,
                "Mailbox/query",
                filter_state.clone(),
                sort_state.clone(),
                cursor,
                &all_ids,
            )
            .await?
        {
            Some(state_id) => encode_query_state_reference(
                account_id,
                "Mailbox/query",
                filter_state.clone(),
                sort_state.clone(),
                state_id,
                cursor,
            )?,
            None => encode_query_state(
                account_id,
                "Mailbox/query",
                filter_state.clone(),
                sort_state.clone(),
                all_ids,
            )?,
        };

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": query_state,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": mailboxes.len(),
        }))
    }

    pub(crate) async fn handle_mailbox_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let previous = decode_query_state(&arguments.since_query_state)?;
        let filter_state = arguments.filter;
        let sort_state = arguments
            .sort
            .map(|sort| sort.into_iter().collect::<Vec<_>>());
        validate_query_state_token(
            account_id,
            "Mailbox/query",
            filter_state.as_ref(),
            sort_state.as_ref(),
            &previous,
        )?;
        let mut previous_cursor = previous.cursor.unwrap_or(0);
        let previous_ids =
            if let Some(state_id) = previous.state_id.as_deref().map(parse_uuid).transpose()? {
                let stored = self
                    .store
                    .fetch_jmap_query_state(
                        account_id,
                        "Mailbox/query",
                        state_id,
                        filter_state.clone(),
                        sort_state.clone(),
                    )
                    .await?
                    .ok_or_else(|| anyhow!("queryState is no longer available"))?;
                previous_cursor = stored.last_change_sequence;
                stored.snapshot_ids
            } else {
                previous.ids.clone()
            };
        let mut mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        mailboxes = filter_mailboxes(mailboxes, filter_state.as_ref())?;
        mailboxes.sort_by_key(|mailbox| {
            (
                mailbox.sort_order,
                mailbox.name.to_lowercase(),
                mailbox.id.to_string(),
            )
        });
        let current_ids = mailboxes
            .into_iter()
            .map(|mailbox| mailbox.id.to_string())
            .collect::<Vec<_>>();
        let total = current_ids.len() as u64;
        let cursor = self
            .store
            .fetch_jmap_mail_change_cursor(account_id)
            .await?
            .unwrap_or(0);
        let diff = query_diff_for_kind(
            "Mailbox/query",
            &previous_ids,
            &current_ids,
            arguments.max_changes,
        );
        let next_cursor = if diff.has_more_changes {
            previous_cursor
        } else {
            cursor
        };
        let next_query_state = match self
            .store
            .save_jmap_query_state(
                account_id,
                "Mailbox/query",
                filter_state.clone(),
                sort_state.clone(),
                next_cursor,
                &diff.query_state_ids,
            )
            .await?
        {
            Some(state_id) => encode_query_state_reference(
                account_id,
                "Mailbox/query",
                filter_state.clone(),
                sort_state.clone(),
                state_id,
                next_cursor,
            )?,
            None => encode_query_state(
                account_id,
                "Mailbox/query",
                filter_state.clone(),
                sort_state.clone(),
                diff.query_state_ids.clone(),
            )?,
        };
        query_changes_response_from_diff(
            account_id,
            "Mailbox/query",
            arguments.since_query_state,
            filter_state,
            sort_state,
            previous,
            next_query_state,
            total,
            diff,
        )
    }

    pub(crate) async fn handle_mailbox_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let entries = self.mailbox_object_state_entries(&account_access).await?;
        let cursor = self.store.fetch_jmap_mail_change_cursor(account_id).await?;
        if let Some(after_cursor) = state_cursor(account_id, "Mailbox", &arguments.since_state)? {
            if let Some(changes) = self
                .store
                .replay_jmap_mail_object_changes(
                    account_id,
                    "Mailbox",
                    after_cursor,
                    crate::store::MAX_JMAP_MAIL_OBJECT_REPLAY_ROWS,
                )
                .await?
            {
                return changes_response_from_durable_with_cursor(
                    account_id,
                    "Mailbox",
                    &arguments.since_state,
                    arguments.max_changes,
                    entries,
                    cursor,
                    changes
                        .into_iter()
                        .map(|change| DurableObjectChange {
                            id: change.object_id.to_string(),
                        })
                        .collect(),
                );
            }
        }
        changes_response_with_cursor(
            account_id,
            "Mailbox",
            &arguments.since_state,
            arguments.max_changes,
            entries,
            cursor,
        )
    }

    pub(crate) async fn handle_mailbox_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: MailboxSetArguments = serde_json::from_value(arguments)?;
        let account_access = self
            .requested_account_access(account, arguments.account_id.as_deref())
            .await?;
        let account_id = account_access.account_id;
        let existing_mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        validate_mailbox_set_names(
            arguments.create.as_ref(),
            arguments.update.as_ref(),
            &existing_mailboxes,
        )?;
        let old_state = self.mailbox_object_state(&account_access).await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();
        let may_write = mailbox_account_may_write(&account_access);

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match ensure_mailbox_write(may_write).and_then(|_| parse_mailbox_create(value)) {
                    Ok(input) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-create".to_string(),
                            subject: creation_id.clone(),
                        };
                        match self
                            .store
                            .create_jmap_mailbox(
                                JmapMailboxCreateInput {
                                    account_id,
                                    name: input.name,
                                    parent_id: input.parent_id,
                                    sort_order: input.sort_order,
                                    is_subscribed: input.is_subscribed,
                                },
                                audit,
                            )
                            .await
                        {
                            Ok(mailbox) => {
                                created_ids.insert(creation_id.clone(), mailbox.id.to_string());
                                created.insert(creation_id, json!({"id": mailbox.id.to_string()}));
                            }
                            Err(error) => {
                                not_created.insert(creation_id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match ensure_mailbox_write(may_write).and_then(|_| {
                    parse_uuid(&id).and_then(|mailbox_id| {
                        parse_mailbox_update(value).map(|input| (mailbox_id, input))
                    })
                }) {
                    Ok((mailbox_id, input)) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-update".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .update_jmap_mailbox(
                                JmapMailboxUpdateInput {
                                    account_id,
                                    mailbox_id,
                                    name: input.name,
                                    parent_id: input.parent_id,
                                    sort_order: input.sort_order,
                                    is_subscribed: input.is_subscribed,
                                },
                                audit,
                            )
                            .await
                        {
                            Ok(_) => {
                                updated.insert(id, Value::Object(Map::new()));
                            }
                            Err(error) => {
                                not_updated.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match ensure_mailbox_write(may_write).and_then(|_| parse_uuid(&id)) {
                    Ok(mailbox_id) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-destroy".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .destroy_jmap_mailbox(account_id, mailbox_id, audit)
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

        let new_state = self.mailbox_object_state(&account_access).await?;
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

fn mailbox_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "parentId".to_string(),
                "role".to_string(),
                "sortOrder".to_string(),
                "totalEmails".to_string(),
                "unreadEmails".to_string(),
                "isSubscribed".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn mailbox_to_value(
    mailbox: &JmapMailbox,
    access: &MailboxAccountAccess,
    properties: &HashSet<String>,
) -> Value {
    let is_drafts = mailbox.role == "drafts";
    let may_draft = is_drafts && mailbox_account_may_draft(access);
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", mailbox.id.to_string());
    insert_if(properties, &mut object, "name", mailbox.name.clone());
    insert_if(
        properties,
        &mut object,
        "parentId",
        mailbox.parent_id.map(|id| id.to_string()),
    );
    insert_if(properties, &mut object, "role", mailbox.role.clone());
    insert_if(properties, &mut object, "sortOrder", mailbox.sort_order);
    insert_if(properties, &mut object, "totalEmails", mailbox.total_emails);
    insert_if(
        properties,
        &mut object,
        "unreadEmails",
        mailbox.unread_emails,
    );
    insert_if(
        properties,
        &mut object,
        "isSubscribed",
        mailbox.is_subscribed,
    );
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayReadItems": access.may_read,
                "mayAddItems": may_draft,
                "mayRemoveItems": may_draft,
                "maySetSeen": access.may_write,
                "maySetKeywords": access.may_write,
                "mayCreateChild": mailbox_account_may_write(access),
                "mayRename": false,
                "mayDelete": false,
                "maySubmit": is_drafts && mailbox_account_may_submit(access),
            }),
        );
    }
    Value::Object(object)
}

pub(crate) fn mailbox_account_may_submit(access: &MailboxAccountAccess) -> bool {
    access.is_owned || (access.may_write && (access.may_send_as || access.may_send_on_behalf))
}

pub(crate) fn mailbox_account_may_write(access: &MailboxAccountAccess) -> bool {
    access.is_owned || access.may_write
}

pub(crate) fn mailbox_account_may_draft(access: &MailboxAccountAccess) -> bool {
    mailbox_account_may_write(access) && mailbox_account_may_submit(access)
}

pub(crate) fn ensure_mailbox_write(may_write: bool) -> Result<()> {
    if may_write {
        Ok(())
    } else {
        bail!("write access is not granted on this mailbox account")
    }
}

pub(crate) fn ensure_mailbox_draft_write(access: &MailboxAccountAccess) -> Result<()> {
    ensure_mailbox_write(mailbox_account_may_write(access))?;
    if mailbox_account_may_submit(access) {
        Ok(())
    } else {
        bail!("sender delegation is required to write drafts in this mailbox account")
    }
}

fn parse_mailbox_create(value: Value) -> Result<MailboxCreateInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("mailbox create arguments must be an object"))?;
    let raw_name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("mailbox name is required"))?
        .to_string();
    let name = MailboxSegment::new(&raw_name)?.to_string();
    let parent_id = parse_parent_id_field(object.get("parentId"))?.flatten();
    let sort_order = object.get("sortOrder").and_then(Value::as_i64).unwrap_or(0) as i32;
    let is_subscribed = object
        .get("isSubscribed")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    Ok(MailboxCreateInput {
        name,
        parent_id,
        sort_order: Some(sort_order),
        is_subscribed,
    })
}

fn parse_mailbox_update(value: Value) -> Result<MailboxUpdateInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("mailbox update arguments must be an object"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(|value| MailboxSegment::new(value).map(|segment| segment.to_string()))
        .transpose()?;
    let parent_id = parse_parent_id_field(object.get("parentId"))?;
    let sort_order = object
        .get("sortOrder")
        .and_then(Value::as_i64)
        .map(|value| value as i32);
    let is_subscribed = object.get("isSubscribed").and_then(Value::as_bool);
    if name.is_none() && parent_id.is_none() && sort_order.is_none() && is_subscribed.is_none() {
        return Err(anyhow!(
            "mailbox update must include at least one mutable property"
        ));
    }
    Ok(MailboxUpdateInput {
        name,
        parent_id,
        sort_order,
        is_subscribed,
    })
}

fn parse_parent_id_field(value: Option<&Value>) -> Result<Option<Option<Uuid>>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(id)) => Ok(Some(Some(parse_uuid(id)?))),
        Some(_) => Err(anyhow!("mailbox parentId must be an id or null")),
    }
}

fn filter_mailboxes(
    mailboxes: Vec<JmapMailbox>,
    filter: Option<&Value>,
) -> Result<Vec<JmapMailbox>> {
    let Some(filter) = filter else {
        return Ok(mailboxes);
    };
    let object = filter
        .as_object()
        .ok_or_else(|| anyhow!("Mailbox/query filter must be an object"))?;
    let parent_id = parse_parent_id_field(object.get("parentId"))?;
    let name = object.get("name").and_then(Value::as_str);
    let role = object.get("role").and_then(Value::as_str);
    let has_any_role = object.get("hasAnyRole").and_then(Value::as_bool);
    let is_subscribed = object.get("isSubscribed").and_then(Value::as_bool);

    Ok(mailboxes
        .into_iter()
        .filter(|mailbox| {
            parent_id
                .as_ref()
                .is_none_or(|parent_id| mailbox.parent_id == *parent_id)
                && name.is_none_or(|name| mailbox.name.contains(name))
                && role.is_none_or(|role| {
                    if role.is_empty() {
                        mailbox.role.is_empty() || mailbox.role == "custom"
                    } else {
                        mailbox.role == role
                    }
                })
                && has_any_role.is_none_or(|expected| {
                    let has_role = !mailbox.role.is_empty() && mailbox.role != "custom";
                    has_role == expected
                })
                && is_subscribed.is_none_or(|expected| mailbox.is_subscribed == expected)
        })
        .collect())
}

fn validate_mailbox_set_names(
    create: Option<&HashMap<String, Value>>,
    update: Option<&HashMap<String, Value>>,
    existing_mailboxes: &[JmapMailbox],
) -> Result<()> {
    let mut requested = Vec::new();
    let existing_by_id = existing_mailboxes
        .iter()
        .map(|mailbox| (mailbox.id, mailbox))
        .collect::<HashMap<_, _>>();

    if let Some(create) = create {
        let mut entries = create.iter().collect::<Vec<_>>();
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (_, value) in entries {
            let object = value
                .as_object()
                .ok_or_else(|| anyhow!("mailbox create arguments must be an object"))?;
            let raw_name = mailbox_name_field(value, "mailbox create arguments must be an object")?;
            let name = MailboxSegment::new(raw_name)?.to_string();
            let parent_id = parse_parent_id_field(object.get("parentId"))?.flatten();
            if parent_id.is_some_and(|parent_id| !existing_by_id.contains_key(&parent_id)) {
                bail!("mailbox parentId must reference a mailbox in the same account");
            }
            requested.push((None, parent_id, MailboxNamePolicy::canonical_key(&name)));
        }
    }

    if let Some(update) = update {
        let mut entries = update.iter().collect::<Vec<_>>();
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (id, value) in entries {
            let object = value
                .as_object()
                .ok_or_else(|| anyhow!("mailbox update arguments must be an object"))?;
            let mailbox_id = parse_uuid(id).ok();
            let parent_id = parse_parent_id_field(object.get("parentId"))?;
            if let Some(parent_id) = parent_id.flatten() {
                if mailbox_id.is_some_and(|mailbox_id| mailbox_id == parent_id) {
                    bail!("mailbox parentId creates a cycle");
                }
                if !existing_by_id.contains_key(&parent_id) {
                    bail!("mailbox parentId must reference a mailbox in the same account");
                }
                if mailbox_id.is_some_and(|mailbox_id| {
                    mailbox_parent_chain_contains(&existing_by_id, parent_id, mailbox_id)
                }) {
                    bail!("mailbox parentId creates a cycle");
                }
            }
            if !object.contains_key("name") && parent_id.is_none() {
                continue;
            };
            let Some(mailbox_id) = mailbox_id else {
                continue;
            };
            let Some(existing) = existing_by_id.get(&mailbox_id) else {
                continue;
            };
            let name = match object.get("name").and_then(Value::as_str) {
                Some(raw_name) => MailboxSegment::new(raw_name)?.to_string(),
                None => existing.name.clone(),
            };
            let parent_id = parent_id.unwrap_or(existing.parent_id);
            requested.push((
                Some(mailbox_id),
                parent_id,
                MailboxNamePolicy::canonical_key(&name),
            ));
        }
    }

    for (mailbox_id, parent_id, requested_key) in &requested {
        for existing in existing_mailboxes {
            if mailbox_id.is_some_and(|mailbox_id| mailbox_id == existing.id) {
                continue;
            }
            if parent_id != &existing.parent_id {
                continue;
            }
            if requested_key.collides_with(&MailboxNamePolicy::canonical_key(&existing.name)) {
                bail!("mailbox already exists");
            }
        }
    }

    for index in 0..requested.len() {
        for other in requested.iter().skip(index + 1) {
            if requested[index].1 == other.1 && requested[index].2.collides_with(&other.2) {
                bail!("mailbox already exists");
            }
        }
    }

    Ok(())
}

fn mailbox_parent_chain_contains(
    mailboxes: &HashMap<Uuid, &JmapMailbox>,
    start: Uuid,
    target: Uuid,
) -> bool {
    let mut current = Some(start);
    while let Some(id) = current {
        if id == target {
            return true;
        }
        current = mailboxes.get(&id).and_then(|mailbox| mailbox.parent_id);
    }
    false
}

fn mailbox_name_field<'a>(value: &'a Value, object_error: &str) -> Result<&'a str> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{}", object_error))?
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("mailbox name is required"))
}
