use super::*;

impl<S: JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_canonical_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let ids = string_ids_from_arguments(&arguments, "ids");
        let properties = property_names_from_arguments(&arguments);
        let ids_set = ids
            .as_ref()
            .map(|ids| ids.iter().cloned().collect::<HashSet<_>>());
        let list = self
            .canonical_objects(account, account_id, data_type)
            .await?
            .into_iter()
            .filter(|object| {
                ids_set
                    .as_ref()
                    .map(|ids| {
                        object
                            .get("id")
                            .and_then(Value::as_str)
                            .is_some_and(|id| ids.contains(id))
                    })
                    .unwrap_or(true)
            })
            .map(|object| project_get_properties(object, properties.as_ref()))
            .collect::<Vec<_>>();
        let not_found = ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| {
                !list
                    .iter()
                    .any(|object| object.get("id").and_then(Value::as_str) == Some(id.as_str()))
            })
            .map(Value::String)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": self.canonical_object_state(account, account_id, data_type).await?,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_canonical_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let mut all_ids = self
            .canonical_objects(account, account_id, data_type)
            .await?
            .into_iter()
            .filter_map(|object| object.get("id").and_then(Value::as_str).map(str::to_string))
            .collect::<Vec<_>>();
        all_ids.sort();
        let position = query_position(
            &all_ids,
            arguments.get("position").and_then(Value::as_i64),
            arguments.get("anchor").and_then(Value::as_str),
            arguments.get("anchorOffset").and_then(Value::as_i64),
        )?;
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = all_ids
            .iter()
            .skip(position)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let total = all_ids.len();
        let method_name = format!("{data_type}/query");
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?
            .unwrap_or(0);
        let query_state = match self
            .store
            .save_jmap_query_state(account_id, &method_name, None, None, cursor, &all_ids)
            .await?
        {
            Some(state_id) => encode_query_state_reference(
                account_id,
                &method_name,
                None,
                None,
                state_id,
                cursor,
            )?,
            None => encode_query_state(account_id, &method_name, None, None, all_ids)?,
        };

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": query_state,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": total,
        }))
    }

    pub(crate) async fn handle_canonical_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let since_query_state = arguments
            .get("sinceQueryState")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("sinceQueryState is required"))?
            .to_string();
        let mut ids = self
            .canonical_query_ids(account, account_id, data_type, &arguments)
            .await?;
        ids.sort();
        let total = ids.len() as u64;
        let method_name = canonical_query_state_method(data_type);
        let filter_state = canonical_query_filter(data_type, &arguments);
        let previous = decode_query_state(&since_query_state)?;
        validate_query_state_token(
            account_id,
            &method_name,
            filter_state.as_ref(),
            None,
            &previous,
        )?;
        let mut previous_cursor = previous.cursor.unwrap_or(0);
        let previous_ids =
            if let Some(state_id) = previous.state_id.as_deref().map(parse_uuid).transpose()? {
                let stored = self
                    .store
                    .fetch_jmap_query_state(
                        account_id,
                        &method_name,
                        state_id,
                        filter_state.clone(),
                        None,
                    )
                    .await?
                    .ok_or_else(|| anyhow!("queryState is no longer available"))?;
                previous_cursor = stored.last_change_sequence;
                stored.snapshot_ids
            } else {
                previous.ids.clone()
            };
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?
            .unwrap_or(0);
        let diff = query_diff_for_kind(
            &method_name,
            &previous_ids,
            &ids,
            arguments.get("maxChanges").and_then(Value::as_u64),
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
                &method_name,
                filter_state.clone(),
                None,
                next_cursor,
                &diff.query_state_ids,
            )
            .await?
        {
            Some(state_id) => encode_query_state_reference(
                account_id,
                &method_name,
                filter_state.clone(),
                None,
                state_id,
                next_cursor,
            )?,
            None => encode_query_state(
                account_id,
                &method_name,
                filter_state.clone(),
                None,
                diff.query_state_ids.clone(),
            )?,
        };
        query_changes_response_from_diff(
            account_id,
            &method_name,
            since_query_state,
            filter_state,
            None,
            previous,
            next_query_state,
            total,
            diff,
        )
    }

    pub(crate) async fn handle_canonical_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let since_state = arguments
            .get("sinceState")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("sinceState is required"))?;
        let max_changes = arguments.get("maxChanges").and_then(Value::as_u64);
        let entries = self
            .canonical_objects(account, account_id, data_type)
            .await?
            .into_iter()
            .filter_map(|object| {
                let id = object.get("id")?.as_str()?.to_string();
                Some(StateEntry {
                    id,
                    fingerprint: opaque_state_fingerprint(&object.to_string()),
                })
            })
            .collect::<Vec<_>>();
        if matches!(data_type, "Share" | "Reminder") {
            return self
                .string_object_changes_response(
                    account_id,
                    data_type,
                    since_state,
                    max_changes,
                    entries,
                )
                .await;
        }
        self.object_changes_response(account_id, data_type, since_state, max_changes, entries)
            .await
    }

    pub(crate) async fn string_object_changes_response(
        &self,
        account_id: Uuid,
        data_type: &str,
        since_state: &str,
        max_changes: Option<u64>,
        entries: Vec<StateEntry>,
    ) -> Result<Value> {
        let cursor = self
            .store
            .fetch_jmap_object_change_cursor(account_id, data_type)
            .await?;
        if let Some(after_cursor) = state_cursor(account_id, data_type, since_state)? {
            if let Some(changes) = self
                .store
                .replay_jmap_string_object_changes(
                    account_id,
                    data_type,
                    after_cursor,
                    crate::store::MAX_JMAP_MAIL_OBJECT_REPLAY_ROWS,
                )
                .await?
            {
                return changes_response_from_durable_with_cursor(
                    account_id,
                    data_type,
                    since_state,
                    max_changes,
                    entries,
                    cursor,
                    changes
                        .into_iter()
                        .map(|change| DurableObjectChange {
                            id: change.object_id,
                        })
                        .collect(),
                );
            }
        }
        changes_response_with_cursor(
            account_id,
            data_type,
            since_state,
            max_changes,
            entries,
            cursor,
        )
    }

    pub(crate) async fn handle_canonical_import_or_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
        data_type: &str,
        method_name: &str,
    ) -> Result<Value> {
        let mut set_arguments = Map::new();
        if let Some(account_id) = arguments.get("accountId").cloned() {
            set_arguments.insert("accountId".to_string(), account_id);
        }
        set_arguments.insert(
            "create".to_string(),
            arguments
                .get("create")
                .cloned()
                .unwrap_or_else(|| json!({})),
        );
        let mut response = match data_type {
            "ContactCard" => {
                self.handle_contact_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "CalendarEvent" => {
                self.handle_calendar_event_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "TaskList" => {
                self.handle_task_list_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "Task" => {
                self.handle_task_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "Note" => {
                self.handle_note_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            "JournalEntry" => {
                self.handle_journal_entry_set(account, Value::Object(set_arguments), created_ids)
                    .await?
            }
            _ => {
                return self
                    .handle_canonical_unsupported_write(account, arguments, data_type, method_name)
                    .await;
            }
        };
        if let Value::Object(map) = &mut response {
            map.insert("method".to_string(), Value::String(method_name.to_string()));
        }
        Ok(response)
    }

    pub(crate) async fn handle_canonical_unsupported_write(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        data_type: &str,
        method_name: &str,
    ) -> Result<Value> {
        let account_id = requested_account_id_from_arguments(&arguments, account)?;
        let old_state = self
            .canonical_object_state(account, account_id, data_type)
            .await?;
        let mut not_created = Map::new();
        let mut not_updated = Map::new();
        let mut not_destroyed = Map::new();
        for id in canonical_create_ids(&arguments) {
            not_created.insert(
                id,
                json!({
                    "type": "forbidden",
                    "description": format!("{method_name} is not a canonical write surface for {data_type}"),
                }),
            );
        }
        if method_name.ends_with("/set") {
            for id in object_keys(&arguments, "update") {
                not_updated.insert(
                    id,
                    json!({
                        "type": "forbidden",
                        "description": format!("{method_name} is not a canonical write surface for {data_type}"),
                    }),
                );
            }
            for id in string_ids_from_arguments(&arguments, "destroy").unwrap_or_default() {
                not_destroyed.insert(
                    id,
                    json!({
                        "type": "forbidden",
                        "description": format!("{method_name} is not a canonical write surface for {data_type}"),
                    }),
                );
            }
        }
        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": old_state,
            "newState": self.canonical_object_state(account, account_id, data_type).await?,
            "created": {},
            "notCreated": Value::Object(not_created),
            "updated": {},
            "notUpdated": Value::Object(not_updated),
            "destroyed": [],
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    pub(crate) async fn canonical_object_state(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<String> {
        match data_type {
            "Identity" => {
                self.identity_object_state(account.account_id, account_id)
                    .await
            }
            "EmailSubmission" => self.email_submission_object_state(account_id).await,
            "Mailbox" => {
                let access = self
                    .requested_account_access(account, Some(&account_id.to_string()))
                    .await?;
                self.mailbox_object_state(&access).await
            }
            "Email" | "Thread" => {
                let access = self
                    .requested_account_access(account, Some(&account_id.to_string()))
                    .await?;
                self.mail_object_state(&access, data_type).await
            }
            "Blob" | "DurableChange" => {
                let entries = self
                    .canonical_objects(account, account_id, data_type)
                    .await?
                    .into_iter()
                    .filter_map(|object| {
                        let id = object.get("id")?.as_str()?.to_string();
                        Some(StateEntry {
                            id,
                            fingerprint: opaque_state_fingerprint(&object.to_string()),
                        })
                    })
                    .collect();
                encode_state(account_id, data_type, entries)
            }
            "Share" | "Reminder" | "Rule" => {
                let entries = self
                    .canonical_objects(account, account_id, data_type)
                    .await?
                    .into_iter()
                    .filter_map(|object| {
                        let id = object.get("id")?.as_str()?.to_string();
                        Some(StateEntry {
                            id,
                            fingerprint: opaque_state_fingerprint(&object.to_string()),
                        })
                    })
                    .collect();
                let cursor = self
                    .store
                    .fetch_jmap_object_change_cursor(account_id, data_type)
                    .await?;
                encode_state_with_cursor(account_id, data_type, entries, cursor)
            }
            _ => self.object_state(account_id, data_type).await,
        }
    }

    pub(super) async fn canonical_objects(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<Vec<Value>> {
        match data_type {
            "Identity" => Ok(self
                .store
                .fetch_sender_identities(account.account_id, account_id)
                .await?
                .into_iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<_>, _>>()?),
            "Reminder" => Ok(self
                .store
                .query_jmap_reminders(
                    account_id,
                    lpe_storage::ReminderQuery {
                        include_inactive: true,
                    },
                )
                .await?
                .into_iter()
                .map(|reminder| {
                    let id = format!("{}:{}", reminder.source_type, reminder.source_id);
                    let mut object = serde_json::to_value(reminder)?;
                    if let Value::Object(map) = &mut object {
                        map.insert("id".to_string(), Value::String(id));
                        map.insert("@type".to_string(), Value::String("Reminder".to_string()));
                    }
                    Ok(object)
                })
                .collect::<Result<Vec<_>>>()?),
            "Rule" => Ok(self
                .store
                .list_mailbox_rules(account_id)
                .await?
                .into_iter()
                .map(rule_to_value)
                .collect()),
            "OutlookProfile" => Ok(vec![outlook_profile_state_to_value(
                self.store.fetch_outlook_profile_state(account_id).await?,
            )]),
            "SearchFolder" => Ok(self
                .store
                .fetch_search_folders(account_id)
                .await?
                .into_iter()
                .map(search_folder_to_value)
                .collect()),
            "Share" => self.store.fetch_jmap_shares(account_id).await,
            "DurableChange" => {
                let cursor = self.store.fetch_canonical_change_cursor(account_id).await?;
                Ok(vec![json!({
                    "id": "canonical",
                    "@type": "DurableChange",
                    "scope": "account",
                    "cursor": cursor,
                    "isAppendOnly": true,
                    "mayRead": true,
                    "mayWrite": false,
                    "categories": [
                        {"id": "mail", "objectTypes": ["Mailbox", "Email", "Thread", "EmailSubmission", "Blob"]},
                        {"id": "contacts", "objectTypes": ["AddressBook", "ContactCard"]},
                        {"id": "calendar", "objectTypes": ["Calendar", "CalendarEvent"]},
                        {"id": "tasks", "objectTypes": ["TaskList", "Task", "Reminder"]},
                        {"id": "notes", "objectTypes": ["Note"]},
                        {"id": "journal", "objectTypes": ["JournalEntry"]},
                        {"id": "rights", "objectTypes": ["Identity", "Share"]},
                        {"id": "search", "objectTypes": ["SearchFolder"]},
                        {"id": "rules", "objectTypes": ["Rule"]},
                        {"id": "profile", "objectTypes": ["OutlookProfile"]}
                    ],
                })])
            }
            "Blob" => Ok(Vec::new()),
            _ => Ok(self
                .object_state_entries(account_id, data_type)
                .await?
                .into_iter()
                .map(|entry| json!({"id": entry.id}))
                .collect()),
        }
    }

    async fn canonical_query_ids(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        data_type: &str,
        arguments: &Value,
    ) -> Result<Vec<String>> {
        if data_type == "Reminder" {
            return Ok(self
                .store
                .query_jmap_reminders(
                    account_id,
                    lpe_storage::ReminderQuery {
                        include_inactive: arguments
                            .get("includeInactive")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    },
                )
                .await?
                .into_iter()
                .map(|reminder| format!("{}:{}", reminder.source_type, reminder.source_id))
                .collect());
        }
        Ok(self
            .canonical_objects(account, account_id, data_type)
            .await?
            .into_iter()
            .filter_map(|object| object.get("id").and_then(Value::as_str).map(str::to_string))
            .collect())
    }
}
