use anyhow::{anyhow, bail, Result};
use lpe_storage::{
    AuthenticatedAccount, ClientTask, ClientTaskList, CreateTaskListInput, UpdateTaskListInput,
    UpsertClientTaskInput,
};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    convert::insert_if,
    error::set_error,
    parse::{parse_optional_string, parse_required_string, parse_uuid, parse_uuid_list},
    protocol::{
        ChangesArguments, QueryChangesArguments, TaskGetArguments, TaskListGetArguments,
        TaskListSetArguments, TaskQueryArguments, TaskQueryFilter, TaskQuerySort, TaskSetArguments,
    },
    state::{changes_response, query_changes_response},
    validation::{validate_task_filter, validate_task_sort},
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
};

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_task_list_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: TaskListGetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = task_list_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let task_lists = if let Some(ids) = requested_ids.as_ref() {
            self.store
                .fetch_jmap_task_lists_by_ids(account_id, ids)
                .await?
        } else {
            self.store.fetch_jmap_task_lists(account_id).await?
        };
        let list = task_lists
            .iter()
            .filter(|task_list| {
                requested_ids.is_none()
                    || requested_ids
                        .as_ref()
                        .is_some_and(|ids| ids.contains(&task_list.id))
            })
            .map(|task_list| task_list_to_value(task_list, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !task_lists.iter().any(|task_list| task_list.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "TaskList").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_task_list_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self.object_state_entries(account_id, "TaskList").await?;
        Ok(changes_response(
            account_id,
            "TaskList",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    pub(crate) async fn handle_task_list_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: TaskListSetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "TaskList").await?;
        let properties = task_list_properties(None);
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_task_list_create(account_id, value) {
                    Ok(input) => match self.store.create_jmap_task_list(input).await {
                        Ok(task_list) => {
                            created
                                .insert(creation_id, task_list_to_value(&task_list, &properties));
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
                    Ok(task_list_id) => {
                        match parse_task_list_update(account_id, task_list_id, value) {
                            Ok(input) => match self.store.update_jmap_task_list(input).await {
                                Ok(task_list) => {
                                    updated.insert(id, task_list_to_value(&task_list, &properties));
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
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }
        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(task_list_id) => match self
                        .store
                        .delete_jmap_task_list(account_id, task_list_id)
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

        let new_state = self.object_state(account_id, "TaskList").await?;
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

    pub(crate) async fn handle_task_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: TaskGetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = task_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let tasks = if let Some(ids) = requested_ids.as_ref() {
            self.store.fetch_jmap_tasks_by_ids(account_id, ids).await?
        } else {
            self.store.fetch_jmap_tasks(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();
        let list = tasks
            .iter()
            .filter(|task| requested_ids.is_none() || requested_set.contains(&task.id))
            .map(|task| task_to_value(task, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !tasks.iter().any(|task| task.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();
        let state = self.object_state(account_id, "Task").await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": state,
            "list": list,
            "notFound": not_found,
        }))
    }

    pub(crate) async fn handle_task_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: TaskQueryArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_task_sort(arguments.sort.as_deref())?;
        validate_task_filter(arguments.filter.as_ref())?;

        let mut tasks = self.store.fetch_jmap_tasks(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            tasks.retain(|task| task_matches_filter(task, filter));
        }
        tasks.sort_by_key(task_sort_key);

        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = tasks
            .iter()
            .skip(position)
            .take(limit)
            .map(|task| task.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": crate::encode_query_state(
                account_id,
                "Task",
                arguments.filter.map(|filter| serde_json::to_value(filter)).transpose()?,
                arguments
                    .sort
                    .map(|sort| {
                        sort.into_iter()
                            .map(serde_json::to_value)
                            .collect::<std::result::Result<Vec<_>, _>>()
                    })
                    .transpose()?,
                tasks.iter().map(|task| task.id.to_string()).collect(),
            )?,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": tasks.len(),
        }))
    }

    pub(crate) async fn handle_task_query_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QueryChangesArguments<TaskQueryFilter, TaskQuerySort> =
            serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_task_sort(arguments.sort.as_deref())?;
        validate_task_filter(arguments.filter.as_ref())?;

        let mut tasks = self.store.fetch_jmap_tasks(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            tasks.retain(|task| task_matches_filter(task, filter));
        }
        tasks.sort_by_key(task_sort_key);

        query_changes_response(
            account_id,
            "Task",
            arguments.since_query_state,
            arguments.filter.map(serde_json::to_value).transpose()?,
            arguments
                .sort
                .map(|sort| {
                    sort.into_iter()
                        .map(serde_json::to_value)
                        .collect::<std::result::Result<Vec<_>, _>>()
                })
                .transpose()?,
            tasks.iter().map(|task| task.id.to_string()).collect(),
            tasks.len() as u64,
            arguments.max_changes,
        )
    }

    pub(crate) async fn handle_task_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let entries = self.object_state_entries(account_id, "Task").await?;
        Ok(changes_response(
            account_id,
            "Task",
            &arguments.since_state,
            arguments.max_changes,
            entries,
        ))
    }

    pub(crate) async fn handle_task_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: TaskSetArguments = serde_json::from_value(arguments)?;
        let account_id = super::requested_account_id(arguments.account_id.as_deref(), account)?;
        let old_state = self.object_state(account_id, "Task").await?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_task_input(None, account_id, value) {
                    Ok(input) => match self.store.upsert_jmap_task(input).await {
                        Ok(task) => {
                            created_ids.insert(creation_id.clone(), task.id.to_string());
                            created.insert(creation_id, json!({ "id": task.id.to_string() }));
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
                    Ok(task_id) => {
                        let existing_task = self
                            .store
                            .fetch_jmap_tasks_by_ids(account_id, &[task_id])
                            .await?
                            .into_iter()
                            .next()
                            .ok_or_else(|| anyhow!("task not found"));
                        match existing_task.and_then(|existing_task| {
                            let mut input = parse_task_input(Some(task_id), account_id, value)?;
                            if input.task_list_id.is_none() {
                                input.task_list_id = Some(existing_task.task_list_id);
                            }
                            Ok(input)
                        }) {
                            Ok(input) => match self.store.upsert_jmap_task(input).await {
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
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(task_id) => match self.store.delete_jmap_task(account_id, task_id).await {
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

        let new_state = self.object_state(account_id, "Task").await?;
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

fn task_list_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "role".to_string(),
                "sortOrder".to_string(),
                "isSubscribed".to_string(),
                "isVisible".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn task_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "uid".to_string(),
                "@type".to_string(),
                "taskListId".to_string(),
                "title".to_string(),
                "description".to_string(),
                "status".to_string(),
                "due".to_string(),
                "completed".to_string(),
                "sortOrder".to_string(),
                "updated".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn task_list_to_value(task_list: &ClientTaskList, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", task_list.id.to_string());
    insert_if(properties, &mut object, "name", task_list.name.clone());
    insert_if(properties, &mut object, "role", task_list.role.clone());
    insert_if(properties, &mut object, "sortOrder", task_list.sort_order);
    insert_if(properties, &mut object, "isSubscribed", true);
    insert_if(properties, &mut object, "isVisible", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayRead": task_list.rights.may_read,
                "mayAddItems": task_list.rights.may_write,
                "mayModifyItems": task_list.rights.may_write,
                "mayRemoveItems": task_list.rights.may_delete,
                "mayRename": task_list.is_owned,
                "mayDelete": task_list.is_owned && task_list.role.is_none(),
                "mayAdmin": task_list.rights.may_share,
            }),
        );
    }
    Value::Object(object)
}

fn task_to_value(task: &ClientTask, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", task.id.to_string());
    insert_if(properties, &mut object, "uid", task.id.to_string());
    insert_if(properties, &mut object, "@type", "Task");
    insert_if(
        properties,
        &mut object,
        "taskListId",
        task.task_list_id.to_string(),
    );
    insert_if(properties, &mut object, "title", task.title.clone());
    insert_if(
        properties,
        &mut object,
        "description",
        task.description.clone(),
    );
    insert_if(properties, &mut object, "status", task.status.clone());
    insert_if(properties, &mut object, "due", task.due_at.clone());
    insert_if(
        properties,
        &mut object,
        "completed",
        task.completed_at.clone(),
    );
    insert_if(properties, &mut object, "sortOrder", task.sort_order);
    insert_if(properties, &mut object, "updated", task.updated_at.clone());
    Value::Object(object)
}

fn task_matches_filter(task: &ClientTask, filter: &TaskQueryFilter) -> bool {
    if let Some(task_list_id) = filter.in_task_list.as_deref() {
        if task_list_id != task.task_list_id.to_string() {
            return false;
        }
    }
    if let Some(status) = filter.status.as_deref() {
        if task.status != status.trim().to_ascii_lowercase() {
            return false;
        }
    }
    if let Some(text) = filter.text.as_deref() {
        let text = text.trim().to_ascii_lowercase();
        if !text.is_empty()
            && !task.title.to_ascii_lowercase().contains(&text)
            && !task.description.to_ascii_lowercase().contains(&text)
        {
            return false;
        }
    }
    true
}

fn task_sort_key(task: &ClientTask) -> (i32, i32, String, String) {
    (
        task.task_list_sort_order,
        task.sort_order,
        task.updated_at.clone(),
        task.id.to_string(),
    )
}

fn parse_task_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<UpsertClientTaskInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("task arguments must be an object"))?;
    reject_unknown_task_properties(object)?;
    let task_list_id = validate_task_list_id(object.get("taskListId"))?;

    let task_type = object
        .get("@type")
        .and_then(Value::as_str)
        .unwrap_or("Task");
    if task_type != "Task" {
        bail!("only @type=Task is supported");
    }
    if let Some(uid) = object.get("uid").and_then(Value::as_str) {
        if uid.trim().is_empty() {
            bail!("uid must not be empty");
        }
    }

    Ok(UpsertClientTaskInput {
        id,
        principal_account_id: account_id,
        account_id,
        task_list_id,
        title: parse_required_string(object.get("title"), "title")?,
        description: parse_optional_string(object.get("description"))?.unwrap_or_default(),
        status: parse_optional_string(object.get("status"))?
            .unwrap_or_else(|| "needs-action".to_string()),
        due_at: parse_optional_string(object.get("due"))?,
        completed_at: parse_optional_string(object.get("completed"))?,
        sort_order: object.get("sortOrder").and_then(Value::as_i64).unwrap_or(0) as i32,
    })
}

fn parse_task_list_create(account_id: Uuid, value: Value) -> Result<CreateTaskListInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("task list arguments must be an object"))?;
    reject_unknown_task_list_properties(object)?;
    Ok(CreateTaskListInput {
        account_id,
        name: parse_required_string(object.get("name"), "name")?,
        sort_order: object.get("sortOrder").and_then(Value::as_i64).unwrap_or(0) as i32,
    })
}

fn parse_task_list_update(
    account_id: Uuid,
    task_list_id: Uuid,
    value: Value,
) -> Result<UpdateTaskListInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("task list arguments must be an object"))?;
    reject_unknown_task_list_properties(object)?;
    Ok(UpdateTaskListInput {
        account_id,
        task_list_id,
        name: parse_optional_string(object.get("name"))?,
        sort_order: object
            .get("sortOrder")
            .and_then(Value::as_i64)
            .map(|value| value as i32),
    })
}

fn reject_unknown_task_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "@type" | "uid" | "title" | "description" | "status" | "due" | "completed"
            | "sortOrder" | "taskListId" => {}
            _ => bail!("unsupported task property: {key}"),
        }
    }
    Ok(())
}

fn reject_unknown_task_list_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "name" | "sortOrder" => {}
            _ => bail!("unsupported task list property: {key}"),
        }
    }
    Ok(())
}

fn validate_task_list_id(value: Option<&Value>) -> Result<Option<Uuid>> {
    if let Some(value) = value {
        let task_list_id = value
            .as_str()
            .ok_or_else(|| anyhow!("taskListId must be a string"))?;
        return Ok(Some(parse_uuid(task_list_id)?));
    }
    Ok(None)
}
