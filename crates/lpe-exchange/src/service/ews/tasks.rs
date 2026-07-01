use super::super::*;

pub(in crate::service) fn task_change_key(task: &ClientTask, sync_version: Option<&str>) -> String {
    stable_change_key(&[
        "task",
        &task.id.to_string(),
        sync_version.unwrap_or_default(),
        &task.task_list_id.to_string(),
        &task.title,
        &task.description,
        &task.status,
        task.due_at.as_deref().unwrap_or_default(),
        task.completed_at.as_deref().unwrap_or_default(),
        &task.sort_order.to_string(),
    ])
}

pub(in crate::service) fn task_item_summary_xml(task: &ClientTask) -> String {
    let change_key = task_change_key(task, None);
    task_item_summary_xml_with_change_key(task, &change_key)
}

fn task_item_summary_xml_with_change_key(task: &ClientTask, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Task>",
            "<t:ItemId Id=\"task:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Status>{status}</t:Status>",
            "{due_date}",
            "{complete_date}",
            "</t:Task>"
        ),
        id = task.id,
        change_key = escape_xml(change_key),
        title = escape_xml(&task.title),
        status = ews_task_status(&task.status),
        due_date = optional_text_element("t:DueDate", task.due_at.as_deref()),
        complete_date = optional_text_element("t:CompleteDate", task.completed_at.as_deref()),
    )
}

pub(in crate::service) fn task_item_xml(task: &ClientTask) -> String {
    let change_key = task_change_key(task, None);
    task_item_xml_with_change_key(task, &change_key)
}

pub(in crate::service) fn task_item_xml_with_change_key(
    task: &ClientTask,
    change_key: &str,
) -> String {
    format!(
        concat!(
            "<t:Task>",
            "<t:ItemId Id=\"task:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Body BodyType=\"Text\">{description}</t:Body>",
            "<t:Status>{status}</t:Status>",
            "{due_date}",
            "{complete_date}",
            "</t:Task>"
        ),
        id = task.id,
        change_key = escape_xml(change_key),
        folder_id = task.task_list_id,
        title = escape_xml(&task.title),
        description = escape_xml(&task.description),
        status = ews_task_status(&task.status),
        due_date = optional_text_element("t:DueDate", task.due_at.as_deref()),
        complete_date = optional_text_element("t:CompleteDate", task.completed_at.as_deref()),
    )
}

pub(in crate::service) fn create_task_success_response(task: &ClientTask) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Task>",
            "<t:ItemId Id=\"task:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Status>{status}</t:Status>",
            "{due_date}",
            "{complete_date}",
            "</t:Task>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = task.id,
        change_key = escape_xml(&task_change_key(task, None)),
        folder_id = task.task_list_id,
        title = escape_xml(&task.title),
        status = ews_task_status(&task.status),
        due_date = optional_text_element("t:DueDate", task.due_at.as_deref()),
        complete_date = optional_text_element("t:CompleteDate", task.completed_at.as_deref()),
    )
}

pub(in crate::service) fn parse_create_task_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertClientTaskInput> {
    let task =
        element_content(request, "Task").ok_or_else(|| anyhow!("CreateItem is missing Task"))?;
    let body_tag = open_tag_text(task, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(task, "Body").unwrap_or_default();
    let description = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value
    };
    let status = element_text(task, "Status")
        .map(|value| ews_task_status_to_canonical(&value))
        .transpose()?
        .unwrap_or("needs-action")
        .to_string();

    Ok(UpsertClientTaskInput {
        id: None,
        principal_account_id: principal.account_id,
        account_id: principal.account_id,
        task_list_id: requested_task_list_id(request)?,
        title: element_text(task, "Subject").unwrap_or_else(|| "Untitled task".to_string()),
        description,
        status,
        due_at: element_text(task, "DueDate"),
        completed_at: element_text(task, "CompleteDate"),
        recurrence_rule: parse_ews_recurrence(task)?,
        sort_order: 0,
    })
}

pub(in crate::service) fn parse_update_task_input(
    principal: &AccountPrincipal,
    existing: &ClientTask,
    request: &str,
) -> Result<UpsertClientTaskInput> {
    let task = element_content(request, "Task").unwrap_or(request);
    let description = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(task, "Body") {
        let body_tag = open_tag_text(task, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.description.clone()
    };
    let status = element_text(task, "Status")
        .map(|value| ews_task_status_to_canonical(&value))
        .transpose()?
        .unwrap_or(existing.status.as_str())
        .to_string();

    Ok(UpsertClientTaskInput {
        id: Some(existing.id),
        principal_account_id: principal.account_id,
        account_id: principal.account_id,
        task_list_id: requested_task_list_id(request)?.or(Some(existing.task_list_id)),
        title: deleted_or_updated_text(request, task, "task:Subject", "Subject", &existing.title)
            .if_empty(existing.title.clone()),
        description,
        status,
        due_at: if field_deleted(request, "task:DueDate") {
            None
        } else {
            element_text(task, "DueDate").or_else(|| existing.due_at.clone())
        },
        completed_at: if field_deleted(request, "task:CompleteDate") {
            None
        } else {
            element_text(task, "CompleteDate").or_else(|| existing.completed_at.clone())
        },
        recurrence_rule: if field_deleted(request, "task:Recurrence") {
            String::new()
        } else {
            parse_ews_recurrence(task)?.if_empty(existing.recurrence_rule.clone())
        },
        sort_order: existing.sort_order,
    })
}

fn requested_task_list_id(request: &str) -> Result<Option<Uuid>> {
    match requested_collection_id(request) {
        Some("default") | Some("tasks") | None => Ok(None),
        Some(id) => Uuid::parse_str(id)
            .map(Some)
            .map_err(|_| anyhow!("Task folder id is not a canonical task-list id")),
    }
}

fn ews_task_status_to_canonical(value: &str) -> Result<&'static str> {
    Ok(EwsTaskStatus::parse(value)?.canonical_status())
}

fn ews_task_status(status: &str) -> &'static str {
    match status {
        "in-progress" => "InProgress",
        "completed" => "Completed",
        "cancelled" => "Deferred",
        _ => "NotStarted",
    }
}

fn optional_text_element(name: &str, value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("<{name}>{}</{name}>", escape_xml(value)))
        .unwrap_or_default()
}
