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
