use super::*;

#[derive(Clone, Copy)]
pub(super) enum SearchContentRow<'a> {
    Message(&'a MapiMessage),
    Task(&'a MapiTask),
}

pub(super) fn todo_search_content_rows<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
) -> Vec<SearchContentRow<'a>> {
    let mut rows = snapshot
        .todo_search_messages()
        .into_iter()
        .filter(|message| restriction_matches_email(restriction, &message.email))
        .map(SearchContentRow::Message)
        .collect::<Vec<_>>();
    rows.extend(
        snapshot
            .todo_search_results()
            .into_iter()
            .filter(|task| restriction_matches_task(restriction, &task.task))
            .map(SearchContentRow::Task),
    );
    rows
}

pub(super) fn reminder_search_content_rows<'a>(
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
) -> Vec<SearchContentRow<'a>> {
    let mut rows = Vec::new();
    rows.extend(
        snapshot
            .reminder_tasks()
            .into_iter()
            .filter(|task| restriction_matches_task(restriction, &task.task))
            .map(SearchContentRow::Task),
    );
    rows.extend(
        snapshot
            .reminder_messages()
            .into_iter()
            .filter(|message| restriction_matches_email(restriction, &message.email))
            .map(SearchContentRow::Message),
    );
    rows
}

pub(super) fn search_content_row_matches(
    row: &SearchContentRow<'_>,
    restriction: Option<&MapiRestriction>,
) -> bool {
    match row {
        SearchContentRow::Message(message) => {
            restriction_matches_email(restriction, &message.email)
        }
        SearchContentRow::Task(task) => restriction_matches_task(restriction, &task.task),
    }
}

pub(super) fn sort_search_content_rows(
    rows: &mut [SearchContentRow<'_>],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match canonical_property_storage_tag(sort_order.property_tag) {
                PID_TAG_DISPLAY_NAME_W | PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(
                        search_content_row_subject(left),
                        search_content_row_subject(right),
                    )
                }
                PID_TAG_MESSAGE_DELIVERY_TIME
                | PID_TAG_LAST_MODIFICATION_TIME
                | PID_TAG_LOCAL_COMMIT_TIME => {
                    search_content_row_time(left).cmp(&search_content_row_time(right))
                }
                PID_TAG_MESSAGE_CLASS_W | PID_TAG_CONTAINER_CLASS_W => {
                    search_content_row_class(left).cmp(search_content_row_class(right))
                }
                PID_TAG_MID => search_content_row_id(left).cmp(&search_content_row_id(right)),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        search_content_row_id(left).cmp(&search_content_row_id(right))
    });
}

pub(super) fn search_content_row_id(row: &SearchContentRow<'_>) -> u64 {
    match row {
        SearchContentRow::Message(message) => message.id,
        SearchContentRow::Task(task) => task.id,
    }
}

fn search_content_row_subject<'a>(row: &'a SearchContentRow<'a>) -> &'a str {
    match row {
        SearchContentRow::Message(message) => &message.email.subject,
        SearchContentRow::Task(task) => &task.task.title,
    }
}

fn search_content_row_class(row: &SearchContentRow<'_>) -> &'static str {
    match row {
        SearchContentRow::Message(_) => "IPM.Note",
        SearchContentRow::Task(_) => "IPM.Task",
    }
}

fn search_content_row_time(row: &SearchContentRow<'_>) -> String {
    match row {
        SearchContentRow::Message(message) => message.email.received_at.clone(),
        SearchContentRow::Task(task) => task.task.updated_at.clone(),
    }
}

pub(super) fn serialize_search_content_row(
    row: SearchContentRow<'_>,
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    reminder_projection: bool,
) -> Vec<u8> {
    match row {
        SearchContentRow::Message(message) => serialize_message_row(&message.email, columns),
        SearchContentRow::Task(task) if reminder_projection => serialize_reminder_task_row(
            task,
            snapshot.reminder_for_source("task", task.canonical_id),
            columns,
        ),
        SearchContentRow::Task(task) => {
            serialize_task_row(&task.task, task.id, TODO_SEARCH_FOLDER_ID, columns)
        }
    }
}
