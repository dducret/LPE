use super::*;

#[derive(Clone, Copy)]
pub(super) enum DeletedItemsContentRow<'a> {
    Message(&'a JmapEmail),
    Event(&'a crate::mapi_store::MapiEvent),
}

pub(super) fn deleted_items_content_rows<'a>(
    mailboxes: &[JmapMailbox],
    emails: &'a [JmapEmail],
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
) -> Vec<DeletedItemsContentRow<'a>> {
    let mut rows = emails_for_folder(TRASH_FOLDER_ID, mailboxes, emails)
        .into_iter()
        .filter(|email| {
            restriction_matches_email_in_snapshot(restriction, email, TRASH_FOLDER_ID, snapshot)
        })
        .map(DeletedItemsContentRow::Message)
        .collect::<Vec<_>>();
    rows.extend(
        snapshot
            .events_for_folder(TRASH_FOLDER_ID)
            .into_iter()
            .filter(|event| restriction_matches_event(restriction, event))
            .map(DeletedItemsContentRow::Event),
    );
    rows
}

pub(super) fn deleted_items_content_row_matches(
    row: &DeletedItemsContentRow<'_>,
    restriction: Option<&MapiRestriction>,
    snapshot: &MapiMailStoreSnapshot,
) -> bool {
    match row {
        DeletedItemsContentRow::Message(email) => {
            restriction_matches_email_in_snapshot(restriction, email, TRASH_FOLDER_ID, snapshot)
        }
        DeletedItemsContentRow::Event(event) => restriction_matches_event(restriction, event),
    }
}

pub(super) fn sort_deleted_items_content_rows(
    rows: &mut [DeletedItemsContentRow<'_>],
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
                        deleted_items_content_row_subject(left),
                        deleted_items_content_row_subject(right),
                    )
                }
                PID_TAG_MESSAGE_DELIVERY_TIME
                | PID_TAG_LAST_MODIFICATION_TIME
                | PID_TAG_LOCAL_COMMIT_TIME => {
                    deleted_items_content_row_time(left).cmp(deleted_items_content_row_time(right))
                }
                PID_TAG_MESSAGE_CLASS_W | PID_TAG_CONTAINER_CLASS_W => {
                    deleted_items_content_row_class(left)
                        .cmp(deleted_items_content_row_class(right))
                }
                PID_TAG_MID => {
                    deleted_items_content_row_id(left).cmp(&deleted_items_content_row_id(right))
                }
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        deleted_items_content_row_id(left).cmp(&deleted_items_content_row_id(right))
    });
}

pub(super) fn deleted_items_content_row_id(row: &DeletedItemsContentRow<'_>) -> u64 {
    match row {
        DeletedItemsContentRow::Message(email) => {
            crate::mapi::identity::mapped_mapi_object_id(&email.id).unwrap_or_default()
        }
        DeletedItemsContentRow::Event(event) => event.id,
    }
}

pub(super) fn serialize_deleted_items_content_row(
    row: DeletedItemsContentRow<'_>,
    columns: &[u32],
) -> Vec<u8> {
    match row {
        DeletedItemsContentRow::Message(email) => serialize_message_row(email, columns),
        DeletedItemsContentRow::Event(event) => serialize_versioned_event_row(event, columns),
    }
}

pub(super) fn serialize_deleted_items_content_property_row(
    row: DeletedItemsContentRow<'_>,
    columns: &[u32],
) -> Vec<u8> {
    match row {
        DeletedItemsContentRow::Message(email) => serialize_message_property_row(email, columns),
        DeletedItemsContentRow::Event(event) => {
            let values = serialize_versioned_event_row(event, columns);
            let mut property_row = Vec::new();
            write_query_rows_property_row(&mut property_row, columns, &values);
            property_row
        }
    }
}

pub(super) fn categorized_deleted_items_content_rows(
    rows: Vec<DeletedItemsContentRow<'_>>,
    columns: &[u32],
    sort_orders: &[MapiSortOrder],
    expanded_count: u16,
    collapsed_categories: &HashSet<u64>,
) -> Vec<CategorizedTableRow> {
    let Some(category_sort) = sort_orders.first() else {
        return rows
            .into_iter()
            .map(|row| CategorizedTableRow {
                category_id: 0,
                leaf_count: 1,
                row: serialize_deleted_items_content_row(row, columns),
                leaf: true,
            })
            .collect();
    };
    let mut groups: Vec<(u64, String, Vec<(DeletedItemsContentRow<'_>, u32)>)> = Vec::new();
    for row in rows {
        for (instance, value) in deleted_items_category_values(&row, category_sort.property_tag)
            .into_iter()
            .enumerate()
        {
            let category_id =
                category_id_for_value(TRASH_FOLDER_ID, category_sort.property_tag, &value);
            let instance_num = instance.saturating_add(1).min(u32::MAX as usize) as u32;
            if let Some((_, _, leaves)) = groups.iter_mut().find(|(id, _, _)| *id == category_id) {
                leaves.push((row, instance_num));
            } else {
                groups.push((category_id, value, vec![(row, instance_num)]));
            }
        }
    }
    groups.sort_by(|left, right| {
        apply_sort_direction(
            compare_case_insensitive(&left.1, &right.1),
            category_sort.order,
        )
    });

    let mut categorized = Vec::new();
    for (category_id, value, leaves) in groups {
        let expanded = expanded_count > 0 && !collapsed_categories.contains(&category_id);
        let unread_count = leaves
            .iter()
            .filter(|(row, _)| deleted_items_content_row_is_unread(row))
            .count();
        categorized.push(CategorizedTableRow {
            category_id,
            leaf_count: leaves.len(),
            row: serialize_category_header_row(
                category_id,
                &value,
                leaves.len(),
                unread_count,
                category_sort.property_tag,
                expanded,
                columns,
            ),
            leaf: false,
        });
        if expanded {
            categorized.extend(
                leaves
                    .into_iter()
                    .map(|(row, instance_num)| CategorizedTableRow {
                        category_id,
                        leaf_count: 1,
                        row: serialize_categorized_deleted_items_content_row(
                            row,
                            columns,
                            category_sort.property_tag,
                            &value,
                            instance_num,
                        ),
                        leaf: true,
                    }),
            );
        }
    }
    categorized
}

fn deleted_items_category_values(
    row: &DeletedItemsContentRow<'_>,
    property_tag: u32,
) -> Vec<String> {
    match row {
        DeletedItemsContentRow::Message(email) => category_values_for_email(email, property_tag),
        DeletedItemsContentRow::Event(event) => {
            let value = if canonical_property_storage_tag(property_tag) == PID_TAG_HAS_ATTACHMENTS {
                Some(MapiValue::Bool(!event.attachments.is_empty()))
            } else {
                versioned_event_property_value_with_reminder(event, property_tag, None)
            };
            value
                .map(category_values_from_mapi_value)
                .unwrap_or_else(|| vec![String::new()])
        }
    }
}

fn deleted_items_content_row_is_unread(row: &DeletedItemsContentRow<'_>) -> bool {
    matches!(row, DeletedItemsContentRow::Message(email) if email.unread)
}

fn serialize_categorized_deleted_items_content_row(
    row: DeletedItemsContentRow<'_>,
    columns: &[u32],
    category_property_tag: u32,
    category_value: &str,
    instance_num: u32,
) -> Vec<u8> {
    match row {
        DeletedItemsContentRow::Message(email) => serialize_categorized_message_row(
            email,
            columns,
            category_property_tag,
            category_value,
            instance_num,
        ),
        DeletedItemsContentRow::Event(event) => serialize_categorized_deleted_event_row(
            event,
            columns,
            category_property_tag,
            category_value,
            instance_num,
        ),
    }
}

fn serialize_categorized_deleted_event_row(
    event: &crate::mapi_store::MapiEvent,
    columns: &[u32],
    category_property_tag: u32,
    category_value: &str,
    instance_num: u32,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        let storage_tag = canonical_property_storage_tag(*column);
        if storage_tag == canonical_property_storage_tag(category_property_tag) {
            write_category_instance_value(&mut row, *column, category_value);
            continue;
        }
        match storage_tag {
            PID_TAG_INST_ID => write_u64(&mut row, event.id),
            PID_TAG_INSTANCE_NUM => write_u32(&mut row, instance_num),
            PID_TAG_ROW_TYPE => write_u32(&mut row, TABLE_LEAF_ROW),
            PID_TAG_DEPTH => write_u32(&mut row, 1),
            PID_TAG_HAS_ATTACHMENTS => row.push((!event.attachments.is_empty()) as u8),
            _ => match versioned_event_property_value_with_reminder(event, *column, None) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

fn deleted_items_content_row_subject<'a>(row: &'a DeletedItemsContentRow<'a>) -> &'a str {
    match row {
        DeletedItemsContentRow::Message(email) => &email.subject,
        DeletedItemsContentRow::Event(event) => &event.event.title,
    }
}

fn deleted_items_content_row_class(row: &DeletedItemsContentRow<'_>) -> &'static str {
    match row {
        DeletedItemsContentRow::Message(_) => "IPM.Note",
        DeletedItemsContentRow::Event(_) => "IPM.Appointment",
    }
}

fn deleted_items_content_row_time<'a>(row: &'a DeletedItemsContentRow<'a>) -> &'a str {
    match row {
        DeletedItemsContentRow::Message(email) => &email.received_at,
        DeletedItemsContentRow::Event(event) => &event.version.updated_at,
    }
}
