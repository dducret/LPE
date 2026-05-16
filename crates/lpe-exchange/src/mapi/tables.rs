use super::permissions::*;
use super::properties::*;
use super::rop::*;
use super::session::*;
use super::sync::*;
use super::*;

pub(in crate::mapi) fn hierarchy_row_count(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    if is_root_hierarchy_folder(folder_id) {
        mailboxes
            .len()
            .saturating_add(snapshot.collaboration_folders().len())
            .min(u32::MAX as usize) as u32
    } else {
        0
    }
}

pub(in crate::mapi) fn folder_message_count(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    if let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) {
        return folder.item_count;
    }
    folder_row_for_id(folder_id, mailboxes)
        .map(|mailbox| mailbox.total_emails)
        .unwrap_or_else(|| emails_for_folder(folder_id, mailboxes, emails).len() as u32)
}

pub(in crate::mapi) fn default_hierarchy_columns() -> Vec<u32> {
    vec![
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_SUBFOLDERS,
    ]
}

pub(in crate::mapi) fn default_contents_columns() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
    ]
}

pub(in crate::mapi) fn default_attachment_columns() -> Vec<u32> {
    vec![
        PID_TAG_ATTACH_NUM,
        PID_TAG_ATTACH_LONG_FILENAME_W,
        PID_TAG_ATTACH_FILENAME_W,
        PID_TAG_ATTACH_MIME_TAG_W,
        PID_TAG_ATTACH_SIZE,
        PID_TAG_ATTACH_METHOD,
        PID_TAG_RENDERING_POSITION,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
    ]
}

pub(in crate::mapi) fn default_folder_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_SUBFOLDERS,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_TAG_LOCAL_COMMIT_TIME_MAX,
        PID_TAG_HIERARCHY_CHANGE_NUMBER,
        PID_TAG_HIER_REV,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

#[derive(Clone, Copy)]
enum HierarchyRow<'a> {
    Mailbox(&'a JmapMailbox),
    Collaboration(&'a MapiCollaborationFolder),
}

fn hierarchy_rows<'a>(
    mailboxes: &'a [JmapMailbox],
    snapshot: &'a MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    sort_orders: &[MapiSortOrder],
) -> Vec<HierarchyRow<'a>> {
    let mut rows = mailboxes
        .iter()
        .filter(|mailbox| restriction_matches_mailbox(restriction, mailbox))
        .map(HierarchyRow::Mailbox)
        .chain(
            snapshot
                .collaboration_folders()
                .iter()
                .filter(|folder| restriction_matches_collaboration_folder(restriction, folder))
                .map(HierarchyRow::Collaboration),
        )
        .collect::<Vec<_>>();
    sort_hierarchy_rows(&mut rows, sort_orders);
    rows
}

fn sort_hierarchy_rows(rows: &mut [HierarchyRow<'_>], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_DISPLAY_NAME_W => compare_case_insensitive(
                    hierarchy_row_display_name(left),
                    hierarchy_row_display_name(right),
                ),
                PID_TAG_CONTENT_COUNT => {
                    hierarchy_row_content_count(left).cmp(&hierarchy_row_content_count(right))
                }
                PID_TAG_CONTENT_UNREAD_COUNT => {
                    hierarchy_row_unread_count(left).cmp(&hierarchy_row_unread_count(right))
                }
                PID_TAG_FOLDER_ID => hierarchy_row_id(left).cmp(&hierarchy_row_id(right)),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        hierarchy_row_id(left).cmp(&hierarchy_row_id(right))
    });
}

fn hierarchy_row_display_name<'a>(row: &'a HierarchyRow<'a>) -> &'a str {
    match row {
        HierarchyRow::Mailbox(mailbox) => &mailbox.name,
        HierarchyRow::Collaboration(folder) => &folder.collection.display_name,
    }
}

fn hierarchy_row_content_count(row: &HierarchyRow<'_>) -> u32 {
    match row {
        HierarchyRow::Mailbox(mailbox) => mailbox.total_emails,
        HierarchyRow::Collaboration(folder) => folder.item_count,
    }
}

fn hierarchy_row_unread_count(row: &HierarchyRow<'_>) -> u32 {
    match row {
        HierarchyRow::Mailbox(mailbox) => mailbox.unread_emails,
        HierarchyRow::Collaboration(_) => 0,
    }
}

fn hierarchy_row_id(row: &HierarchyRow<'_>) -> u64 {
    match row {
        HierarchyRow::Mailbox(mailbox) => mapi_folder_id(mailbox),
        HierarchyRow::Collaboration(folder) => folder.id,
    }
}

fn hierarchy_row_matches(row: &HierarchyRow<'_>, restriction: Option<&MapiRestriction>) -> bool {
    match row {
        HierarchyRow::Mailbox(mailbox) => restriction_matches_mailbox(restriction, mailbox),
        HierarchyRow::Collaboration(folder) => {
            restriction_matches_collaboration_folder(restriction, folder)
        }
    }
}

fn serialize_hierarchy_row(row: HierarchyRow<'_>, columns: &[u32]) -> Vec<u8> {
    match row {
        HierarchyRow::Mailbox(mailbox) => serialize_folder_row(mailbox, columns),
        HierarchyRow::Collaboration(folder) => serialize_collaboration_folder_row(folder, columns),
    }
}

pub(in crate::mapi) fn default_message_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_FLAG_STATUS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_BODY_W,
        PID_TAG_INTERNET_MESSAGE_ID_W,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

pub(in crate::mapi) fn default_contact_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_GIVEN_NAME_W,
        PID_TAG_SURNAME_W,
        PID_TAG_EMAIL_ADDRESS_W,
        PID_TAG_SMTP_ADDRESS_W,
        PID_TAG_MOBILE_TELEPHONE_NUMBER_W,
        PID_TAG_BUSINESS_TELEPHONE_NUMBER_W,
        PID_TAG_COMPANY_NAME_W,
        PID_TAG_TITLE_W,
        PID_TAG_BODY_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

pub(in crate::mapi) fn default_event_property_tags() -> Vec<u32> {
    vec![
        PID_TAG_MID,
        PID_TAG_ENTRY_ID,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_NORMALIZED_SUBJECT_W,
        PID_TAG_BODY_W,
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
        PID_TAG_LOCATION_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_SIZE,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
}

pub(in crate::mapi) fn rop_query_rows_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    if !object.as_deref().is_some_and(is_table_object) {
        return rop_error_response(0x15, request.response_handle_index(), 0x8004_0102);
    }

    let mut response = vec![0x15, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0x02);
    let mut start_position = 0usize;
    let mut position_base = 0usize;
    let rows = match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            columns,
            sort_orders,
            restriction,
            position: table_position,
            ..
        }) if is_root_hierarchy_folder(*folder_id) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            hierarchy_rows(mailboxes, snapshot, restriction.as_ref(), sort_orders)
                .into_iter()
                .map(|row| serialize_hierarchy_row(row, &columns))
                .collect::<Vec<_>>()
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            columns,
            sort_orders,
            restriction,
            position: table_position,
            ..
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_contents_columns()
            } else {
                columns.clone()
            };
            if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                match folder.kind {
                    MapiCollaborationFolderKind::Contacts => {
                        let mut rows = snapshot.contacts_for_folder(*folder_id);
                        rows.retain(|contact| {
                            restriction_matches_contact(restriction.as_ref(), &contact.contact)
                        });
                        sort_contacts(&mut rows, sort_orders);
                        rows.into_iter()
                            .map(|contact| {
                                serialize_contact_row(
                                    &contact.contact,
                                    contact.id,
                                    contact.folder_id,
                                    &columns,
                                )
                            })
                            .collect::<Vec<_>>()
                    }
                    MapiCollaborationFolderKind::Calendar => {
                        let mut rows = snapshot.events_for_folder(*folder_id);
                        rows.retain(|event| {
                            restriction_matches_event(restriction.as_ref(), &event.event)
                        });
                        sort_events(&mut rows, sort_orders);
                        rows.into_iter()
                            .map(|event| {
                                serialize_event_row(
                                    &event.event,
                                    event.id,
                                    event.folder_id,
                                    &columns,
                                )
                            })
                            .collect::<Vec<_>>()
                    }
                }
            } else {
                let window_offset = if request.query_forward_read() {
                    start_position
                } else {
                    start_position.saturating_sub(request.query_row_count().unwrap_or(0))
                };
                if let Some((_, window_emails)) = snapshot.content_table_window_emails(
                    *folder_id,
                    table_view_signature(sort_orders, restriction.as_ref()),
                    window_offset,
                    request.query_row_count().unwrap_or(0),
                ) {
                    position_base = window_offset;
                    start_position = if request.query_forward_read() {
                        0
                    } else {
                        window_emails.len()
                    };
                    window_emails
                        .into_iter()
                        .map(|email| serialize_message_row(email, &columns))
                        .collect::<Vec<_>>()
                } else {
                    let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                    rows.retain(|email| restriction_matches_email(restriction.as_ref(), email));
                    sort_emails(&mut rows, sort_orders);
                    rows.into_iter()
                        .map(|email| serialize_message_row(email, &columns))
                        .collect::<Vec<_>>()
                }
            }
        }
        Some(MapiObject::AttachmentTable {
            folder_id,
            message_id,
            columns,
            sort_orders,
            restriction,
            position: table_position,
            ..
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_attachment_columns()
            } else {
                columns.clone()
            };
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            rows.retain(|attachment| {
                restriction_matches_attachment(restriction.as_ref(), attachment)
            });
            sort_attachments(&mut rows, sort_orders);
            rows.into_iter()
                .map(|attachment| serialize_attachment_row(attachment, &columns))
                .collect::<Vec<_>>()
        }
        Some(MapiObject::PermissionTable {
            folder_id,
            columns,
            position: table_position,
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_permission_columns()
            } else {
                columns.clone()
            };
            snapshot
                .permissions_for_folder(*folder_id)
                .into_iter()
                .map(|permission| serialize_permission_row(&permission, &columns))
                .collect::<Vec<_>>()
        }
        _ => Vec::new(),
    };
    let row_count = request.query_row_count().unwrap_or(rows.len());
    let forward_read = request.query_forward_read();
    let (selected, next_position) = if forward_read {
        let selected = rows
            .into_iter()
            .skip(start_position)
            .take(row_count)
            .collect::<Vec<_>>();
        let next_position =
            position_base.saturating_add(start_position.saturating_add(selected.len()));
        (selected, next_position)
    } else {
        let end_position = start_position.min(rows.len());
        let selected_start = end_position.saturating_sub(row_count);
        let selected = rows[selected_start..end_position]
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        (selected, position_base.saturating_add(selected_start))
    };
    if !request.query_no_advance() {
        if let Some(
            MapiObject::HierarchyTable { position, .. }
            | MapiObject::ContentsTable { position, .. }
            | MapiObject::AttachmentTable { position, .. }
            | MapiObject::PermissionTable { position, .. },
        ) = object
        {
            *position = next_position;
        }
    }
    response.extend_from_slice(&(selected.len() as u16).to_le_bytes());
    for row in selected {
        write_standard_property_row(&mut response, &row);
    }
    response
}

pub(in crate::mapi) fn rop_query_columns_all_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let columns = match object {
        Some(MapiObject::HierarchyTable { .. }) => default_folder_property_tags(),
        Some(MapiObject::ContentsTable { folder_id, .. }) => match snapshot
            .collaboration_folder_for_id(*folder_id)
            .map(|folder| folder.kind)
        {
            Some(MapiCollaborationFolderKind::Contacts) => default_contact_property_tags(),
            Some(MapiCollaborationFolderKind::Calendar) => default_event_property_tags(),
            None => default_message_property_tags(),
        },
        Some(MapiObject::AttachmentTable { .. }) => default_attachment_columns(),
        Some(MapiObject::PermissionTable { .. }) => default_permission_columns(),
        _ => return rop_error_response(0x37, request.response_handle_index(), 0x8004_0102),
    };

    let mut response = vec![0x37, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for column in columns {
        response.extend_from_slice(&column.to_le_bytes());
    }
    response
}

pub(in crate::mapi) fn sort_emails(rows: &mut [&JmapEmail], sort_orders: &[MapiSortOrder]) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.subject, &right.subject)
                }
                PID_TAG_SENDER_NAME_W => compare_case_insensitive(
                    left.from_display.as_deref().unwrap_or(&left.from_address),
                    right.from_display.as_deref().unwrap_or(&right.from_address),
                ),
                PID_TAG_SENDER_EMAIL_ADDRESS_W => {
                    compare_case_insensitive(&left.from_address, &right.from_address)
                }
                PID_TAG_DISPLAY_TO_W => {
                    compare_case_insensitive(&display_to(left), &display_to(right))
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.received_at.cmp(&right.received_at)
                }
                PID_TAG_MESSAGE_FLAGS => message_flags(left).cmp(&message_flags(right)),
                PID_TAG_MESSAGE_SIZE => left.size_octets.cmp(&right.size_octets),
                PID_TAG_HAS_ATTACHMENTS => left.has_attachments.cmp(&right.has_attachments),
                PID_TAG_MID => mapi_message_id(left).cmp(&mapi_message_id(right)),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_attachments(
    rows: &mut [&MapiAttachment],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_ATTACH_NUM => left.attach_num.cmp(&right.attach_num),
                PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                    compare_case_insensitive(&left.file_name, &right.file_name)
                }
                PID_TAG_ATTACH_MIME_TAG_W => {
                    compare_case_insensitive(&left.media_type, &right.media_type)
                }
                PID_TAG_ATTACH_SIZE => left.size_octets.cmp(&right.size_octets),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_contacts(
    rows: &mut [&crate::mapi_store::MapiContact],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_DISPLAY_NAME_W | PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.contact.name, &right.contact.name)
                }
                PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
                    compare_case_insensitive(&left.contact.email, &right.contact.email)
                }
                PID_TAG_COMPANY_NAME_W => {
                    compare_case_insensitive(&left.contact.team, &right.contact.team)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn sort_events(
    rows: &mut [&crate::mapi_store::MapiEvent],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.event.title, &right.event.title)
                }
                PID_TAG_START_DATE
                | PID_TAG_MESSAGE_DELIVERY_TIME
                | PID_TAG_LAST_MODIFICATION_TIME => {
                    event_start_sort_key(&left.event).cmp(&event_start_sort_key(&right.event))
                }
                PID_TAG_LOCATION_W => {
                    compare_case_insensitive(&left.event.location, &right.event.location)
                }
                PID_TAG_MID => left.id.cmp(&right.id),
                _ => Ordering::Equal,
            };
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

pub(in crate::mapi) fn apply_sort_direction(ordering: Ordering, sort_order: u8) -> Ordering {
    if sort_order == 0x01 {
        ordering.reverse()
    } else {
        ordering
    }
}

pub(in crate::mapi) fn compare_case_insensitive(left: &str, right: &str) -> Ordering {
    left.to_ascii_lowercase().cmp(&right.to_ascii_lowercase())
}

pub(in crate::mapi) fn table_view_signature(
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    fn push_bytes(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash = (*hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
        }
    }

    fn push_restriction(hash: &mut u64, restriction: &MapiRestriction) {
        match restriction {
            MapiRestriction::And(children) => {
                push_bytes(hash, b"and");
                for child in children {
                    push_restriction(hash, child);
                }
            }
            MapiRestriction::Or(children) => {
                push_bytes(hash, b"or");
                for child in children {
                    push_restriction(hash, child);
                }
            }
            MapiRestriction::Not(child) => {
                push_bytes(hash, b"not");
                push_restriction(hash, child);
            }
            MapiRestriction::Content {
                property_tag,
                value,
            } => {
                push_bytes(hash, b"content");
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, value.as_bytes());
            }
            MapiRestriction::Property {
                relop,
                property_tag,
                value,
            } => {
                push_bytes(hash, b"property");
                push_bytes(hash, &[*relop]);
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, format!("{value:?}").as_bytes());
            }
            MapiRestriction::Bitmask {
                property_tag,
                mask,
                must_be_nonzero,
            } => {
                push_bytes(hash, b"bitmask");
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, &mask.to_le_bytes());
                push_bytes(hash, &[*must_be_nonzero as u8]);
            }
            MapiRestriction::Size {
                relop,
                property_tag,
                size,
            } => {
                push_bytes(hash, b"size");
                push_bytes(hash, &[*relop]);
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, &size.to_le_bytes());
            }
            MapiRestriction::Exist { property_tag } => {
                push_bytes(hash, b"exist");
                push_bytes(hash, &property_tag.to_le_bytes());
            }
        }
    }

    let mut hash = FNV_OFFSET;
    for sort_order in sort_orders {
        push_bytes(&mut hash, &sort_order.property_tag.to_le_bytes());
        push_bytes(&mut hash, &[sort_order.order]);
    }
    if let Some(restriction) = restriction {
        push_restriction(&mut hash, restriction);
    }
    hash
}

pub(in crate::mapi) fn serialize_attachment_row(
    attachment: &MapiAttachment,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attachment.attach_num),
            PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &attachment.file_name)
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &attachment.media_type),
            PID_TAG_ATTACH_SIZE => {
                write_u32(&mut row, attachment.size_octets.min(u32::MAX as u64) as u32)
            }
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, 1),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ENTRY_ID => {
                write_u16_prefixed_bytes(&mut row, attachment.canonical_id.as_bytes())
            }
            PID_TAG_INSTANCE_KEY => {
                write_u16_prefixed_bytes(&mut row, attachment.file_reference.as_bytes())
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn rop_get_status_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Vec<u8> {
    if !object.is_some_and(is_table_object) {
        return rop_error_response(0x16, request.response_handle_index(), 0x8004_0102);
    }

    let mut response = vec![0x16, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

pub(in crate::mapi) fn is_table_object(object: &MapiObject) -> bool {
    matches!(
        object,
        MapiObject::HierarchyTable { .. }
            | MapiObject::ContentsTable { .. }
            | MapiObject::AttachmentTable { .. }
            | MapiObject::PermissionTable { .. }
    )
}

pub(in crate::mapi) fn rop_query_position_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    if !object.is_some_and(is_table_object) {
        return rop_error_response(0x17, request.response_handle_index(), 0x8004_0102);
    }

    let (position, row_count) = table_position_and_count(object, mailboxes, emails, snapshot);
    let mut response = vec![0x17, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, position as u32);
    write_u32(&mut response, row_count as u32);
    response
}

pub(in crate::mapi) fn rop_seek_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };
    let total_rows = table_position_and_count(Some(object), mailboxes, emails, snapshot).1;
    let Some(position) = table_position_mut(object) else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };

    let requested_rows = request.seek_row_count().unwrap_or(0);
    let base_position = match request.seek_origin().unwrap_or(1) {
        0 => 0isize,
        2 => total_rows as isize,
        _ => *position as isize,
    };
    let requested_position = base_position.saturating_add(requested_rows as isize);
    let new_position = requested_position.clamp(0, total_rows as isize);
    let rows_sought = (new_position - base_position) as i32;
    *position = new_position as usize;

    let mut response = vec![0x18, request.response_handle_index()];
    write_u32(&mut response, 0);
    let want_row_moved_count = request.want_row_moved_count();
    response.push((want_row_moved_count && rows_sought != requested_rows) as u8);
    response.extend_from_slice(&if want_row_moved_count { rows_sought } else { 0 }.to_le_bytes());
    response
}

pub(in crate::mapi) fn rop_create_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x1B, request.response_handle_index(), 0x8004_0102);
    };
    let row_key = table_row_keys(object, mailboxes, emails, snapshot)
        .get(table_position(object).unwrap_or(0))
        .copied();
    let Some((position, bookmarks, next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x1B, request.response_handle_index(), 0x8004_0102);
    };
    let bookmark = next_bookmark.to_le_bytes().to_vec();
    bookmarks.insert(
        bookmark.clone(),
        TableBookmark {
            position: *position,
            row_key,
        },
    );
    *next_bookmark = next_bookmark.saturating_add(1).max(1);

    let mut response = vec![0x1B, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    response.extend_from_slice(&bookmark);
    response
}

pub(in crate::mapi) fn rop_seek_row_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0102);
    };
    let row_keys = table_row_keys(object, mailboxes, emails, snapshot);
    let total_rows = row_keys.len();
    let Some((position, bookmarks, _next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0102);
    };
    let Some(bookmark) = bookmarks.get(request.bookmark()).cloned() else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0405);
    };
    let mut row_no_longer_visible = false;
    let base_position = match bookmark.row_key {
        Some(row_key) => row_keys
            .iter()
            .position(|key| *key == row_key)
            .unwrap_or_else(|| {
                row_no_longer_visible = true;
                bookmark.position.min(total_rows)
            }),
        None => bookmark.position.min(total_rows),
    };

    let requested_rows = request.bookmark_row_count().unwrap_or(0);
    let requested_position = (base_position as isize).saturating_add(requested_rows as isize);
    let new_position = requested_position.clamp(0, total_rows as isize);
    let rows_sought = (new_position - base_position as isize) as i32;
    *position = new_position as usize;

    let mut response = vec![0x19, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(row_no_longer_visible as u8);
    response.push((request.bookmark_want_row_moved_count() && rows_sought != requested_rows) as u8);
    response.extend_from_slice(
        &if request.bookmark_want_row_moved_count() {
            rows_sought
        } else {
            0
        }
        .to_le_bytes(),
    );
    response
}

pub(in crate::mapi) fn rop_seek_row_fractional_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    let total_rows = table_position_and_count(Some(object), mailboxes, emails, snapshot).1;
    let Some(position) = table_position_mut(object) else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    let Some((numerator, denominator)) = request.fractional_position() else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    if denominator == 0 {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    }
    let new_position = (total_rows as u128)
        .saturating_mul(numerator as u128)
        .checked_div(denominator as u128)
        .unwrap_or(0)
        .min(total_rows as u128) as usize;
    *position = new_position;

    let mut response = vec![0x1A, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_free_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    let Some((_position, bookmarks, _next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    bookmarks.remove(request.bookmark());

    let mut response = vec![0x89, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_find_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Ok(restriction) = request.restriction() else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    let Some(restriction) = restriction else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };

    let Some(object) = object else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    let mut response = vec![0x4F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);

    match object {
        MapiObject::HierarchyTable {
            folder_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } if is_root_hierarchy_folder(*folder_id) => {
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            let rows = hierarchy_rows(mailboxes, snapshot, table_restriction.as_ref(), sort_orders);
            if let Some((index, row)) =
                find_hierarchy_row(rows.as_slice(), *position, request, Some(&restriction))
            {
                *position = index;
                response.push(1);
                write_standard_property_row(&mut response, &serialize_hierarchy_row(row, &columns));
            } else {
                response.push(0);
            }
        }
        MapiObject::ContentsTable {
            folder_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } => {
            let columns = if columns.is_empty() {
                default_contents_columns()
            } else {
                columns.clone()
            };
            let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
            rows.retain(|email| restriction_matches_email(table_restriction.as_ref(), email));
            sort_emails(&mut rows, sort_orders);
            if let Some((index, email)) = find_row(rows.as_slice(), *position, request, |email| {
                restriction_matches_email(Some(&restriction), email)
            }) {
                *position = index;
                response.push(1);
                write_standard_property_row(&mut response, &serialize_message_row(email, &columns));
            } else {
                response.push(0);
            }
        }
        MapiObject::AttachmentTable {
            folder_id,
            message_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } => {
            let columns = if columns.is_empty() {
                default_attachment_columns()
            } else {
                columns.clone()
            };
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            rows.retain(|attachment| {
                restriction_matches_attachment(table_restriction.as_ref(), attachment)
            });
            sort_attachments(&mut rows, sort_orders);
            if let Some((index, attachment)) =
                find_row(rows.as_slice(), *position, request, |attachment| {
                    restriction_matches_attachment(Some(&restriction), attachment)
                })
            {
                *position = index;
                response.push(1);
                write_standard_property_row(
                    &mut response,
                    &serialize_attachment_row(attachment, &columns),
                );
            } else {
                response.push(0);
            }
        }
        _ => return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102),
    }

    response
}

pub(in crate::mapi) fn find_row<'a, T>(
    rows: &'a [&'a T],
    current_position: usize,
    request: &RopRequest,
    matches: impl Fn(&T) -> bool,
) -> Option<(usize, &'a T)> {
    if rows.is_empty() {
        return None;
    }
    let start = match request.find_origin().unwrap_or(1) {
        0 => 0,
        2 => rows.len().saturating_sub(1),
        _ => current_position.min(rows.len()),
    };
    if request.find_backward() {
        let end = start.min(rows.len().saturating_sub(1));
        (0..=end)
            .rev()
            .find_map(|index| matches(rows[index]).then_some((index, rows[index])))
    } else {
        rows.iter()
            .enumerate()
            .skip(start)
            .find_map(|(index, row)| matches(row).then_some((index, *row)))
    }
}

fn find_hierarchy_row<'a>(
    rows: &'a [HierarchyRow<'a>],
    current_position: usize,
    request: &RopRequest,
    restriction: Option<&MapiRestriction>,
) -> Option<(usize, HierarchyRow<'a>)> {
    if rows.is_empty() {
        return None;
    }
    let start = match request.find_origin().unwrap_or(1) {
        0 => 0,
        2 => rows.len().saturating_sub(1),
        _ => current_position.min(rows.len()),
    };
    if request.find_backward() {
        let end = start.min(rows.len().saturating_sub(1));
        (0..=end).rev().find_map(|index| {
            hierarchy_row_matches(&rows[index], restriction).then_some((index, rows[index]))
        })
    } else {
        rows.iter()
            .enumerate()
            .skip(start)
            .find_map(|(index, row)| {
                hierarchy_row_matches(row, restriction).then_some((index, *row))
            })
    }
}

pub(in crate::mapi) fn table_position_and_count(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> (usize, usize) {
    match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            position,
            restriction,
            sort_orders,
            ..
        }) if is_root_hierarchy_folder(*folder_id) => {
            let total =
                hierarchy_rows(mailboxes, snapshot, restriction.as_ref(), sort_orders).len();
            (*position, total)
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            position,
            restriction,
            sort_orders,
            ..
        }) => {
            let total = snapshot
                .content_table_total(
                    *folder_id,
                    table_view_signature(sort_orders, restriction.as_ref()),
                )
                .unwrap_or_else(|| {
                    emails_for_folder(*folder_id, mailboxes, emails)
                        .into_iter()
                        .filter(|email| restriction_matches_email(restriction.as_ref(), email))
                        .count()
                });
            (*position, total)
        }
        Some(MapiObject::AttachmentTable {
            folder_id,
            message_id,
            position,
            restriction,
            ..
        }) => (
            *position,
            snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .filter(|attachment| {
                    restriction_matches_attachment(restriction.as_ref(), attachment)
                })
                .count(),
        ),
        Some(MapiObject::PermissionTable {
            folder_id,
            position,
            ..
        }) => (*position, snapshot.permissions_for_folder(*folder_id).len()),
        _ => (0, 0),
    }
}

pub(in crate::mapi) fn table_position_mut(object: &mut MapiObject) -> Option<&mut usize> {
    match object {
        MapiObject::HierarchyTable { position, .. }
        | MapiObject::ContentsTable { position, .. }
        | MapiObject::AttachmentTable { position, .. }
        | MapiObject::PermissionTable { position, .. } => Some(position),
        _ => None,
    }
}

pub(in crate::mapi) fn table_position(object: &MapiObject) -> Option<usize> {
    match object {
        MapiObject::HierarchyTable { position, .. }
        | MapiObject::ContentsTable { position, .. }
        | MapiObject::AttachmentTable { position, .. }
        | MapiObject::PermissionTable { position, .. } => Some(*position),
        _ => None,
    }
}

pub(in crate::mapi) fn table_bookmark_state_mut(
    object: &mut MapiObject,
) -> Option<(&mut usize, &mut HashMap<Vec<u8>, TableBookmark>, &mut u32)> {
    match object {
        MapiObject::HierarchyTable {
            position,
            bookmarks,
            next_bookmark,
            ..
        }
        | MapiObject::ContentsTable {
            position,
            bookmarks,
            next_bookmark,
            ..
        } => Some((position, bookmarks, next_bookmark)),
        _ => None,
    }
}

pub(in crate::mapi) fn table_row_keys(
    object: &MapiObject,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u64> {
    match object {
        MapiObject::HierarchyTable {
            folder_id,
            sort_orders,
            restriction,
            ..
        } if is_root_hierarchy_folder(*folder_id) => {
            hierarchy_rows(mailboxes, snapshot, restriction.as_ref(), sort_orders)
                .into_iter()
                .map(|row| hierarchy_row_id(&row))
                .collect()
        }
        MapiObject::ContentsTable {
            folder_id,
            sort_orders,
            restriction,
            ..
        } => {
            let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
            rows.retain(|email| restriction_matches_email(restriction.as_ref(), email));
            sort_emails(&mut rows, sort_orders);
            rows.into_iter().map(mapi_message_id).collect()
        }
        MapiObject::AttachmentTable {
            folder_id,
            message_id,
            sort_orders,
            restriction,
            ..
        } => {
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            rows.retain(|attachment| {
                restriction_matches_attachment(restriction.as_ref(), attachment)
            });
            sort_attachments(&mut rows, sort_orders);
            rows.into_iter()
                .map(|attachment| u64::from(attachment.attach_num))
                .collect()
        }
        MapiObject::PermissionTable { folder_id, .. } => snapshot
            .permissions_for_folder(*folder_id)
            .into_iter()
            .map(|permission| {
                permission
                    .member_account_id
                    .and_then(|id| crate::mapi::identity::mapped_mapi_object_id(&id))
                    .unwrap_or(0)
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub(in crate::mapi) fn is_root_hierarchy_folder(folder_id: u64) -> bool {
    matches!(folder_id, ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID)
}

pub(in crate::mapi) fn role_for_folder_id(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        INBOX_FOLDER_ID => Some("inbox"),
        DRAFTS_FOLDER_ID => Some("drafts"),
        SENT_FOLDER_ID => Some("sent"),
        TRASH_FOLDER_ID => Some("trash"),
        OUTBOX_FOLDER_ID => Some("outbox"),
        CONTACTS_FOLDER_ID => Some("contacts"),
        CALENDAR_FOLDER_ID => Some("calendar"),
        JOURNAL_FOLDER_ID => Some("journal"),
        NOTES_FOLDER_ID => Some("notes"),
        TASKS_FOLDER_ID => Some("tasks"),
        REMINDERS_FOLDER_ID => Some("reminders"),
        _ => None,
    }
}

pub(in crate::mapi) fn serialize_special_folder_row(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    columns: &[u32],
) -> Vec<u8> {
    match folder_id {
        IPM_SUBTREE_FOLDER_ID => serialize_ipm_subtree_folder_row(mailboxes, columns),
        ROOT_FOLDER_ID => serialize_root_folder_row(mailboxes, columns),
        _ => serialize_advertised_special_folder_row(folder_id, columns),
    }
}

fn serialize_advertised_special_folder_row(folder_id: u64, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    let (display_name, parent_folder_id, message_class, has_subfolders) =
        special_folder_metadata(folder_id);
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, display_name),
            PID_TAG_FOLDER_ID => write_u64(&mut row, folder_id),
            PID_TAG_PARENT_FOLDER_ID => write_u64(&mut row, parent_folder_id),
            PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            PID_TAG_SUBFOLDERS => row.push(has_subfolders as u8),
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_LOCAL_COMMIT_TIME_MAX
            | PID_TAG_HIER_REV => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_change_number(folder_id),
            ),
            PID_TAG_HIERARCHY_CHANGE_NUMBER => {
                write_u32(&mut row, folder_id.min(u64::from(u32::MAX)) as u32)
            }
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(folder_id),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(parent_folder_id),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(folder_id),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(folder_id),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, folder_id),
            _ if folder_id == INBOX_FOLDER_ID => {
                match special_folder_identification_property_value(*column) {
                    Some(value) => write_mapi_value(&mut row, *column, &value),
                    None => write_property_default(&mut row, *column),
                }
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn special_folder_metadata(folder_id: u64) -> (&'static str, u64, &'static str, bool) {
    match folder_id {
        DEFERRED_ACTION_FOLDER_ID => ("Deferred Action", ROOT_FOLDER_ID, "IPF.Root", false),
        SPOOLER_QUEUE_FOLDER_ID => ("Spooler Queue", ROOT_FOLDER_ID, "IPF.Root", false),
        INBOX_FOLDER_ID => ("Inbox", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        OUTBOX_FOLDER_ID => ("Outbox", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        SENT_FOLDER_ID => ("Sent", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        TRASH_FOLDER_ID => ("Deleted Items", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        COMMON_VIEWS_FOLDER_ID => ("Common Views", ROOT_FOLDER_ID, "IPF.Root", false),
        SCHEDULE_FOLDER_ID => ("Schedule", ROOT_FOLDER_ID, "IPF.Root", false),
        SEARCH_FOLDER_ID => ("Search", ROOT_FOLDER_ID, "IPF.Root", false),
        VIEWS_FOLDER_ID => ("Views", ROOT_FOLDER_ID, "IPF.Root", false),
        SHORTCUTS_FOLDER_ID => ("Shortcuts", ROOT_FOLDER_ID, "IPF.Root", false),
        DRAFTS_FOLDER_ID => ("Drafts", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        CONTACTS_FOLDER_ID => ("Contacts", IPM_SUBTREE_FOLDER_ID, "IPF.Contact", false),
        CALENDAR_FOLDER_ID => ("Calendar", IPM_SUBTREE_FOLDER_ID, "IPF.Appointment", false),
        JOURNAL_FOLDER_ID => ("Journal", IPM_SUBTREE_FOLDER_ID, "IPF.Journal", false),
        NOTES_FOLDER_ID => ("Notes", IPM_SUBTREE_FOLDER_ID, "IPF.StickyNote", false),
        TASKS_FOLDER_ID => ("Tasks", IPM_SUBTREE_FOLDER_ID, "IPF.Task", false),
        REMINDERS_FOLDER_ID => ("Reminders", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        _ => ("Root", 0, "IPF.Root", true),
    }
}

pub(in crate::mapi) fn serialize_root_folder_row(
    mailboxes: &[JmapMailbox],
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, "Root"),
            PID_TAG_FOLDER_ID => write_u64(&mut row, ROOT_FOLDER_ID),
            PID_TAG_PARENT_FOLDER_ID => write_u64(&mut row, 0),
            PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            PID_TAG_SUBFOLDERS => row.push((!mailboxes.is_empty()) as u8),
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, "IPF.Root"),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPF.Root"),
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_LOCAL_COMMIT_TIME_MAX
            | PID_TAG_HIER_REV => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_change_number(ROOT_FOLDER_ID),
            ),
            PID_TAG_HIERARCHY_CHANGE_NUMBER => {
                write_u32(&mut row, ROOT_FOLDER_ID.min(u64::from(u32::MAX)) as u32)
            }
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(ROOT_FOLDER_ID),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(&mut row, &[]),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(ROOT_FOLDER_ID),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(ROOT_FOLDER_ID),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, ROOT_FOLDER_ID),
            _ => match special_folder_identification_property_value(*column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_ipm_subtree_folder_row(
    mailboxes: &[JmapMailbox],
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, "Top of Information Store"),
            PID_TAG_FOLDER_ID => write_u64(&mut row, IPM_SUBTREE_FOLDER_ID),
            PID_TAG_PARENT_FOLDER_ID => write_u64(&mut row, ROOT_FOLDER_ID),
            PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            PID_TAG_SUBFOLDERS => row.push((!mailboxes.is_empty()) as u8),
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, "IPF.Note"),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPF.Note"),
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_LOCAL_COMMIT_TIME_MAX
            | PID_TAG_HIER_REV => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_change_number(IPM_SUBTREE_FOLDER_ID),
            ),
            PID_TAG_HIERARCHY_CHANGE_NUMBER => write_u32(
                &mut row,
                IPM_SUBTREE_FOLDER_ID.min(u64::from(u32::MAX)) as u32,
            ),
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(ROOT_FOLDER_ID),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(IPM_SUBTREE_FOLDER_ID),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(IPM_SUBTREE_FOLDER_ID),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, IPM_SUBTREE_FOLDER_ID),
            PID_TAG_OST_OSTID => write_u16_prefixed_bytes(&mut row, &[]),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn write_standard_property_row(response: &mut Vec<u8>, values: &[u8]) {
    response.push(0);
    response.extend_from_slice(values);
}

pub(in crate::mapi) fn write_logon_property_row(
    response: &mut Vec<u8>,
    principal: &AccountPrincipal,
    columns: &[u32],
) {
    if columns
        .iter()
        .all(|column| logon_property_value(principal, *column).is_some())
    {
        write_standard_property_row(response, &serialize_logon_row(principal, columns));
        return;
    }

    response.push(1);
    for column in columns {
        match logon_property_value(principal, *column) {
            Some(value) => {
                response.push(0);
                write_mapi_value(response, *column, &value);
            }
            None => {
                response.push(0x0A);
                write_u32(response, ROP_ERROR_NOT_SUPPORTED);
            }
        }
    }
}

pub(in crate::mapi) fn serialize_logon_row(
    principal: &AccountPrincipal,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match logon_property_value(principal, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialized_replid_guid_map() -> Vec<u8> {
    let mut value = Vec::with_capacity(18);
    value.extend_from_slice(&(STORE_REPLICA_ID as u16).to_le_bytes());
    value.extend_from_slice(&crate::mapi::identity::STORE_REPLICA_GUID);
    value
}

pub(in crate::mapi) fn serialize_pending_attachment_row(
    attach_num: u32,
    properties: &HashMap<u32, MapiValue>,
    data: &[u8],
    columns: &[u32],
) -> Vec<u8> {
    let file_name = pending_attachment_file_name(attach_num, properties);
    let media_type = pending_attachment_media_type(properties);
    let size = data.len().min(u32::MAX as usize) as u32;
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) = properties.get(column) {
            write_mapi_value(&mut row, *column, value);
            continue;
        }
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, &file_name)
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, &media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size),
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, 1),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ATTACH_DATA_BINARY => write_u16_prefixed_bytes(&mut row, data),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_saved_attachment_row(
    attach_num: u32,
    file_reference: &str,
    file_name: &str,
    media_type: &str,
    size_octets: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_ATTACH_NUM => write_u32(&mut row, attach_num),
            PID_TAG_ATTACH_FILENAME_W | PID_TAG_ATTACH_LONG_FILENAME_W => {
                write_utf16z(&mut row, file_name)
            }
            PID_TAG_ATTACH_MIME_TAG_W => write_utf16z(&mut row, media_type),
            PID_TAG_ATTACH_SIZE => write_u32(&mut row, size_octets.min(u32::MAX as u64) as u32),
            PID_TAG_ATTACH_METHOD => write_u32(&mut row, 1),
            PID_TAG_RENDERING_POSITION => write_u32(&mut row, u32::MAX),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
                write_u16_prefixed_bytes(&mut row, file_reference.as_bytes())
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_folder_row(mailbox: &JmapMailbox, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &mailbox.name),
            PID_TAG_FOLDER_ID => write_u64(&mut row, mapi_folder_id(mailbox)),
            PID_TAG_PARENT_FOLDER_ID => write_u64(&mut row, mapi_parent_folder_id(mailbox)),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, mailbox.total_emails),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, mailbox.unread_emails),
            PID_TAG_SUBFOLDERS => row.push(0),
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, folder_message_class(mailbox)),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, folder_message_class(mailbox)),
            _ => match mailbox_property_value(mailbox, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_collaboration_folder_row(
    folder: &MapiCollaborationFolder,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &folder.collection.display_name),
            PID_TAG_FOLDER_ID => write_u64(&mut row, folder.id),
            PID_TAG_PARENT_FOLDER_ID => write_u64(&mut row, IPM_SUBTREE_FOLDER_ID),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, folder.item_count),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            PID_TAG_SUBFOLDERS => row.push(0),
            PID_TAG_CONTAINER_CLASS_W => {
                write_utf16z(&mut row, collaboration_folder_message_class(folder.kind))
            }
            PID_TAG_MESSAGE_CLASS_W => {
                write_utf16z(&mut row, collaboration_folder_message_class(folder.kind))
            }
            _ => match collaboration_folder_property_value(folder, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_message_row(email: &JmapEmail, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_MID => write_u64(&mut row, mapi_message_id(email)),
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                write_utf16z(&mut row, &email.subject)
            }
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPM.Note"),
            PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at),
            ),
            PID_TAG_MESSAGE_FLAGS => write_u32(&mut row, message_flags(email)),
            PID_TAG_MESSAGE_SIZE => {
                write_u32(&mut row, email.size_octets.clamp(0, u32::MAX as i64) as u32)
            }
            PID_TAG_SENDER_NAME_W => write_utf16z(
                &mut row,
                email.from_display.as_deref().unwrap_or(&email.from_address),
            ),
            PID_TAG_SENDER_EMAIL_ADDRESS_W => write_utf16z(&mut row, &email.from_address),
            PID_TAG_DISPLAY_TO_W => write_utf16z(&mut row, &display_to(email)),
            PID_TAG_HAS_ATTACHMENTS => row.push(email.has_attachments as u8),
            PID_TAG_BODY_W => write_utf16z(&mut row, &email.body_text),
            PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(mapi_message_id(email)),
            ),
            PID_TAG_INTERNET_MESSAGE_ID_W => {
                write_utf16z(&mut row, email.internet_message_id.as_deref().unwrap_or(""))
            }
            _ => match email_property_value(email, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_contact_row(
    contact: &AccessibleContact,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match contact_property_value(contact, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_event_row(
    event: &AccessibleEvent,
    item_id: u64,
    folder_id: u64,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match event_property_value(event, item_id, folder_id, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialize_pending_message_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) = pending_message_property_value(principal, properties, *column) {
            write_mapi_value(&mut row, *column, &value);
        } else {
            write_property_default(&mut row, *column);
        }
    }
    row
}

pub(in crate::mapi) fn pending_message_property_value(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Option<MapiValue> {
    let lookup_tag = canonical_property_storage_tag(property_tag);
    properties
        .get(&lookup_tag)
        .cloned()
        .or_else(|| match lookup_tag {
            PID_TAG_NORMALIZED_SUBJECT_W => properties.get(&PID_TAG_SUBJECT_W).cloned(),
            PID_TAG_SUBJECT_W => properties.get(&PID_TAG_NORMALIZED_SUBJECT_W).cloned(),
            PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Note".to_string())),
            PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(0x0000_0008)),
            PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
            PID_TAG_MESSAGE_SIZE => Some(MapiValue::I64(pending_message_size(properties))),
            PID_TAG_SENDER_NAME_W => Some(MapiValue::String(principal.display_name.clone())),
            PID_TAG_SENDER_EMAIL_ADDRESS_W => Some(MapiValue::String(principal.email.clone())),
            _ => None,
        })
}

pub(in crate::mapi) fn serialize_pending_contact_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let contact = contact_input_from_mapi(
        principal.account_id,
        None,
        &default_contact_for_mapping(principal.account_id, "default"),
        properties,
    );
    let contact = AccessibleContact {
        id: Uuid::nil(),
        collection_id: "default".to_string(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        rights: default_mapping_rights(),
        name: contact.name,
        role: contact.role,
        email: contact.email,
        phone: contact.phone,
        team: contact.team,
        notes: contact.notes,
    };
    serialize_contact_row(&contact, 0, CONTACTS_FOLDER_ID, columns)
}

pub(in crate::mapi) fn serialize_pending_event_row(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    columns: &[u32],
) -> Vec<u8> {
    let event = event_input_from_mapi(
        principal.account_id,
        None,
        &default_event_for_mapping(principal.account_id, "default"),
        properties,
    )
    .unwrap_or_else(|_| default_event_input(principal.account_id, None));
    let event = AccessibleEvent {
        id: Uuid::nil(),
        uid: Uuid::nil().to_string(),
        collection_id: "default".to_string(),
        owner_account_id: principal.account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        rights: default_mapping_rights(),
        date: event.date,
        time: event.time,
        time_zone: event.time_zone,
        duration_minutes: event.duration_minutes,
        recurrence_rule: event.recurrence_rule,
        title: event.title,
        location: event.location,
        attendees: event.attendees,
        attendees_json: event.attendees_json,
        notes: event.notes,
    };
    serialize_event_row(&event, 0, CALENDAR_FOLDER_ID, columns)
}

pub(in crate::mapi) fn display_to(email: &JmapEmail) -> String {
    email
        .to
        .iter()
        .map(|address| {
            address
                .display_name
                .as_deref()
                .unwrap_or(&address.address)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("; ")
}

pub(in crate::mapi) struct MapiRecipient<'a> {
    pub(in crate::mapi) recipient_type: u8,
    pub(in crate::mapi) address: &'a JmapEmailAddress,
}

pub(in crate::mapi) fn message_recipients(email: &JmapEmail) -> Vec<MapiRecipient<'_>> {
    email
        .to
        .iter()
        .map(|address| MapiRecipient {
            recipient_type: 0x01,
            address,
        })
        .chain(email.cc.iter().map(|address| MapiRecipient {
            recipient_type: 0x02,
            address,
        }))
        .chain(
            message_can_expose_bcc(email)
                .then_some(email.bcc.iter())
                .into_iter()
                .flatten()
                .map(|address| MapiRecipient {
                    recipient_type: 0x03,
                    address,
                }),
        )
        .collect()
}

pub(in crate::mapi) fn message_can_expose_bcc(email: &JmapEmail) -> bool {
    matches!(email.mailbox_role.as_str(), "drafts" | "sent")
}

pub(in crate::mapi) fn serialize_recipient_row(address: &JmapEmailAddress) -> Vec<u8> {
    let mut row = Vec::new();
    let recipient_flags = 0x0200u16 | 0x0010 | 0x0008 | 0x0003;
    row.extend_from_slice(&recipient_flags.to_le_bytes());
    write_utf16z(&mut row, &address.address);
    write_utf16z(
        &mut row,
        address.display_name.as_deref().unwrap_or(&address.address),
    );
    row.extend_from_slice(&0u16.to_le_bytes());
    row
}

pub(in crate::mapi) fn serialize_pending_recipient_row(recipient: &PendingRecipient) -> Vec<u8> {
    let address = JmapEmailAddress {
        address: recipient.address.clone(),
        display_name: recipient.display_name.clone(),
    };
    serialize_recipient_row(&address)
}

pub(in crate::mapi) fn message_flags(email: &JmapEmail) -> u32 {
    mapi_mailstore::canonical_message_flags(email)
}

pub(in crate::mapi) fn contact_size(contact: &AccessibleContact) -> i64 {
    contact
        .name
        .len()
        .saturating_add(contact.email.len())
        .saturating_add(contact.phone.len())
        .saturating_add(contact.team.len())
        .saturating_add(contact.notes.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn event_size(event: &AccessibleEvent) -> i64 {
    event
        .title
        .len()
        .saturating_add(event.location.len())
        .saturating_add(event.notes.len())
        .min(i64::MAX as usize) as i64
}

pub(in crate::mapi) fn event_start_sort_key(event: &AccessibleEvent) -> String {
    format!("{}T{}", event.date, event.time)
}

pub(in crate::mapi) fn event_start_filetime(event: &AccessibleEvent) -> u64 {
    date_time_to_filetime(&event.date, &event.time)
}

pub(in crate::mapi) fn event_end_filetime(event: &AccessibleEvent) -> u64 {
    let start = event_start_filetime(event);
    let duration = event.duration_minutes.max(0) as u64 * 60 * 10_000_000;
    start.saturating_add(duration)
}

pub(in crate::mapi) fn date_time_to_filetime(date: &str, time: &str) -> u64 {
    let year = date
        .get(0..4)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(1970);
    let month = date
        .get(5..7)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let day = date
        .get(8..10)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let hour = time
        .get(0..2)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let minute = time
        .get(3..5)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let days = days_from_civil(year, month, day).max(0) as u64;
    let unix_seconds = days
        .saturating_mul(86_400)
        .saturating_add(u64::from(hour.min(23)) * 3_600)
        .saturating_add(u64::from(minute.min(59)) * 60);
    unix_seconds_to_filetime(unix_seconds)
}

pub(in crate::mapi) fn filetime_to_date_time(filetime: i64) -> Option<(String, String)> {
    let filetime = u64::try_from(filetime).ok()?;
    let unix_seconds = filetime_to_unix_seconds(filetime)?;
    let days = unix_seconds / 86_400;
    let seconds = unix_seconds % 86_400;
    let (year, month, day) = civil_from_unix_days(days as i64);
    let hour = seconds / 3_600;
    let minute = (seconds % 3_600) / 60;
    Some((
        format!("{year:04}-{month:02}-{day:02}"),
        format!("{hour:02}:{minute:02}"),
    ))
}

pub(in crate::mapi) fn unix_seconds_to_filetime(unix_seconds: u64) -> u64 {
    unix_seconds
        .saturating_add(11_644_473_600)
        .saturating_mul(10_000_000)
}

pub(in crate::mapi) fn filetime_to_unix_seconds(filetime: u64) -> Option<u64> {
    filetime
        .checked_div(10_000_000)?
        .checked_sub(11_644_473_600)
}

pub(in crate::mapi) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(in crate::mapi) fn unread_from_read_flags(read_flags: Option<u8>) -> Option<bool> {
    match read_flags {
        Some(flags) if flags & 0x10 != 0 => None,
        Some(flags) if flags & 0x04 != 0 => Some(true),
        Some(_) => Some(false),
        None => Some(false),
    }
}

pub(in crate::mapi) fn folder_message_class(mailbox: &JmapMailbox) -> &'static str {
    match mailbox.role.as_str() {
        "contacts" => "IPF.Contact",
        "calendar" => "IPF.Appointment",
        "journal" => "IPF.Journal",
        "notes" => "IPF.StickyNote",
        "tasks" => "IPF.Task",
        _ => "IPF.Note",
    }
}

pub(in crate::mapi) fn collaboration_folder_message_class(
    kind: MapiCollaborationFolderKind,
) -> &'static str {
    match kind {
        MapiCollaborationFolderKind::Contacts => "IPF.Contact",
        MapiCollaborationFolderKind::Calendar => "IPF.Appointment",
    }
}

pub(in crate::mapi) fn write_property_default(row: &mut Vec<u8>, property_tag: u32) {
    match MapiPropertyTag::new(property_tag).property_type() {
        Some(MapiPropertyType::Integer16) => write_u16(row, 0),
        Some(MapiPropertyType::Integer32) | Some(MapiPropertyType::Error) => write_u32(row, 0),
        Some(MapiPropertyType::Boolean) => row.push(0),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => write_u64(row, 0),
        Some(MapiPropertyType::String8) => write_ascii_z(row, ""),
        Some(MapiPropertyType::String) => write_utf16z(row, ""),
        Some(MapiPropertyType::Guid) => row.extend_from_slice(Uuid::nil().as_bytes()),
        Some(MapiPropertyType::Binary) => write_rop_binary(row, &[]),
        Some(
            MapiPropertyType::MultipleInteger16
            | MapiPropertyType::MultipleInteger32
            | MapiPropertyType::MultipleInteger64
            | MapiPropertyType::MultipleString8
            | MapiPropertyType::MultipleString
            | MapiPropertyType::MultipleGuid
            | MapiPropertyType::MultipleBinary,
        ) => write_u32(row, 0),
        _ => write_u32(row, 0x8004_0102),
    }
}

pub(in crate::mapi) fn mapi_folder_id(mailbox: &JmapMailbox) -> u64 {
    match mailbox.role.as_str() {
        "inbox" => INBOX_FOLDER_ID,
        "drafts" => DRAFTS_FOLDER_ID,
        "outbox" => OUTBOX_FOLDER_ID,
        "sent" => SENT_FOLDER_ID,
        "trash" => TRASH_FOLDER_ID,
        "contacts" => CONTACTS_FOLDER_ID,
        "calendar" => CALENDAR_FOLDER_ID,
        "journal" => JOURNAL_FOLDER_ID,
        "notes" => NOTES_FOLDER_ID,
        "tasks" => TASKS_FOLDER_ID,
        "reminders" => REMINDERS_FOLDER_ID,
        _ => crate::mapi::identity::mapped_mapi_object_id(&mailbox.id)
            .expect("MAPI folder identity mapping missing"),
    }
}

fn mapi_parent_folder_id(mailbox: &JmapMailbox) -> u64 {
    mailbox
        .parent_id
        .and_then(|parent_id| crate::mapi::identity::mapped_mapi_object_id(&parent_id))
        .unwrap_or(IPM_SUBTREE_FOLDER_ID)
}

pub(in crate::mapi) fn mapi_message_id(email: &JmapEmail) -> u64 {
    mapi_item_id(&email.id)
}

pub(in crate::mapi) fn mapi_item_id(id: &Uuid) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(id).expect("MAPI item identity mapping missing")
}
