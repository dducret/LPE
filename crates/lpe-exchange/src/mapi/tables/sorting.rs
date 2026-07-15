use super::*;
use std::cmp::Ordering;

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

pub(in crate::mapi) fn sort_common_views_messages(
    rows: &mut [MapiCommonViewsMessage],
    sort_orders: &[MapiSortOrder],
    mailbox_guid: Uuid,
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = compare_optional_mapi_values(
                common_views_message_property_value(left, mailbox_guid, sort_order.property_tag),
                common_views_message_property_value(right, mailbox_guid, sort_order.property_tag),
            );
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        common_views_message_id(left).cmp(&common_views_message_id(right))
    });
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
                PID_TAG_SENDER_NAME_W => {
                    compare_case_insensitive(email_sender_name(left), email_sender_name(right))
                }
                PID_TAG_SENDER_EMAIL_ADDRESS_W => compare_case_insensitive(
                    email_sender_address(left),
                    email_sender_address(right),
                ),
                PID_TAG_DISPLAY_TO_W => {
                    compare_case_insensitive(&display_to(left), &display_to(right))
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.received_at.cmp(&right.received_at)
                }
                PID_TAG_CLIENT_SUBMIT_TIME => {
                    client_submit_sort_key(left).cmp(client_submit_sort_key(right))
                }
                PID_TAG_MESSAGE_FLAGS => message_flags(left).cmp(&message_flags(right)),
                PID_TAG_MESSAGE_SIZE | PID_TAG_MESSAGE_SIZE_EXTENDED => {
                    left.size_octets.cmp(&right.size_octets)
                }
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

pub(in crate::mapi) fn sort_mapi_messages(
    rows: &mut [&crate::mapi_store::MapiMessage],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.email.subject, &right.email.subject)
                }
                PID_TAG_SENDER_NAME_W => compare_case_insensitive(
                    email_sender_name(&left.email),
                    email_sender_name(&right.email),
                ),
                PID_TAG_SENDER_EMAIL_ADDRESS_W => compare_case_insensitive(
                    email_sender_address(&left.email),
                    email_sender_address(&right.email),
                ),
                PID_TAG_DISPLAY_TO_W => {
                    compare_case_insensitive(&display_to(&left.email), &display_to(&right.email))
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.email.received_at.cmp(&right.email.received_at)
                }
                PID_TAG_CLIENT_SUBMIT_TIME => {
                    client_submit_sort_key(&left.email).cmp(client_submit_sort_key(&right.email))
                }
                PID_TAG_MESSAGE_FLAGS => {
                    message_flags(&left.email).cmp(&message_flags(&right.email))
                }
                PID_TAG_MESSAGE_SIZE | PID_TAG_MESSAGE_SIZE_EXTENDED => {
                    left.email.size_octets.cmp(&right.email.size_octets)
                }
                PID_TAG_HAS_ATTACHMENTS => {
                    left.email.has_attachments.cmp(&right.email.has_attachments)
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

fn client_submit_sort_key(email: &JmapEmail) -> &str {
    email.sent_at.as_deref().unwrap_or(&email.received_at)
}

pub(super) fn sort_associated_table_rows(
    rows: &mut [AssociatedTableRow],
    sort_orders: &[MapiSortOrder],
    mailbox_guid: Uuid,
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let left_value =
                associated_table_row_property_value(left, mailbox_guid, sort_order.property_tag);
            let right_value =
                associated_table_row_property_value(right, mailbox_guid, sort_order.property_tag);
            let ordering = compare_optional_mapi_values(left_value, right_value);
            let ordering = apply_sort_direction(ordering, sort_order.order);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        associated_table_row_id(left).cmp(&associated_table_row_id(right))
    });
}

pub(in crate::mapi) fn sort_recoverable_items(
    rows: &mut [&crate::mapi_store::MapiRecoverableItemMessage],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    compare_case_insensitive(&left.item.subject, &right.item.subject)
                }
                PID_TAG_SENDER_NAME_W | PID_TAG_SENDER_EMAIL_ADDRESS_W => {
                    compare_case_insensitive(&left.item.sender_address, &right.item.sender_address)
                }
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    left.item.received_at.cmp(&right.item.received_at)
                }
                PID_TAG_MESSAGE_SIZE | PID_TAG_MESSAGE_SIZE_EXTENDED => {
                    left.item.size_octets.cmp(&right.item.size_octets)
                }
                PID_TAG_HAS_ATTACHMENTS => {
                    left.item.has_attachments.cmp(&right.item.has_attachments)
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
                PID_LID_LOCATION_W_TAG => {
                    compare_case_insensitive(&left.event.location, &right.event.location)
                }
                PID_TAG_START_DATE
                | PID_LID_COMMON_START_TAG
                | PID_LID_APPOINTMENT_START_WHOLE_TAG
                | PID_TAG_MESSAGE_DELIVERY_TIME => {
                    (left.event.date.as_str(), left.event.time.as_str())
                        .cmp(&(right.event.date.as_str(), right.event.time.as_str()))
                }
                PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => {
                    left.version.updated_at.cmp(&right.version.updated_at)
                }
                PID_TAG_END_DATE | PID_LID_COMMON_END_TAG | PID_LID_APPOINTMENT_END_WHOLE_TAG => {
                    event_end_filetime(&left.event).cmp(&event_end_filetime(&right.event))
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

pub(in crate::mapi) fn sort_tasks(
    rows: &mut [&crate::mapi_store::MapiTask],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
                    compare_case_insensitive(&left.task.title, &right.task.title)
                }
                PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => {
                    left.task.updated_at.cmp(&right.task.updated_at)
                }
                PID_LID_TASK_DUE_DATE_TAG => left.task.due_at.cmp(&right.task.due_at),
                PID_LID_TASK_START_DATE_TAG => left.task.updated_at.cmp(&right.task.updated_at),
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

pub(in crate::mapi) fn sort_notes(
    rows: &mut [&crate::mapi_store::MapiNote],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
                    compare_case_insensitive(&left.note.title, &right.note.title)
                }
                PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => {
                    left.note.updated_at.cmp(&right.note.updated_at)
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

pub(in crate::mapi) fn sort_journal_entries(
    rows: &mut [&crate::mapi_store::MapiJournalEntry],
    sort_orders: &[MapiSortOrder],
) {
    if sort_orders.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for sort_order in sort_orders {
            let ordering = match sort_order.property_tag {
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_DISPLAY_NAME_W => {
                    compare_case_insensitive(&left.entry.subject, &right.entry.subject)
                }
                PID_TAG_START_DATE | PID_LID_LOG_START_TAG | PID_TAG_MESSAGE_DELIVERY_TIME => {
                    journal_entry_start_sort_key(&left.entry)
                        .cmp(&journal_entry_start_sort_key(&right.entry))
                }
                PID_TAG_LAST_MODIFICATION_TIME | PID_TAG_LOCAL_COMMIT_TIME => {
                    left.entry.updated_at.cmp(&right.entry.updated_at)
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

fn compare_optional_mapi_values(left: Option<MapiValue>, right: Option<MapiValue>) -> Ordering {
    match (left, right) {
        (Some(MapiValue::String(left)), Some(MapiValue::String(right))) => {
            compare_case_insensitive(&left, &right)
        }
        (Some(MapiValue::U64(left)), Some(MapiValue::U64(right))) => left.cmp(&right),
        (Some(MapiValue::I64(left)), Some(MapiValue::I64(right))) => left.cmp(&right),
        (Some(MapiValue::U32(left)), Some(MapiValue::U32(right))) => left.cmp(&right),
        (Some(MapiValue::I32(left)), Some(MapiValue::I32(right))) => left.cmp(&right),
        (Some(MapiValue::Bool(left)), Some(MapiValue::Bool(right))) => left.cmp(&right),
        (Some(MapiValue::Guid(left)), Some(MapiValue::Guid(right))) => left.cmp(&right),
        (Some(MapiValue::Binary(left)), Some(MapiValue::Binary(right))) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

pub(super) fn common_views_message_id(message: &MapiCommonViewsMessage) -> u64 {
    match message {
        MapiCommonViewsMessage::NavigationShortcut(message) => message.id,
        MapiCommonViewsMessage::NamedView(message) => message.id,
        MapiCommonViewsMessage::SearchFolderDefinition(message) => {
            crate::mapi::identity::mapped_mapi_object_id(&message.id).unwrap_or_default()
        }
    }
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
            MapiRestriction::InvalidTableRestriction => {
                push_bytes(hash, b"invalid");
            }
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
            MapiRestriction::Count { count, child } => {
                push_bytes(hash, b"count");
                push_bytes(hash, &count.to_le_bytes());
                push_restriction(hash, child);
            }
            MapiRestriction::SubObject { subobject, child } => {
                push_bytes(hash, b"subobject");
                push_bytes(hash, &subobject.to_le_bytes());
                push_restriction(hash, child);
            }
            MapiRestriction::Content {
                property_tag,
                value,
                fuzzy_level_low,
                fuzzy_level_high,
            } => {
                push_bytes(hash, b"content");
                push_bytes(hash, &property_tag.to_le_bytes());
                push_bytes(hash, &fuzzy_level_low.to_le_bytes());
                push_bytes(hash, &fuzzy_level_high.to_le_bytes());
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
            MapiRestriction::CompareProperties {
                relop,
                left_property_tag,
                right_property_tag,
            } => {
                push_bytes(hash, b"compare_properties");
                push_bytes(hash, &[*relop]);
                push_bytes(hash, &left_property_tag.to_le_bytes());
                push_bytes(hash, &right_property_tag.to_le_bytes());
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
