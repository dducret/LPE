use super::*;

pub(in crate::mapi) fn hierarchy_row_count_excluding_deleted(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    deleted_advertised_special_folders: &HashSet<u64>,
) -> u32 {
    if is_queryable_hierarchy_folder(folder_id)
        || snapshot.public_folder_for_id(folder_id).is_some()
    {
        hierarchy_rows_excluding_deleted(
            folder_id,
            mailboxes,
            snapshot,
            None,
            &[],
            Uuid::nil(),
            deleted_advertised_special_folders,
        )
        .len()
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
    if folder_id == CONVERSATION_MEMBERS_CONTENTS_TABLE_ID {
        return emails.len().min(u32::MAX as usize) as u32;
    }
    if let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) {
        return match folder.kind {
            MapiCollaborationFolderKind::Contacts => snapshot
                .contacts_for_folder(folder_id)
                .len()
                .min(u32::MAX as usize) as u32,
            MapiCollaborationFolderKind::Calendar => snapshot
                .events_for_folder(folder_id)
                .len()
                .min(u32::MAX as usize) as u32,
            MapiCollaborationFolderKind::Task => snapshot
                .tasks_for_folder(folder_id)
                .len()
                .min(u32::MAX as usize) as u32,
        };
    }
    if folder_id == CALENDAR_FOLDER_ID {
        return snapshot
            .events_for_folder(folder_id)
            .len()
            .min(u32::MAX as usize) as u32;
    }
    if folder_id == CONTACTS_SEARCH_FOLDER_ID {
        return snapshot
            .contacts_search_results()
            .len()
            .min(u32::MAX as usize) as u32;
    }
    if folder_id == FREEBUSY_DATA_FOLDER_ID {
        return snapshot
            .delegate_freebusy_messages()
            .len()
            .min(u32::MAX as usize) as u32;
    }
    if is_contact_contents_folder(folder_id) {
        return snapshot
            .contacts_for_folder(folder_id)
            .len()
            .min(u32::MAX as usize) as u32;
    }
    if let Some(folder) = snapshot.public_folder_for_id(folder_id) {
        return folder.item_count;
    }
    if folder_id == REMINDERS_FOLDER_ID {
        let count = snapshot.reminder_tasks().len() + snapshot.reminder_messages().len();
        return count.min(u32::MAX as usize) as u32;
    }
    if crate::mapi_store::recoverable_storage_folder(folder_id).is_some() {
        return snapshot
            .recoverable_items_for_folder(folder_id)
            .len()
            .min(u32::MAX as usize) as u32;
    }
    folder_row_for_id(folder_id, mailboxes)
        .map(|mailbox| mailbox.total_emails)
        .unwrap_or_else(|| emails_for_folder(folder_id, mailboxes, emails).len() as u32)
}

pub(super) fn is_contact_contents_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CONTACTS_FOLDER_ID | SUGGESTED_CONTACTS_FOLDER_ID | QUICK_CONTACTS_FOLDER_ID
    )
}

pub(in crate::mapi) fn associated_folder_message_count(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    if folder_id == COMMON_VIEWS_FOLDER_ID {
        snapshot
            .common_views_table_messages()
            .count()
            .min(u32::MAX as usize) as u32
    } else if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
        snapshot
            .conversation_action_table_messages()
            .len()
            .min(u32::MAX as usize) as u32
    } else if folder_id == FREEBUSY_DATA_FOLDER_ID {
        snapshot
            .delegate_freebusy_messages()
            .len()
            .min(u32::MAX as usize) as u32
    } else if has_associated_table_rows(folder_id, snapshot) {
        associated_table_rows(folder_id, snapshot, None, Uuid::nil())
            .len()
            .min(u32::MAX as usize) as u32
    } else if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
    {
        0
    } else {
        0
    }
}

pub(in crate::mapi) fn contents_table_open_row_count(
    folder_id: u64,
    associated: bool,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    if associated {
        associated_folder_message_count(folder_id, snapshot)
    } else {
        folder_message_count(folder_id, mailboxes, emails, snapshot)
    }
}

pub(in crate::mapi) fn restricted_associated_folder_message_count(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    restriction: Option<&MapiRestriction>,
    mailbox_guid: Uuid,
) -> usize {
    if folder_id == COMMON_VIEWS_FOLDER_ID {
        snapshot
            .common_views_table_messages()
            .filter(|message| {
                restriction_matches_common_views_message(restriction, message, mailbox_guid)
            })
            .count()
    } else if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
        snapshot
            .conversation_action_table_messages()
            .iter()
            .filter(|message| {
                restriction_matches(restriction, |property_tag| {
                    conversation_action_property_value(message, property_tag)
                })
            })
            .count()
    } else if folder_id == FREEBUSY_DATA_FOLDER_ID {
        snapshot
            .delegate_freebusy_messages()
            .iter()
            .filter(|message| {
                restriction_matches(restriction, |property_tag| {
                    delegate_freebusy_property_value(message, property_tag)
                })
            })
            .count()
    } else {
        associated_table_rows(folder_id, snapshot, restriction, mailbox_guid).len()
    }
}

pub(in crate::mapi) fn table_position_and_count(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> (usize, usize) {
    let (position, total) = match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            position,
            restriction,
            sort_orders,
            deleted_advertised_special_folders,
            ..
        }) if is_queryable_hierarchy_folder(*folder_id) => {
            let total = hierarchy_table_rows_excluding_deleted(
                *folder_id,
                mailboxes,
                snapshot,
                restriction.as_ref(),
                sort_orders,
                mailbox_guid,
                deleted_advertised_special_folders,
            )
            .len();
            (*position, total)
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            restriction,
            sort_orders,
            category_count,
            expanded_count,
            collapsed_categories,
            ..
        }) => {
            let total = if *associated
                && *folder_id == COMMON_VIEWS_FOLDER_ID
                && is_unrestricted_common_views_navigation_projection(columns, restriction)
            {
                snapshot
                    .common_views_table_messages()
                    .filter(|message| {
                        matches!(message, MapiCommonViewsMessage::NavigationShortcut(_))
                    })
                    .count()
            } else if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                restricted_associated_folder_message_count(
                    *folder_id,
                    snapshot,
                    restriction.as_ref(),
                    mailbox_guid,
                )
            } else if *associated {
                restricted_associated_folder_message_count(
                    *folder_id,
                    snapshot,
                    restriction.as_ref(),
                    mailbox_guid,
                )
            } else if *folder_id == CALENDAR_FOLDER_ID {
                calendar_content_rows(snapshot, *folder_id, restriction.as_ref()).len()
            } else if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                match folder.kind {
                    MapiCollaborationFolderKind::Contacts => snapshot
                        .contacts_for_folder(*folder_id)
                        .into_iter()
                        .filter(|contact| {
                            restriction_matches_contact_in_folder(
                                restriction.as_ref(),
                                &contact.contact,
                                *folder_id,
                            )
                        })
                        .count(),
                    MapiCollaborationFolderKind::Calendar => {
                        calendar_content_rows(snapshot, *folder_id, restriction.as_ref()).len()
                    }
                    MapiCollaborationFolderKind::Task => snapshot
                        .tasks_for_folder(*folder_id)
                        .into_iter()
                        .filter(|task| restriction_matches_task(restriction.as_ref(), &task.task))
                        .count(),
                }
            } else if is_contact_contents_folder(*folder_id) {
                snapshot
                    .contacts_for_folder(*folder_id)
                    .into_iter()
                    .filter(|contact| {
                        restriction_matches_contact_in_folder(
                            restriction.as_ref(),
                            &contact.contact,
                            *folder_id,
                        )
                    })
                    .count()
            } else if *folder_id == NOTES_FOLDER_ID {
                snapshot
                    .notes_for_folder(*folder_id)
                    .into_iter()
                    .filter(|note| restriction_matches_note(restriction.as_ref(), &note.note))
                    .count()
            } else if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                snapshot
                    .contacts_search_results()
                    .into_iter()
                    .filter(|contact| {
                        restriction_matches_contact_in_folder(
                            restriction.as_ref(),
                            &contact.contact,
                            CONTACTS_SEARCH_FOLDER_ID,
                        )
                    })
                    .count()
            } else if *folder_id == TODO_SEARCH_FOLDER_ID {
                let mut message_rows = snapshot
                    .todo_search_messages()
                    .into_iter()
                    .collect::<Vec<_>>();
                retain_rows_by_restriction(
                    &mut message_rows,
                    restriction.as_ref(),
                    |message, restriction| restriction_matches_email(restriction, &message.email),
                );
                message_rows.len()
                    + snapshot
                        .todo_search_results()
                        .into_iter()
                        .filter(|task| restriction_matches_task(restriction.as_ref(), &task.task))
                        .count()
            } else if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID {
                let mut rows = snapshot
                    .tracked_mail_processing_messages()
                    .into_iter()
                    .collect::<Vec<_>>();
                retain_rows_by_restriction(
                    &mut rows,
                    restriction.as_ref(),
                    |message, restriction| {
                        restriction_matches_email_in_snapshot(
                            restriction,
                            &message.email,
                            *folder_id,
                            snapshot,
                        )
                    },
                );
                rows.len()
            } else if *folder_id == REMINDERS_FOLDER_ID {
                snapshot
                    .reminder_tasks()
                    .into_iter()
                    .filter(|task| restriction_matches_task(restriction.as_ref(), &task.task))
                    .count()
                    + {
                        let mut message_rows =
                            snapshot.reminder_messages().into_iter().collect::<Vec<_>>();
                        retain_rows_by_restriction(
                            &mut message_rows,
                            restriction.as_ref(),
                            |message, restriction| {
                                restriction_matches_email(restriction, &message.email)
                            },
                        );
                        message_rows.len()
                    }
            } else if *folder_id == JOURNAL_FOLDER_ID {
                snapshot
                    .journal_entries_for_folder(*folder_id)
                    .into_iter()
                    .filter(|entry| {
                        restriction_matches_journal_entry(restriction.as_ref(), &entry.entry)
                    })
                    .count()
            } else if snapshot.public_folder_for_id(*folder_id).is_some() {
                let mut rows = snapshot.public_folder_items_for_folder(*folder_id);
                retain_rows_by_restriction(&mut rows, restriction.as_ref(), |item, restriction| {
                    restriction_matches_public_folder_item(restriction, item)
                });
                rows.len()
            } else if crate::mapi_store::recoverable_storage_folder(*folder_id).is_some() {
                snapshot.recoverable_items_for_folder(*folder_id).len()
            } else if *folder_id == CONVERSATION_MEMBERS_CONTENTS_TABLE_ID {
                let mut rows = emails.iter().collect::<Vec<_>>();
                retain_rows_by_restriction(
                    &mut rows,
                    restriction.as_ref(),
                    |email, restriction| {
                        restriction_matches_conversation_member_in_snapshot(
                            restriction,
                            email,
                            snapshot,
                        )
                    },
                );
                rows.len()
            } else if *category_count > 0 {
                let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                retain_rows_by_restriction(
                    &mut rows,
                    restriction.as_ref(),
                    |email, restriction| {
                        restriction_matches_email_in_snapshot(
                            restriction,
                            email,
                            *folder_id,
                            snapshot,
                        )
                    },
                );
                sort_emails(&mut rows, sort_orders);
                categorized_email_rows(
                    *folder_id,
                    rows,
                    &default_contents_columns(),
                    sort_orders,
                    *expanded_count,
                    collapsed_categories,
                )
                .len()
            } else {
                if is_top_level_count_restriction(restriction.as_ref()) {
                    None
                } else {
                    snapshot.content_table_total(
                        *folder_id,
                        table_view_signature(sort_orders, restriction.as_ref()),
                    )
                }
                .unwrap_or_else(|| {
                    let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                    retain_rows_by_restriction(
                        &mut rows,
                        restriction.as_ref(),
                        |email, restriction| {
                            restriction_matches_email_in_snapshot(
                                restriction,
                                email,
                                *folder_id,
                                snapshot,
                            )
                        },
                    );
                    rows.len()
                })
            };
            (*position, total)
        }
        Some(MapiObject::AttachmentTable {
            folder_id,
            message_id,
            position,
            restriction,
            ..
        }) => {
            let mut rows = snapshot
                .attachments_for_message(*folder_id, *message_id)
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            retain_rows_by_restriction(
                &mut rows,
                restriction.as_ref(),
                |attachment, restriction| restriction_matches_attachment(restriction, attachment),
            );
            (*position, rows.len())
        }
        Some(MapiObject::PermissionTable {
            folder_id,
            position,
            ..
        }) => (*position, snapshot.permissions_for_folder(*folder_id).len()),
        Some(MapiObject::RuleTable {
            folder_id,
            position,
            ..
        }) => (
            *position,
            if snapshot.public_folder_for_id(*folder_id).is_some() {
                0
            } else {
                snapshot.rules().len()
            },
        ),
        _ => (0, 0),
    };
    (position.min(total), total)
}
