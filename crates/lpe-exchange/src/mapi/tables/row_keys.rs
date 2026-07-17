use super::*;

pub(in crate::mapi) fn table_row_keys(
    object: &MapiObject,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u64> {
    match object {
        MapiObject::HierarchyTable {
            folder_id,
            sort_orders,
            restriction,
            deleted_advertised_special_folders,
            ..
        } if is_queryable_hierarchy_folder(*folder_id) => hierarchy_table_rows_excluding_deleted(
            *folder_id,
            mailboxes,
            snapshot,
            restriction.as_ref(),
            sort_orders,
            mailbox_guid,
            deleted_advertised_special_folders,
        )
        .into_iter()
        .map(|row| hierarchy_row_id(&row))
        .collect(),
        MapiObject::ContentsTable {
            folder_id,
            associated,
            sort_orders,
            restriction,
            position,
            ..
        } => {
            if !*associated && *folder_id == TRASH_FOLDER_ID {
                let mut rows =
                    deleted_items_content_rows(mailboxes, emails, snapshot, restriction.as_ref());
                sort_deleted_items_content_rows(&mut rows, sort_orders);
                return rows
                    .into_iter()
                    .map(|row| deleted_items_content_row_id(&row))
                    .collect();
            }
            if *folder_id == CALENDAR_FOLDER_ID {
                let mut rows = calendar_content_rows(snapshot, *folder_id, restriction.as_ref());
                sort_events(&mut rows, sort_orders);
                return rows.into_iter().map(|event| event.id).collect();
            }
            if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                return match folder.kind {
                    MapiCollaborationFolderKind::Contacts => {
                        let mut rows = snapshot.contacts_for_folder(*folder_id);
                        rows.retain(|contact| {
                            restriction_matches_contact_in_folder(
                                restriction.as_ref(),
                                &contact.contact,
                                *folder_id,
                            )
                        });
                        sort_contacts(&mut rows, sort_orders);
                        rows.into_iter().map(|contact| contact.id).collect()
                    }
                    MapiCollaborationFolderKind::Calendar => {
                        let mut rows =
                            calendar_content_rows(snapshot, *folder_id, restriction.as_ref());
                        sort_events(&mut rows, sort_orders);
                        rows.into_iter().map(|event| event.id).collect()
                    }
                    MapiCollaborationFolderKind::Task => {
                        let mut rows = snapshot.tasks_for_folder(*folder_id);
                        rows.retain(|task| {
                            restriction_matches_task(restriction.as_ref(), &task.task)
                        });
                        sort_tasks(&mut rows, sort_orders);
                        rows.into_iter().map(|task| task.id).collect()
                    }
                };
            }
            if is_contact_contents_folder(*folder_id) {
                let mut rows = snapshot.contacts_for_folder(*folder_id);
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        restriction.as_ref(),
                        &contact.contact,
                        *folder_id,
                    )
                });
                sort_contacts(&mut rows, sort_orders);
                return rows.into_iter().map(|contact| contact.id).collect();
            }
            if *folder_id == NOTES_FOLDER_ID {
                let mut rows = snapshot.notes_for_folder(*folder_id);
                rows.retain(|note| restriction_matches_note(restriction.as_ref(), &note.note));
                sort_notes(&mut rows, sort_orders);
                return rows.into_iter().map(|note| note.id).collect();
            }
            if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                let mut rows = snapshot.contacts_search_results();
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        restriction.as_ref(),
                        &contact.contact,
                        CONTACTS_SEARCH_FOLDER_ID,
                    )
                });
                sort_contacts(&mut rows, sort_orders);
                return rows.into_iter().map(|contact| contact.id).collect();
            }
            if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                let mut rows = snapshot
                    .delegate_freebusy_messages()
                    .iter()
                    .collect::<Vec<_>>();
                rows.retain(|message| {
                    restriction_matches(restriction.as_ref(), |property_tag| {
                        delegate_freebusy_property_value(message, property_tag)
                    })
                });
                return rows.into_iter().map(|message| message.id).collect();
            }
            if *folder_id == TODO_SEARCH_FOLDER_ID {
                let mut rows = todo_search_content_rows(snapshot, restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                return rows
                    .into_iter()
                    .map(|row| search_content_row_id(&row))
                    .collect();
            }
            if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID {
                let mut rows = snapshot.tracked_mail_processing_messages();
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
                sort_mapi_messages(&mut rows, sort_orders);
                return rows.into_iter().map(|message| message.id).collect();
            }
            if *folder_id == REMINDERS_FOLDER_ID {
                let mut rows = reminder_search_content_rows(snapshot, restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                return rows
                    .into_iter()
                    .map(|row| search_content_row_id(&row))
                    .collect();
            }
            if *folder_id == JOURNAL_FOLDER_ID {
                let mut rows = snapshot.journal_entries_for_folder(*folder_id);
                rows.retain(|entry| {
                    restriction_matches_journal_entry(restriction.as_ref(), &entry.entry)
                });
                sort_journal_entries(&mut rows, sort_orders);
                return rows.into_iter().map(|entry| entry.id).collect();
            }
            if crate::mapi_store::recoverable_storage_folder(*folder_id).is_some() {
                let mut rows = snapshot.recoverable_items_for_folder(*folder_id);
                sort_recoverable_items(&mut rows, sort_orders);
                return rows.into_iter().map(|item| item.id).collect();
            }
            if normal_contents_suppressed_for_associated_only_folder(*folder_id) {
                return Vec::new();
            }
            let window_emails = if is_top_level_count_restriction(restriction.as_ref()) {
                None
            } else {
                snapshot.content_table_window_emails_containing(
                    *folder_id,
                    table_view_signature(sort_orders, restriction.as_ref()),
                    *position,
                )
            };
            if let Some((offset, total, window_emails)) = window_emails {
                let mut row_keys = vec![0; total];
                for (index, email) in window_emails.into_iter().enumerate() {
                    let row_index = offset.saturating_add(index);
                    if row_index < row_keys.len() {
                        row_keys[row_index] = mapi_message_id(email);
                    }
                }
                return row_keys;
            }
            let total = if is_top_level_count_restriction(restriction.as_ref()) {
                None
            } else {
                snapshot.content_table_total(
                    *folder_id,
                    table_view_signature(sort_orders, restriction.as_ref()),
                )
            };
            if let Some(total) = total {
                return vec![0; total];
            }
            let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
            retain_rows_by_restriction(&mut rows, restriction.as_ref(), |email, restriction| {
                restriction_matches_email_in_snapshot(restriction, email, *folder_id, snapshot)
            });
            sort_emails(&mut rows, sort_orders);
            rows.into_iter().map(mapi_message_id).collect()
        }
        MapiObject::AttachmentTable {
            folder_id,
            message_id,
            materialized_attachments,
            sort_orders,
            restriction,
            ..
        } => {
            let mut rows = materialized_attachments
                .as_deref()
                .or_else(|| snapshot.attachments_for_message(*folder_id, *message_id))
                .unwrap_or_default()
                .iter()
                .collect::<Vec<_>>();
            retain_rows_by_restriction(
                &mut rows,
                restriction.as_ref(),
                |attachment, restriction| restriction_matches_attachment(restriction, attachment),
            );
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
        MapiObject::RuleTable { folder_id, .. } => {
            if snapshot.public_folder_for_id(*folder_id).is_some() {
                Vec::new()
            } else {
                snapshot.rules().iter().map(|rule| rule.id).collect()
            }
        }
        _ => Vec::new(),
    }
}
