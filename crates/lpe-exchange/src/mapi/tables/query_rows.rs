use super::*;

pub(in crate::mapi) fn rop_query_rows_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !object.as_deref().is_some_and(is_table_object) {
        return rop_error_response(0x15, request.response_handle_index(), 0x8004_0102);
    }
    if !query_rows_request_is_valid(request) {
        return rop_error_response(0x15, request.response_handle_index(), 0x8007_0057);
    }
    if !object.as_deref().is_some_and(table_columns_are_available) {
        return rop_error_response(0x15, request.response_handle_index(), 0x0000_04B9);
    }

    let response_columns = query_rows_response_columns(object.as_deref(), snapshot);
    let mut total_row_count = object
        .as_deref()
        .map(|object| {
            table_position_and_count(Some(object), mailboxes, emails, snapshot, mailbox_guid).1
        })
        .unwrap_or(0);
    let mut response = vec![0x15, request.response_handle_index()];
    write_u32(&mut response, 0);
    let mut start_position = 0usize;
    let mut position_base = 0usize;
    let rows = match object {
        Some(MapiObject::HierarchyTable {
            folder_id,
            columns,
            sort_orders,
            restriction,
            deleted_advertised_special_folders,
            position: table_position,
            ..
        }) if is_queryable_hierarchy_folder(*folder_id)
            || snapshot.public_folder_for_id(*folder_id).is_some() =>
        {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            let rows = hierarchy_table_rows_excluding_deleted(
                *folder_id,
                mailboxes,
                snapshot,
                restriction.as_ref(),
                sort_orders,
                mailbox_guid,
                deleted_advertised_special_folders,
            );
            log_sync_issues_hierarchy_query_rows(
                request,
                *folder_id,
                &columns,
                restriction.as_ref(),
                sort_orders,
                *table_position,
                &rows,
                mailbox_guid,
            );
            rows.into_iter()
                .map(|row| {
                    serialize_hierarchy_row(row, mailboxes, snapshot, &columns, mailbox_guid)
                })
                .collect::<Vec<_>>()
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            sort_orders,
            category_count,
            expanded_count,
            collapsed_categories,
            restriction,
            position: table_position,
            ..
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                if !*associated
                    && (is_contact_contents_folder(*folder_id)
                        || *folder_id == CONTACTS_SEARCH_FOLDER_ID
                        || snapshot
                            .collaboration_folder_for_id(*folder_id)
                            .is_some_and(|folder| {
                                folder.kind == MapiCollaborationFolderKind::Contacts
                            }))
                {
                    default_contact_property_tags()
                } else if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                    default_navigation_shortcut_property_tags()
                } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    default_conversation_action_property_tags()
                } else if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                    default_message_property_tags()
                } else if *associated
                    && (*folder_id == CALENDAR_FOLDER_ID
                        || snapshot
                            .collaboration_folder_for_id(*folder_id)
                            .is_some_and(|folder| {
                                folder.kind == MapiCollaborationFolderKind::Calendar
                            }))
                {
                    default_calendar_configuration_property_tags()
                } else if *associated
                    && should_use_associated_config_table(
                        *folder_id,
                        snapshot,
                        restriction.as_ref(),
                    )
                {
                    default_associated_config_columns()
                } else {
                    default_contents_columns()
                }
            } else {
                columns.clone()
            };
            if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                snapshot
                    .delegate_freebusy_messages()
                    .iter()
                    .filter(|message| {
                        restriction_matches(restriction.as_ref(), |property_tag| {
                            delegate_freebusy_property_value(message, property_tag)
                        })
                    })
                    .map(|message| serialize_delegate_freebusy_row(message, &columns))
                    .collect::<Vec<_>>()
            } else if *associated {
                if *folder_id == COMMON_VIEWS_FOLDER_ID {
                    let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
                    let total_common_views_rows = rows.len();
                    let navigation_projection =
                        is_unrestricted_common_views_navigation_projection(&columns, restriction);
                    let navigation_shortcut_count = rows
                        .iter()
                        .filter(|message| {
                            matches!(message, MapiCommonViewsMessage::NavigationShortcut(_))
                        })
                        .count();
                    let virtual_navigation_shortcut_count = rows
                        .iter()
                        .filter(|message| {
                            matches!(
                                message,
                                MapiCommonViewsMessage::NavigationShortcut(shortcut)
                                    if crate::mapi_store::is_outlook_common_views_default_navigation_shortcut_id(shortcut.id)
                            )
                        })
                        .count();
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        request_type = "Execute",
                        request_rop_id = "0x15",
                        folder_id = %format!("0x{folder_id:016x}"),
                        folder_role = role_for_folder_id(*folder_id).unwrap_or(""),
                        associated = true,
                        common_views_row_count = total_common_views_rows,
                        common_views_navigation_shortcut_count = navigation_shortcut_count,
                        common_views_persisted_navigation_shortcut_count =
                            navigation_shortcut_count.saturating_sub(virtual_navigation_shortcut_count),
                        common_views_virtual_navigation_shortcut_count =
                            virtual_navigation_shortcut_count,
                        common_views_named_view_count =
                            total_common_views_rows.saturating_sub(navigation_shortcut_count),
                        common_views_navigation_projection = navigation_projection,
                        table_has_restriction = restriction.is_some(),
                        current_position = *table_position,
                        selected_property_tag_count = columns.len(),
                        selected_property_tags = %format_table_property_tags(&columns),
                        "rca debug outlook common views query rows"
                    );
                    rows.retain(|message| {
                        restriction_matches_common_views_message(
                            restriction.as_ref(),
                            message,
                            mailbox_guid,
                        )
                    });
                    if navigation_projection {
                        rows.retain(|message| {
                            matches!(message, MapiCommonViewsMessage::NavigationShortcut(_))
                        });
                    }
                    sort_common_views_messages(&mut rows, sort_orders);
                    total_row_count = rows.len();
                    rows.iter()
                        .map(|message| {
                            serialize_common_views_row_with_mailbox_guid(
                                message,
                                mailbox_guid,
                                &columns,
                            )
                        })
                        .collect::<Vec<_>>()
                } else if *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    snapshot
                        .conversation_action_table_messages()
                        .iter()
                        .filter(|message| {
                            restriction_matches(restriction.as_ref(), |property_tag| {
                                conversation_action_property_value(message, property_tag)
                            })
                        })
                        .map(|message| serialize_conversation_action_row(message, &columns))
                        .collect::<Vec<_>>()
                } else if should_use_associated_config_table(
                    *folder_id,
                    snapshot,
                    restriction.as_ref(),
                ) {
                    let mut rows = associated_table_rows(
                        *folder_id,
                        snapshot,
                        restriction.as_ref(),
                        mailbox_guid,
                    );
                    sort_associated_table_rows(&mut rows, sort_orders, mailbox_guid);
                    rows.iter()
                        .map(|message| {
                            serialize_associated_table_row(message, mailbox_guid, &columns)
                        })
                        .collect::<Vec<_>>()
                } else if *folder_id == CALENDAR_FOLDER_ID
                    || snapshot
                        .collaboration_folder_for_id(*folder_id)
                        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
                {
                    Vec::new()
                } else {
                    Vec::new()
                }
            } else if snapshot.public_folder_for_id(*folder_id).is_some() {
                let mut rows = snapshot.public_folder_items_for_folder(*folder_id);
                retain_rows_by_restriction(&mut rows, restriction.as_ref(), |item, restriction| {
                    restriction_matches_public_folder_item(restriction, item)
                });
                rows.into_iter()
                    .map(|item| serialize_public_folder_item_row(item, &columns))
                    .collect::<Vec<_>>()
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
                sort_emails(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|email| serialize_message_row(email, &columns))
                    .collect::<Vec<_>>()
            } else if *folder_id == CALENDAR_FOLDER_ID {
                let mut rows = calendar_content_rows(snapshot, *folder_id, restriction.as_ref());
                sort_events(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|event| {
                        serialize_event_row(&event.event, event.id, event.folder_id, &columns)
                    })
                    .collect::<Vec<_>>()
            } else if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                match folder.kind {
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
                        let mut rows =
                            calendar_content_rows(snapshot, *folder_id, restriction.as_ref());
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
                    MapiCollaborationFolderKind::Task => {
                        let mut rows = snapshot.tasks_for_folder(*folder_id);
                        rows.retain(|task| {
                            restriction_matches_task(restriction.as_ref(), &task.task)
                        });
                        sort_tasks(&mut rows, sort_orders);
                        rows.into_iter()
                            .map(|task| {
                                serialize_task_row(&task.task, task.id, task.folder_id, &columns)
                            })
                            .collect::<Vec<_>>()
                    }
                }
            } else if is_contact_contents_folder(*folder_id) {
                let mut rows = snapshot.contacts_for_folder(*folder_id);
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        restriction.as_ref(),
                        &contact.contact,
                        *folder_id,
                    )
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
            } else if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                let mut rows = snapshot.contacts_search_results();
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        restriction.as_ref(),
                        &contact.contact,
                        CONTACTS_SEARCH_FOLDER_ID,
                    )
                });
                sort_contacts(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|contact| {
                        serialize_contact_row(
                            &contact.contact,
                            contact.id,
                            CONTACTS_SEARCH_FOLDER_ID,
                            &columns,
                        )
                    })
                    .collect::<Vec<_>>()
            } else if *folder_id == TODO_SEARCH_FOLDER_ID {
                let mut rows = todo_search_content_rows(snapshot, restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|row| serialize_search_content_row(row, snapshot, &columns, false))
                    .collect::<Vec<_>>()
            } else if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID {
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
                rows.into_iter()
                    .map(|message| serialize_message_row(&message.email, &columns))
                    .collect::<Vec<_>>()
            } else if *folder_id == REMINDERS_FOLDER_ID {
                let mut rows = reminder_search_content_rows(snapshot, restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|row| serialize_search_content_row(row, snapshot, &columns, true))
                    .collect::<Vec<_>>()
            } else if *folder_id == NOTES_FOLDER_ID {
                let mut rows = snapshot.notes_for_folder(*folder_id);
                rows.retain(|note| restriction_matches_note(restriction.as_ref(), &note.note));
                sort_notes(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|note| serialize_note_row(&note.note, note.id, note.folder_id, &columns))
                    .collect::<Vec<_>>()
            } else if *folder_id == JOURNAL_FOLDER_ID {
                let mut rows = snapshot.journal_entries_for_folder(*folder_id);
                rows.retain(|entry| {
                    restriction_matches_journal_entry(restriction.as_ref(), &entry.entry)
                });
                sort_journal_entries(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|entry| {
                        serialize_journal_entry_row(
                            &entry.entry,
                            entry.id,
                            entry.folder_id,
                            &columns,
                        )
                    })
                    .collect::<Vec<_>>()
            } else if crate::mapi_store::recoverable_storage_folder(*folder_id).is_some() {
                let mut rows = snapshot.recoverable_items_for_folder(*folder_id);
                sort_recoverable_items(&mut rows, sort_orders);
                rows.into_iter()
                    .map(|item| serialize_recoverable_item_row(item, &columns))
                    .collect::<Vec<_>>()
            } else {
                let window_offset = if request.query_forward_read() {
                    start_position
                } else {
                    start_position.saturating_sub(request.query_row_count().unwrap_or(0))
                };
                let window_emails = if is_top_level_count_restriction(restriction.as_ref()) {
                    None
                } else {
                    snapshot.content_table_window_emails(
                        *folder_id,
                        table_view_signature(sort_orders, restriction.as_ref()),
                        window_offset,
                        request.query_row_count().unwrap_or(0),
                    )
                };
                if let Some((_, window_emails)) = window_emails {
                    position_base = window_offset;
                    start_position = if request.query_forward_read() {
                        0
                    } else {
                        window_emails.len()
                    };
                    if *category_count > 0 {
                        categorized_email_rows(
                            *folder_id,
                            window_emails,
                            &columns,
                            sort_orders,
                            *expanded_count,
                            collapsed_categories,
                        )
                        .into_iter()
                        .map(|row| row.row)
                        .collect::<Vec<_>>()
                    } else {
                        window_emails
                            .into_iter()
                            .map(|email| serialize_message_row(email, &columns))
                            .collect::<Vec<_>>()
                    }
                } else {
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
                    if *category_count > 0 {
                        categorized_email_rows(
                            *folder_id,
                            rows,
                            &columns,
                            sort_orders,
                            *expanded_count,
                            collapsed_categories,
                        )
                        .into_iter()
                        .map(|row| row.row)
                        .collect::<Vec<_>>()
                    } else {
                        rows.into_iter()
                            .map(|email| serialize_message_row(email, &columns))
                            .collect::<Vec<_>>()
                    }
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
            retain_rows_by_restriction(
                &mut rows,
                restriction.as_ref(),
                |attachment, restriction| restriction_matches_attachment(restriction, attachment),
            );
            sort_attachments(&mut rows, sort_orders);
            rows.into_iter()
                .map(|attachment| serialize_attachment_row(attachment, &columns))
                .collect::<Vec<_>>()
        }
        Some(MapiObject::PermissionTable {
            folder_id,
            columns,
            columns_set: _,
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
        Some(MapiObject::RuleTable {
            folder_id,
            columns,
            position: table_position,
            ..
        }) => {
            start_position = *table_position;
            let columns = if columns.is_empty() {
                default_rule_columns()
            } else {
                columns.clone()
            };
            if snapshot.public_folder_for_id(*folder_id).is_some() {
                Vec::new()
            } else {
                snapshot
                    .rules()
                    .iter()
                    .map(|rule| serialize_rule_row(rule, &columns))
                    .collect::<Vec<_>>()
            }
        }
        _ => Vec::new(),
    };
    start_position = start_position.min(total_row_count.saturating_sub(position_base));
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
            | MapiObject::PermissionTable { position, .. }
            | MapiObject::RuleTable { position, .. },
        ) = object
        {
            *position = next_position;
        }
    }
    let response_origin = if forward_read && next_position >= total_row_count {
        0x02
    } else if !forward_read && next_position == 0 {
        0x00
    } else {
        0x01
    };
    response.push(response_origin);
    response.extend_from_slice(&(selected.len() as u16).to_le_bytes());
    for row in selected {
        write_query_rows_property_row(&mut response, &response_columns, &row);
    }
    response
}
