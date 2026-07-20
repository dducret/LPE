use super::permissions::*;
use super::properties::*;
use super::rop::*;
use super::session::*;
use super::sync::*;
use super::wire::MapiPropertyType;
use super::*;
use crate::mapi::identity::{
    CONVERSATION_MEMBERS_CONTENTS_TABLE_ID, QUICK_STEP_SETTINGS_FOLDER_ID,
    RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID, RECOVERABLE_ITEMS_PURGES_FOLDER_ID,
    RECOVERABLE_ITEMS_ROOT_FOLDER_ID, RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID,
};
use crate::mapi_store::{
    MapiAssociatedConfigMessage, MapiCommonViewNamedViewMessage, MapiCommonViewsMessage,
    MapiConversationActionMessage, MapiDelegateFreeBusyMessage, MapiMessage,
    MapiNavigationShortcutMessage, MapiTask,
};
use lpe_storage::SearchFolderDefinition;

mod associated_contents;
mod attachments;
mod calendar;
mod collaboration_items;
mod collapse;
mod columns;
mod contents;
mod controls;
mod counts;
mod deleted_items;
mod diagnostics;
mod filters;
mod find;
mod flags;
mod folders;
mod hierarchy;
mod pending;
mod public_folders;
mod query;
mod query_rows;
mod recipients;
mod recoverable_items;
mod row_codecs;
mod row_keys;
mod rules;
mod search_folders;
mod sorting;
mod state;
mod time;

pub(in crate::mapi) use associated_contents::*;
pub(super) use attachments::*;
pub(in crate::mapi) use calendar::*;
pub(in crate::mapi) use collaboration_items::*;
pub(in crate::mapi) use collapse::*;
pub(super) use columns::*;
pub(in crate::mapi) use contents::*;
pub(in crate::mapi) use controls::*;
pub(in crate::mapi) use counts::*;
use deleted_items::*;
pub(in crate::mapi) use diagnostics::outlook_bootstrap_row_invariant_summaries;
use diagnostics::*;
pub(in crate::mapi) use filters::is_unrestricted_common_views_navigation_projection;
use filters::*;
use find::*;
pub(in crate::mapi) use flags::*;
pub(in crate::mapi) use folders::*;
use hierarchy::*;
pub(in crate::mapi) use hierarchy::{
    mailbox_shadowed_by_active_outlook_special_folder, special_folder_property_value,
    special_folder_property_value_with_change_number,
};
pub(in crate::mapi) use pending::*;
pub(in crate::mapi) use public_folders::*;
use query::*;
pub(in crate::mapi) use query_rows::*;
pub(in crate::mapi) use recipients::*;
pub(in crate::mapi) use recoverable_items::*;
pub(super) use row_codecs::*;
pub(in crate::mapi) use row_keys::*;
pub(super) use rules::*;
use search_folders::*;
pub(in crate::mapi) use sorting::*;
pub(in crate::mapi) use state::*;
pub(in crate::mapi) use time::*;

const TABLE_LEAF_ROW: u32 = 0x0000_0001;

pub(in crate::mapi) fn rop_find_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !find_row_request_is_valid(request) {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8007_0057);
    }
    let Ok(restriction) = request.restriction() else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    let Some(restriction) = restriction else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };

    let Some(object) = object else {
        return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102);
    };
    if !table_columns_are_available(object) {
        return rop_error_response(0x4F, request.response_handle_index(), 0x0000_04B9);
    }
    let mut response = vec![0x4F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);

    match object {
        MapiObject::HierarchyTable {
            folder_id,
            columns,
            sort_orders,
            restriction: table_restriction,
            deleted_advertised_special_folders,
            position,
            ..
        } if is_queryable_hierarchy_folder(*folder_id) => {
            let columns = if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            };
            let rows = hierarchy_table_rows_excluding_deleted(
                *folder_id,
                mailboxes,
                snapshot,
                table_restriction.as_ref(),
                sort_orders,
                mailbox_guid,
                deleted_advertised_special_folders,
            );
            if let Some((index, row)) = find_hierarchy_row(
                rows.as_slice(),
                mailboxes,
                *position,
                request,
                Some(&restriction),
                mailbox_guid,
            ) {
                *position = index;
                response.push(1);
                write_standard_property_row(
                    &mut response,
                    &serialize_hierarchy_row(row, mailboxes, snapshot, &columns, mailbox_guid),
                );
            } else {
                return rop_find_row_no_match_response(request);
            }
        }
        MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            sort_orders,
            restriction: table_restriction,
            position,
            ..
        } => {
            let columns = if columns.is_empty() {
                if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                    default_navigation_shortcut_property_tags()
                } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    default_conversation_action_property_tags()
                } else if *associated
                    && should_use_associated_config_table(
                        *folder_id,
                        snapshot,
                        table_restriction.as_ref(),
                    )
                {
                    default_associated_config_columns()
                } else {
                    default_contents_columns()
                }
            } else {
                columns.clone()
            };
            if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
                sort_common_views_messages(&mut rows, sort_orders, mailbox_guid);
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
                    request_rop_id = "0x4f",
                    folder_id = %format!("0x{folder_id:016x}"),
                    folder_role = role_for_folder_id(*folder_id).unwrap_or(""),
                    associated = true,
                    common_views_row_count = rows.len(),
                    common_views_navigation_shortcut_count = navigation_shortcut_count,
                    common_views_persisted_navigation_shortcut_count =
                        navigation_shortcut_count.saturating_sub(virtual_navigation_shortcut_count),
                    common_views_virtual_navigation_shortcut_count =
                        virtual_navigation_shortcut_count,
                    common_views_named_view_count = rows.len().saturating_sub(navigation_shortcut_count),
                    current_position = *position,
                    selected_property_tag_count = columns.len(),
                    selected_property_tags = %format_table_property_tags(&columns),
                    "rca debug outlook common views find row"
                );
                let rows = rows.iter().collect::<Vec<_>>();
                if let Some((index, message)) = find_row(
                    rows.as_slice(),
                    *position,
                    request,
                    |message| match message {
                        MapiCommonViewsMessage::NavigationShortcut(shortcut) => {
                            restriction_matches_navigation_shortcut(
                                Some(&restriction),
                                shortcut,
                                mailbox_guid,
                            )
                        }
                        MapiCommonViewsMessage::NamedView(view) => {
                            restriction_matches_common_view_named_view(
                                Some(&restriction),
                                view,
                                mailbox_guid,
                            )
                        }
                        MapiCommonViewsMessage::SearchFolderDefinition(definition) => {
                            restriction_matches(Some(&restriction), |property_tag| {
                                search_folder_definition_message_property_value(
                                    definition,
                                    mailbox_guid,
                                    property_tag,
                                )
                            })
                        }
                        MapiCommonViewsMessage::AssociatedConfig(message) => {
                            restriction_matches_associated_config(Some(&restriction), message)
                        }
                    },
                ) {
                    *position = index;
                    response.push(1);
                    response.extend_from_slice(
                        &serialize_common_views_property_row_with_mailbox_guid(
                            message,
                            mailbox_guid,
                            &columns,
                        ),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                let rows = snapshot.conversation_action_table_messages();
                let rows = rows.iter().collect::<Vec<_>>();
                if let Some((index, message)) =
                    find_row(rows.as_slice(), *position, request, |message| {
                        restriction_matches(Some(&restriction), |property_tag| {
                            conversation_action_property_value(message, property_tag)
                        })
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_conversation_action_row(message, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == FREEBUSY_DATA_FOLDER_ID {
                let rows = snapshot
                    .delegate_freebusy_messages()
                    .iter()
                    .collect::<Vec<_>>();
                if let Some((index, message)) =
                    find_row(rows.as_slice(), *position, request, |message| {
                        restriction_matches(Some(&restriction), |property_tag| {
                            delegate_freebusy_property_value(message, property_tag)
                        })
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_delegate_freebusy_row(message, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *associated
                && should_use_associated_config_table(*folder_id, snapshot, Some(&restriction))
            {
                let mut rows = associated_table_rows(
                    *folder_id,
                    snapshot,
                    table_restriction.as_ref(),
                    mailbox_guid,
                );
                sort_associated_table_rows(&mut rows, sort_orders, mailbox_guid);
                let row_refs = rows.iter().collect::<Vec<_>>();
                if let Some((index, message)) =
                    find_row(row_refs.as_slice(), *position, request, |message| {
                        associated_table_row_matches(message, Some(&restriction), mailbox_guid)
                    })
                {
                    *position = index;
                    response.push(1);
                    response.extend_from_slice(&serialize_associated_table_property_row(
                        message,
                        mailbox_guid,
                        &columns,
                    ));
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if normal_contents_suppressed_for_associated_only_folder(*folder_id) {
                return rop_find_row_no_match_response(request);
            } else if !*associated && *folder_id == TRASH_FOLDER_ID {
                let mut rows = deleted_items_content_rows(
                    mailboxes,
                    emails,
                    snapshot,
                    table_restriction.as_ref(),
                );
                sort_deleted_items_content_rows(&mut rows, sort_orders);
                let row_refs = rows.iter().collect::<Vec<_>>();
                if let Some((index, row)) =
                    find_row(row_refs.as_slice(), *position, request, |row| {
                        deleted_items_content_row_matches(row, Some(&restriction), snapshot)
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_deleted_items_content_row(*row, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == CALENDAR_FOLDER_ID {
                let mut rows =
                    calendar_content_rows(snapshot, *folder_id, table_restriction.as_ref());
                sort_events(&mut rows, sort_orders);
                if let Some((index, event)) =
                    find_row(rows.as_slice(), *position, request, |event| {
                        restriction_matches_event(Some(&restriction), event)
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_versioned_event_row(event, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if let Some(folder) = snapshot.collaboration_folder_for_id(*folder_id) {
                match folder.kind {
                    MapiCollaborationFolderKind::Contacts => {
                        let mut rows = snapshot.contacts_for_folder(*folder_id);
                        rows.retain(|contact| {
                            restriction_matches_contact_in_folder(
                                table_restriction.as_ref(),
                                &contact.contact,
                                *folder_id,
                            )
                        });
                        sort_contacts(&mut rows, sort_orders);
                        if let Some((index, contact)) =
                            find_row(rows.as_slice(), *position, request, |contact| {
                                restriction_matches_contact_in_folder(
                                    Some(&restriction),
                                    &contact.contact,
                                    *folder_id,
                                )
                            })
                        {
                            *position = index;
                            response.push(1);
                            write_standard_property_row(
                                &mut response,
                                &serialize_contact_row(
                                    &contact.contact,
                                    contact.id,
                                    contact.folder_id,
                                    &columns,
                                ),
                            );
                        } else {
                            return rop_find_row_no_match_response(request);
                        }
                    }
                    MapiCollaborationFolderKind::Calendar => {
                        let mut rows =
                            calendar_content_rows(snapshot, *folder_id, table_restriction.as_ref());
                        sort_events(&mut rows, sort_orders);
                        if let Some((index, event)) =
                            find_row(rows.as_slice(), *position, request, |event| {
                                restriction_matches_event(Some(&restriction), event)
                            })
                        {
                            *position = index;
                            response.push(1);
                            write_standard_property_row(
                                &mut response,
                                &serialize_versioned_event_row(event, &columns),
                            );
                        } else {
                            return rop_find_row_no_match_response(request);
                        }
                    }
                    MapiCollaborationFolderKind::Task => {
                        let mut rows = snapshot.tasks_for_folder(*folder_id);
                        rows.retain(|task| {
                            restriction_matches_task(table_restriction.as_ref(), &task.task)
                        });
                        sort_tasks(&mut rows, sort_orders);
                        if let Some((index, task)) =
                            find_row(rows.as_slice(), *position, request, |task| {
                                restriction_matches_task(Some(&restriction), &task.task)
                            })
                        {
                            *position = index;
                            response.push(1);
                            write_standard_property_row(
                                &mut response,
                                &serialize_task_row(&task.task, task.id, task.folder_id, &columns),
                            );
                        } else {
                            return rop_find_row_no_match_response(request);
                        }
                    }
                }
            } else if *folder_id == CONTACTS_SEARCH_FOLDER_ID {
                let mut rows = snapshot.contacts_search_results();
                rows.retain(|contact| {
                    restriction_matches_contact_in_folder(
                        table_restriction.as_ref(),
                        &contact.contact,
                        CONTACTS_SEARCH_FOLDER_ID,
                    )
                });
                sort_contacts(&mut rows, sort_orders);
                if let Some((index, contact)) =
                    find_row(rows.as_slice(), *position, request, |contact| {
                        restriction_matches_contact_in_folder(
                            Some(&restriction),
                            &contact.contact,
                            CONTACTS_SEARCH_FOLDER_ID,
                        )
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_contact_row(
                            &contact.contact,
                            contact.id,
                            CONTACTS_SEARCH_FOLDER_ID,
                            &columns,
                        ),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == TODO_SEARCH_FOLDER_ID {
                let mut rows = todo_search_content_rows(snapshot, table_restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                let row_refs = rows.iter().collect::<Vec<_>>();
                if let Some((index, row)) =
                    find_row(row_refs.as_slice(), *position, request, |row| {
                        search_content_row_matches(row, Some(&restriction))
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_search_content_row(*row, snapshot, &columns, false),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID {
                let mut rows = snapshot.tracked_mail_processing_messages();
                retain_rows_by_restriction(
                    &mut rows,
                    table_restriction.as_ref(),
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
                if let Some((index, message)) =
                    find_row(rows.as_slice(), *position, request, |message| {
                        restriction_matches_email_in_snapshot(
                            Some(&restriction),
                            &message.email,
                            *folder_id,
                            snapshot,
                        )
                    })
                {
                    *position = index;
                    response.push(1);
                    response.extend_from_slice(&serialize_message_property_row(
                        &message.email,
                        &columns,
                    ));
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == REMINDERS_FOLDER_ID {
                let mut rows = reminder_search_content_rows(snapshot, table_restriction.as_ref());
                sort_search_content_rows(&mut rows, sort_orders);
                let row_refs = rows.iter().collect::<Vec<_>>();
                if let Some((index, row)) =
                    find_row(row_refs.as_slice(), *position, request, |row| {
                        search_content_row_matches(row, Some(&restriction))
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_search_content_row(*row, snapshot, &columns, true),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == NOTES_FOLDER_ID {
                let mut rows = snapshot.notes_for_folder(*folder_id);
                rows.retain(|note| {
                    restriction_matches_note(table_restriction.as_ref(), &note.note)
                });
                sort_notes(&mut rows, sort_orders);
                if let Some((index, note)) = find_row(rows.as_slice(), *position, request, |note| {
                    restriction_matches_note(Some(&restriction), &note.note)
                }) {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_note_row(&note.note, note.id, note.folder_id, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if *folder_id == JOURNAL_FOLDER_ID {
                let mut rows = snapshot.journal_entries_for_folder(*folder_id);
                rows.retain(|entry| {
                    restriction_matches_journal_entry(table_restriction.as_ref(), &entry.entry)
                });
                sort_journal_entries(&mut rows, sort_orders);
                if let Some((index, entry)) =
                    find_row(rows.as_slice(), *position, request, |entry| {
                        restriction_matches_journal_entry(Some(&restriction), &entry.entry)
                    })
                {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_journal_entry_row(
                            &entry.entry,
                            entry.id,
                            entry.folder_id,
                            &columns,
                        ),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if snapshot.public_folder_for_id(*folder_id).is_some() {
                let mut rows = snapshot.public_folder_items_for_folder(*folder_id);
                retain_rows_by_restriction(
                    &mut rows,
                    table_restriction.as_ref(),
                    |item, restriction| restriction_matches_public_folder_item(restriction, item),
                );
                if let Some((index, item)) = find_row(rows.as_slice(), *position, request, |item| {
                    restriction_matches_public_folder_item(Some(&restriction), item)
                }) {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_public_folder_item_row(item, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else if crate::mapi_store::recoverable_storage_folder(*folder_id).is_some() {
                let mut rows = snapshot.recoverable_items_for_folder(*folder_id);
                sort_recoverable_items(&mut rows, sort_orders);
                if let Some((index, item)) = find_row(rows.as_slice(), *position, request, |item| {
                    restriction_matches(Some(&restriction), |property_tag| {
                        recoverable_item_property_value(item, property_tag)
                    })
                }) {
                    *position = index;
                    response.push(1);
                    write_standard_property_row(
                        &mut response,
                        &serialize_recoverable_item_row(item, &columns),
                    );
                } else {
                    return rop_find_row_no_match_response(request);
                }
            } else {
                let view_signature = table_view_signature(sort_orders, table_restriction.as_ref());
                let window_emails = if is_top_level_count_restriction(table_restriction.as_ref()) {
                    None
                } else {
                    snapshot.content_table_window_emails_containing(
                        *folder_id,
                        view_signature,
                        *position,
                    )
                };
                if let Some((offset, _total, window_emails)) = window_emails {
                    let local_position = position.saturating_sub(offset);
                    if let Some((index, email)) =
                        find_row(window_emails.as_slice(), local_position, request, |email| {
                            restriction_matches_email_in_snapshot(
                                Some(&restriction),
                                email,
                                *folder_id,
                                snapshot,
                            )
                        })
                    {
                        *position = offset.saturating_add(index);
                        response.push(1);
                        response
                            .extend_from_slice(&serialize_message_property_row(email, &columns));
                    } else {
                        let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                        retain_rows_by_restriction(
                            &mut rows,
                            table_restriction.as_ref(),
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
                        let complete_rows_available =
                            !is_top_level_count_restriction(table_restriction.as_ref())
                                && snapshot
                                    .content_table_total(*folder_id, view_signature)
                                    .is_some_and(|total| total == rows.len());
                        let found = complete_rows_available.then(|| {
                            find_row(rows.as_slice(), *position, request, |email| {
                                restriction_matches_email_in_snapshot(
                                    Some(&restriction),
                                    email,
                                    *folder_id,
                                    snapshot,
                                )
                            })
                        });
                        if let Some(Some((index, email))) = found {
                            *position = index;
                            response.push(1);
                            response.extend_from_slice(&serialize_message_property_row(
                                email, &columns,
                            ));
                        } else {
                            return rop_find_row_no_match_response(request);
                        }
                    }
                } else {
                    let mut rows = emails_for_folder(*folder_id, mailboxes, emails);
                    retain_rows_by_restriction(
                        &mut rows,
                        table_restriction.as_ref(),
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
                    if let Some((index, email)) =
                        find_row(rows.as_slice(), *position, request, |email| {
                            restriction_matches_email_in_snapshot(
                                Some(&restriction),
                                email,
                                *folder_id,
                                snapshot,
                            )
                        })
                    {
                        *position = index;
                        response.push(1);
                        response
                            .extend_from_slice(&serialize_message_property_row(email, &columns));
                    } else {
                        return rop_find_row_no_match_response(request);
                    }
                }
            }
        }
        MapiObject::AttachmentTable {
            folder_id,
            message_id,
            materialized_attachments,
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
            let mut rows = materialized_attachments
                .as_deref()
                .or_else(|| snapshot.attachments_for_message(*folder_id, *message_id))
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
                return rop_find_row_no_match_response(request);
            }
        }
        _ => return rop_error_response(0x4F, request.response_handle_index(), 0x8004_0102),
    }

    response
}

#[cfg(test)]
mod tests;
