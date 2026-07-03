use super::*;

pub(super) fn apply_outlook_smart_input_variant_before_query_rows(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    request_rop_names: &str,
) -> Option<String> {
    if session.outlook_smart_input_variant != "fai_cursor_reset_before_query_rows" {
        return None;
    }
    let input_index = request.input_handle_index().unwrap_or(0);
    let handle = input_handle(handle_slots, request);
    let Some(handle_value) = handle else {
        return None;
    };
    let Some(mut object) = session.handles.remove(&handle_value) else {
        return None;
    };
    let applied = match &mut object {
        MapiObject::ContentsTable {
            folder_id,
            associated,
            position,
            ..
        } if *folder_id == INBOX_FOLDER_ID && *associated => {
            let previous_position = *position;
            *position = 0;
            Some((*folder_id, previous_position))
        }
        _ => None,
    };
    session.handles.insert(handle_value, object);
    let Some((folder_id, previous_position)) = applied else {
        return None;
    };
    session.outlook_smart_input_variant_applied = true;
    Some(format!(
        "variant=fai_cursor_reset_before_query_rows;request_id={request_id};request_rops={request_rop_names};input_index={input_index};handle={};folder=0x{folder_id:016x};associated=true;previous_position={previous_position};new_position=0",
        format_optional_debug_handle(handle)
    ))
}

pub(super) fn hierarchy_table_object(
    folder_id: u64,
    deleted_advertised_special_folders: HashSet<u64>,
) -> MapiObject {
    MapiObject::HierarchyTable {
        folder_id,
        columns: default_hierarchy_columns(),
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        deleted_advertised_special_folders,
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    }
}

pub(super) fn contents_table_object(folder_id: u64, associated: bool) -> MapiObject {
    MapiObject::ContentsTable {
        folder_id,
        associated,
        columns: Vec::new(),
        columns_set: false,
        sort_orders: Vec::new(),
        category_count: 0,
        expanded_count: 0,
        collapsed_categories: HashSet::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    }
}

pub(super) fn attachment_table_object(folder_id: u64, message_id: u64) -> MapiObject {
    MapiObject::AttachmentTable {
        folder_id,
        message_id,
        columns: Vec::new(),
        columns_set: false,
        sort_orders: Vec::new(),
        restriction: None,
        bookmarks: HashMap::new(),
        next_bookmark: 1,
        position: 0,
    }
}

pub(super) fn permission_table_object(folder_id: u64) -> MapiObject {
    MapiObject::PermissionTable {
        folder_id,
        columns: default_permission_columns(),
        columns_set: false,
        position: 0,
    }
}

pub(super) fn rule_table_object(folder_id: u64) -> MapiObject {
    MapiObject::RuleTable {
        folder_id,
        columns: default_rule_columns(),
        columns_set: false,
        position: 0,
    }
}

pub(super) fn normal_message_table_column_support_summary(columns: &[u32]) -> String {
    table_column_support_summary(columns, normal_message_table_column_is_backed)
}

pub(super) fn normal_message_defaulted_column_detail(columns: &[u32]) -> String {
    columns
        .iter()
        .filter_map(|column| {
            let storage_tag = canonical_property_storage_tag(*column);
            let tag = MapiPropertyTag::new(storage_tag);
            if tag.property_id() >= FIRST_NAMED_PROPERTY_ID
                || normal_message_table_column_is_backed(storage_tag)
            {
                return None;
            }
            let mut default_bytes = Vec::new();
            write_property_default(&mut default_bytes, storage_tag);
            let property_type = tag
                .property_type()
                .map(|property_type| format!("{property_type:?}"))
                .unwrap_or_else(|| "unknown".to_string());
            let source = if tag.property_id() == 0x1213 {
                "ms_oxprops=not_found_in_cached_2025_05_20"
            } else {
                "ms_oxprops=unchecked"
            };
            Some(format!(
                "tag=0x{column:08x};storage=0x{storage_tag:08x};property_id=0x{:04x};property_type=0x{:04x}:{property_type};backed=false;default_wire_bytes={};{}",
                tag.property_id(),
                tag.property_type_code(),
                hex_preview(&default_bytes, 32),
                source
            ))
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(super) fn associated_contents_table_column_support_summary(columns: &[u32]) -> String {
    table_column_support_summary(columns, associated_contents_table_column_is_backed)
}

pub(super) fn contents_table_column_support_summary(associated: bool, columns: &[u32]) -> String {
    if associated {
        associated_contents_table_column_support_summary(columns)
    } else {
        normal_message_table_column_support_summary(columns)
    }
}

pub(super) fn effective_contents_table_columns(
    folder_id: u64,
    associated: bool,
    columns: &[u32],
) -> Vec<u32> {
    if !columns.is_empty() {
        return columns.to_vec();
    }
    if associated && folder_id == COMMON_VIEWS_FOLDER_ID {
        default_navigation_shortcut_property_tags()
    } else if associated && folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
        default_conversation_action_property_tags()
    } else if associated && matches!(folder_id, INBOX_FOLDER_ID | QUICK_STEP_SETTINGS_FOLDER_ID) {
        default_associated_config_columns()
    } else {
        default_contents_columns()
    }
}

pub(super) fn normalize_table_property_tags_for_session(
    session: &MapiSession,
    tags: Vec<u32>,
) -> Vec<u32> {
    tags.into_iter()
        .map(|tag| normalize_table_property_tag_for_session(session, tag))
        .collect()
}

pub(super) fn select_query_window(
    total: usize,
    position: usize,
    forward_read: bool,
    row_count: usize,
) -> Vec<usize> {
    if forward_read {
        let end = total.min(position.saturating_add(row_count));
        return (position.min(total)..end).collect();
    }
    let start = position.saturating_sub(row_count);
    (start..position.min(total)).rev().collect()
}

pub(super) fn format_outlook_query_row_window(
    folder_id: u64,
    associated: bool,
    position: usize,
    forward_read: bool,
    row_count: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    columns: &[u32],
    account_id: Uuid,
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !associated || row_count == 0 {
        return String::new();
    }
    if folder_id == INBOX_FOLDER_ID {
        return format_inbox_associated_query_row_window(
            account_id,
            position,
            forward_read,
            row_count,
            sort_orders,
            restriction,
            snapshot,
        );
    }
    if folder_id == COMMON_VIEWS_FOLDER_ID {
        return format_common_views_query_row_window(
            position,
            forward_read,
            row_count,
            sort_orders,
            restriction,
            columns,
            account_id,
            snapshot,
        );
    }
    String::new()
}

pub(super) fn format_inbox_associated_query_row_window(
    account_id: Uuid,
    position: usize,
    forward_read: bool,
    row_count: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    let mut rows = debug_associated_table_rows(INBOX_FOLDER_ID, snapshot, restriction, account_id);
    sort_debug_associated_table_rows(&mut rows, sort_orders, account_id);
    let selected = select_query_window(rows.len(), position, forward_read, row_count);
    let parts = selected
        .iter()
        .map(|index| {
            let message = &rows[*index];
            format!(
                "index={};id=0x{:016x};class={};subject={}",
                index,
                debug_associated_row_id(message),
                debug_associated_row_class(message),
                debug_associated_row_subject(message)
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "total={};start={};forward={};returned={};{}",
        rows.len(),
        position,
        forward_read,
        selected.len(),
        parts
    )
}

pub(super) fn format_common_views_query_row_window(
    position: usize,
    forward_read: bool,
    row_count: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    columns: &[u32],
    account_id: Uuid,
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
    rows.retain(|message| {
        restriction_matches_common_views_message(restriction, message, account_id)
    });
    if is_unrestricted_common_views_navigation_projection(columns, &restriction.cloned()) {
        rows.retain(|message| {
            matches!(
                message,
                crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(_)
            )
        });
    }
    sort_common_views_messages(&mut rows, sort_orders);
    let selected = select_query_window(rows.len(), position, forward_read, row_count);
    let parts = selected
        .iter()
        .filter_map(|index| match &rows[*index] {
            crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(message) => {
                Some(format!(
                    "index={};id=0x{:016x};subject={};target={};type={};section={};ordinal={}",
                    index,
                    message.id,
                    message.subject,
                    message
                        .target_folder_id
                        .map(|folder_id| format!("0x{folder_id:016x}"))
                        .unwrap_or_else(|| "none".to_string()),
                    message.shortcut_type,
                    message.section,
                    message.ordinal
                ))
            }
            crate::mapi_store::MapiCommonViewsMessage::NamedView(message) => Some(format!(
                "index={};id=0x{:016x};subject={};class=IPM.Microsoft.FolderDesign.NamedView;view_flags={};view_type={}",
                index, message.id, message.name, message.view_flags, message.view_type
            )),
            crate::mapi_store::MapiCommonViewsMessage::SearchFolderDefinition(message) => Some(
                format!(
                    "index={};id={};subject={};class=IPM.Microsoft.WunderBar.SFInfo;role={}",
                    index, message.id, message.display_name, message.role
                ),
            ),
        })
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "total={};start={};forward={};returned={};{}",
        rows.len(),
        position,
        forward_read,
        selected.len(),
        parts
    )
}

pub(super) fn format_outlook_query_row_values(
    account_id: Uuid,
    folder_id: u64,
    associated: bool,
    position: usize,
    forward_read: bool,
    row_count: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if !associated || row_count == 0 || columns.is_empty() {
        return String::new();
    }
    if folder_id == COMMON_VIEWS_FOLDER_ID {
        let mut rows = snapshot.common_views_table_messages().collect::<Vec<_>>();
        rows.retain(|message| {
            restriction_matches_common_views_message(restriction, message, account_id)
        });
        if is_unrestricted_common_views_navigation_projection(columns, &restriction.cloned()) {
            rows.retain(|message| {
                matches!(
                    message,
                    crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(_)
                )
            });
        }
        sort_common_views_messages(&mut rows, sort_orders);
        return select_query_window(rows.len(), position, forward_read, row_count)
            .iter()
            .map(|index| {
                let message = &rows[*index];
                match message {
                    crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(shortcut) => {
                        let values = columns
                            .iter()
                            .map(|tag| {
                                let value =
                                    navigation_shortcut_property_value(shortcut, account_id, *tag)
                                        .map(|value| format_debug_mapi_value(&value))
                                        .unwrap_or_else(|| "default".to_string());
                                format!("0x{tag:08x}={value}")
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("index={};id=0x{:016x};{}", index, shortcut.id, values)
                    }
                    crate::mapi_store::MapiCommonViewsMessage::NamedView(view) => {
                        let values = columns
                            .iter()
                            .map(|tag| {
                                let value =
                                    common_view_named_view_property_value(view, account_id, *tag)
                                        .map(|value| format_debug_mapi_value(&value))
                                        .unwrap_or_else(|| "default".to_string());
                                format!("0x{tag:08x}={value}")
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("index={};id=0x{:016x};{}", index, view.id, values)
                    }
                    crate::mapi_store::MapiCommonViewsMessage::SearchFolderDefinition(
                        definition,
                    ) => {
                        let values = columns
                            .iter()
                            .map(|tag| {
                                let value = search_folder_definition_message_property_value(
                                    definition, account_id, *tag,
                                )
                                .map(|value| format_debug_mapi_value(&value))
                                .unwrap_or_else(|| "default".to_string());
                                format!("0x{tag:08x}={value}")
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        let id = crate::mapi::identity::mapped_mapi_object_id(&definition.id)
                            .unwrap_or_default();
                        format!("index={index};id=0x{id:016x};{values}")
                    }
                }
            })
            .collect::<Vec<_>>()
            .join("|");
    }
    let mut rows = debug_associated_table_rows(folder_id, snapshot, restriction, account_id);
    if rows.is_empty() {
        return String::new();
    }
    sort_debug_associated_table_rows(&mut rows, sort_orders, account_id);
    select_query_window(rows.len(), position, forward_read, row_count)
        .iter()
        .map(|index| {
            let message = &rows[*index];
            let values = columns
                .iter()
                .map(|tag| {
                    let value = debug_associated_row_property_value(message, account_id, *tag)
                        .map(|value| format_debug_mapi_value(&value))
                        .unwrap_or_else(|| "default".to_string());
                    format!("0x{tag:08x}={value}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "index={};id=0x{:016x};class={};{}",
                index,
                debug_associated_row_id(message),
                debug_associated_row_class(message),
                values
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(super) fn format_normal_message_query_row_summary(
    folder_id: u64,
    associated: bool,
    position: usize,
    forward_read: bool,
    row_count: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if associated || row_count == 0 || columns.is_empty() {
        return String::new();
    }
    if folder_id == CONTACTS_FOLDER_ID
        || folder_id == CONTACTS_SEARCH_FOLDER_ID
        || snapshot
            .collaboration_folder_for_id(folder_id)
            .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Contacts)
    {
        return format_contact_query_row_summary(
            folder_id,
            position,
            forward_read,
            row_count,
            sort_orders,
            restriction,
            columns,
            snapshot,
        );
    }

    let mut rows = emails_for_folder(folder_id, mailboxes, emails);
    rows.retain(|email| restriction_matches_email(restriction, email));
    sort_emails(&mut rows, sort_orders);
    let selected = select_query_window(rows.len(), position, forward_read, row_count);
    let row_summaries = selected
        .iter()
        .take(5)
        .map(|index| {
            let email = rows[*index];
            let serialized = serialize_message_row(email, columns);
            let standard_row = standard_property_row_bytes(&serialized);
            let values = columns
                .iter()
                .map(|tag| {
                    let value = normal_message_debug_property_value(email, *tag)
                        .map(|value| format_normal_message_debug_value(*tag, &value))
                        .unwrap_or_else(|| "default".to_string());
                    format!("0x{tag:08x}={value}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "index={};mid=0x{:016x};subject={};class={};unread={};body_text_len={};body_html_len={};value_len={};status_row_len={};values={}",
                index,
                mapi_message_id(email),
                email.subject,
                message_class_for_email(email),
                email.unread,
                email.body_text.len(),
                email.body_html_sanitized.as_ref().map(|body| body.len()).unwrap_or(0),
                serialized.len(),
                standard_row.len(),
                values
            )
        })
        .collect::<Vec<_>>()
        .join("|");

    format!(
        "total={};position={};forward={};requested={};returned={};summarized={};{}",
        rows.len(),
        position,
        forward_read,
        row_count,
        selected.len(),
        selected.len().min(5),
        row_summaries
    )
}

fn format_contact_query_row_summary(
    folder_id: u64,
    position: usize,
    forward_read: bool,
    row_count: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    let mut rows = if folder_id == CONTACTS_SEARCH_FOLDER_ID {
        snapshot.contacts_search_results()
    } else {
        snapshot.contacts_for_folder(folder_id)
    };
    rows.retain(|contact| {
        restriction_matches_contact_in_folder(restriction, &contact.contact, folder_id)
    });
    sort_contacts(&mut rows, sort_orders);
    let selected = select_query_window(rows.len(), position, forward_read, row_count);
    let row_summaries = selected
        .iter()
        .take(5)
        .map(|index| {
            let contact = rows[*index];
            let serialized =
                serialize_contact_row(&contact.contact, contact.id, contact.folder_id, columns);
            let standard_row = standard_property_row_bytes(&serialized);
            let values = columns
                .iter()
                .map(|tag| {
                    let value = contact_property_value(
                        &contact.contact,
                        contact.id,
                        contact.folder_id,
                        *tag,
                    )
                    .map(|value| format_debug_mapi_value(&value))
                    .unwrap_or_else(|| "default".to_string());
                    format!("0x{tag:08x}={value}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "index={};mid=0x{:016x};name={};email={};value_len={};status_row_len={};values={}",
                index,
                contact.id,
                contact.contact.name,
                contact.contact.email,
                serialized.len(),
                standard_row.len(),
                values
            )
        })
        .collect::<Vec<_>>()
        .join("|");

    format!(
        "contact_total={};position={};forward={};requested={};returned={};summarized={};{}",
        rows.len(),
        position,
        forward_read,
        row_count,
        selected.len(),
        selected.len().min(5),
        row_summaries
    )
}

pub(super) fn format_calendar_event_query_position_summary(
    folder_id: u64,
    associated: bool,
    position: usize,
    row_count: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    columns: &[u32],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    if associated || folder_id != CALENDAR_FOLDER_ID || row_count == 0 || columns.is_empty() {
        return String::new();
    }
    let mut rows = calendar_content_rows(snapshot, folder_id, restriction);
    sort_events(&mut rows, sort_orders);
    let selected = select_query_window(rows.len(), position, true, row_count);
    let row_summaries = selected
        .iter()
        .take(5)
        .map(|index| {
            let event = rows[*index];
            let serialized = serialize_event_row(&event.event, event.id, event.folder_id, columns);
            let standard_row = standard_property_row_bytes(&serialized);
            let values = columns
                .iter()
                .map(|tag| {
                    let value = event_property_value(&event.event, event.id, event.folder_id, *tag)
                        .map(|value| format_debug_mapi_value(&value))
                        .unwrap_or_else(|| "default".to_string());
                    format!("0x{tag:08x}={value}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "index={};mid=0x{:016x};title={};value_len={};status_row_len={};values={}",
                index,
                event.id,
                event.event.title,
                serialized.len(),
                standard_row.len(),
                values
            )
        })
        .collect::<Vec<_>>()
        .join("|");

    format!(
        "event_total={};position={};forward=true;requested={};returned={};summarized={};{}",
        rows.len(),
        position,
        row_count,
        selected.len(),
        selected.len().min(5),
        row_summaries
    )
}

pub(super) fn format_normal_message_find_row_failure_candidates(
    folder_id: u64,
    position: usize,
    find_backward: bool,
    request: &RopRequest,
    table_restriction: Option<&MapiRestriction>,
    sort_orders: &[MapiSortOrder],
    selected_columns: &[u32],
    restriction_property_tags: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> String {
    let mut rows = emails_for_folder(folder_id, mailboxes, emails);
    rows.retain(|email| restriction_matches_email(table_restriction, email));
    sort_emails(&mut rows, sort_orders);
    let candidate_tags = candidate_find_row_debug_tags(selected_columns, restriction_property_tags);
    let matching_indices = request
        .restriction()
        .ok()
        .flatten()
        .map(|find_restriction| {
            rows.iter()
                .enumerate()
                .filter_map(|(index, email)| {
                    restriction_matches_email(Some(&find_restriction), email).then_some(index)
                })
                .take(5)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let selected = select_query_window(rows.len(), position, !find_backward, rows.len().min(5));
    let row_summaries = selected
        .iter()
        .map(|index| {
            let email = rows[*index];
            let values = candidate_tags
                .iter()
                .map(|tag| {
                    let value = normal_message_debug_property_value(email, *tag)
                        .map(|value| format_normal_message_debug_value(*tag, &value))
                        .unwrap_or_else(|| "missing".to_string());
                    format!("0x{tag:08x}={value}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "index={};mid=0x{:016x};subject={};class={};values={}",
                index,
                mapi_message_id(email),
                email.subject,
                message_class_for_email(email),
                values
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "total={};position={};backward={};matching_index_count_limited={};matching_indices={};candidate_tags={};summarized={};{}",
        rows.len(),
        position,
        find_backward,
        matching_indices.len(),
        matching_indices
            .iter()
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(","),
        format_debug_property_tags(&candidate_tags),
        selected.len(),
        row_summaries
    )
}

fn candidate_find_row_debug_tags(
    selected_columns: &[u32],
    restriction_property_tags: &[u32],
) -> Vec<u32> {
    let mut tags = Vec::new();
    for tag in selected_columns
        .iter()
        .chain(restriction_property_tags.iter())
    {
        if !tags.contains(tag) {
            tags.push(*tag);
        }
    }
    for tag in [
        PID_TAG_MID,
        PID_TAG_SEARCH_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_SUBJECT_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_DELIVERY_TIME,
        PID_TAG_CLIENT_SUBMIT_TIME,
    ] {
        if !tags.contains(&tag) {
            tags.push(tag);
        }
    }
    tags
}

fn normalize_table_property_tag_for_session(session: &MapiSession, property_tag: u32) -> u32 {
    let tag = MapiPropertyTag::new(property_tag);
    if tag.property_id() < FIRST_NAMED_PROPERTY_ID {
        return property_tag;
    }
    if tag.property_id()
        == MapiPropertyTag::new(OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG).property_id()
    {
        return (PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG
            & 0xffff_0000)
            | u32::from(tag.property_type_code());
    }
    let Some(property) = session.named_property_ids.get(&tag.property_id()) else {
        return property_tag;
    };
    if let Some(well_known_id) = well_known_named_property_id(property) {
        return (u32::from(well_known_id) << 16) | u32::from(tag.property_type_code());
    }
    if is_sharing_local_folder_named_property(property) {
        return (PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG
            & 0xffff_0000)
            | u32::from(tag.property_type_code());
    }
    property_tag
}

fn is_sharing_local_folder_named_property(property: &MapiNamedProperty) -> bool {
    property.guid == PSETID_SHARING_GUID
        && matches!(
            &property.kind,
            MapiNamedPropertyKind::Name(name)
                if name.eq_ignore_ascii_case(
                    "SharingCalendarGroupEntryAssociatedLocalFolderId"
                )
        )
}

fn table_column_support_summary(columns: &[u32], is_backed: impl Fn(u32) -> bool) -> String {
    let mut backed = Vec::new();
    let mut defaulted = Vec::new();
    let mut named_or_dynamic = Vec::new();

    for column in columns {
        let storage_tag = canonical_property_storage_tag(*column);
        if is_backed(storage_tag) {
            backed.push(*column);
        } else if MapiPropertyTag::new(storage_tag).property_id() >= FIRST_NAMED_PROPERTY_ID {
            named_or_dynamic.push(*column);
        } else {
            defaulted.push(*column);
        }
    }

    format!(
        "backed={};defaulted={};named_or_dynamic={}",
        format_debug_property_tags(&backed),
        format_debug_property_tags(&defaulted),
        format_debug_property_tags(&named_or_dynamic)
    )
}

fn associated_contents_table_column_is_backed(storage_tag: u32) -> bool {
    normal_message_table_column_is_backed(storage_tag)
        || matches!(
            storage_tag,
            PID_TAG_ASSOCIATED
                | PID_TAG_SENT_MAIL_SVR_EID
                | PID_TAG_ROAMING_DATATYPES
                | PID_TAG_ROAMING_DICTIONARY
                | PID_TAG_ROAMING_XML_STREAM
                | OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
                | PID_NAME_CONTENT_TYPE_W_TAG
                | PID_TAG_VIEW_DESCRIPTOR_CLSID
                | PID_TAG_VIEW_DESCRIPTOR_FLAGS
                | PID_TAG_VIEW_DESCRIPTOR_VERSION
                | PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL
                | PID_TAG_VIEW_DESCRIPTOR_NAME_W
                | PID_TAG_VIEW_DESCRIPTOR_STRINGS_W
                | PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE
                | PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE
                | PID_TAG_VIEW_DESCRIPTOR_BINARY
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835
                | OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C
                | PID_TAG_WLINK_GROUP_HEADER_ID
                | PID_TAG_WLINK_SAVE_STAMP
                | PID_TAG_WLINK_TYPE
                | PID_TAG_WLINK_FLAGS
                | PID_TAG_WLINK_ORDINAL
                | PID_TAG_WLINK_ENTRY_ID
                | PID_TAG_WLINK_RECORD_KEY
                | PID_TAG_WLINK_STORE_ENTRY_ID
                | PID_TAG_WLINK_FOLDER_TYPE
                | PID_TAG_WLINK_GROUP_CLSID
                | PID_TAG_WLINK_GROUP_NAME_W
                | PID_TAG_WLINK_SECTION
                | PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID
                | 0x685D_0003
                | 0x7C09_0102
                | PID_TAG_WLINK_CALENDAR_COLOR
                | PID_TAG_WLINK_ADDRESS_BOOK_EID
                | PID_TAG_WLINK_CLIENT_ID
                | PID_TAG_WLINK_RO_GROUP_TYPE
                | 0x6893_0102
        )
        || property_ids_match(storage_tag, PID_TAG_VIEW_DESCRIPTOR_CLSID)
        || property_ids_match(storage_tag, PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE)
        || property_ids_match(storage_tag, PID_TAG_WLINK_GROUP_HEADER_ID)
        || property_ids_match(storage_tag, PID_TAG_WLINK_FOLDER_TYPE)
        || property_ids_match(storage_tag, PID_TAG_WLINK_GROUP_CLSID)
}

fn normal_message_table_column_is_backed(storage_tag: u32) -> bool {
    matches!(
        storage_tag,
        PID_TAG_FOLDER_ID
            | PID_TAG_PARENT_FOLDER_ID
            | PID_TAG_MID
            | PID_TAG_INST_ID
            | PID_TAG_INSTANCE_NUM
            | PID_TAG_ROW_TYPE
            | PID_TAG_SUBJECT_W
            | PID_TAG_NORMALIZED_SUBJECT_W
            | PID_TAG_CONVERSATION_TOPIC_W
            | PID_TAG_MESSAGE_CLASS_W
            | PID_TAG_ORIGINAL_MESSAGE_CLASS_W
            | PID_TAG_CREATION_TIME
            | PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_CLIENT_SUBMIT_TIME
            | PID_TAG_ACCESS
            | PID_TAG_ACCESS_LEVEL
            | PID_TAG_IMPORTANCE
            | PID_TAG_MESSAGE_STATUS
            | PID_TAG_MESSAGE_FLAGS
            | PID_TAG_READ
            | PID_TAG_FLAG_STATUS
            | PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG
            | PID_LID_OUTLOOK_COMMON_8514_TAG
            | 0x8017_000B
            | PID_TAG_FLAG_COMPLETE_TIME
            | PID_TAG_FOLLOWUP_ICON
            | PID_TAG_TODO_ITEM_FLAGS
            | PID_TAG_SWAPPED_TODO_STORE
            | PID_TAG_SWAPPED_TODO_DATA
            | PID_TAG_MESSAGE_SIZE
            | OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG
            | PID_TAG_SENDER_NAME_W
            | PID_TAG_SENDER_ADDRESS_TYPE_W
            | PID_TAG_SENDER_EMAIL_ADDRESS_W
            | PID_TAG_SENDER_SMTP_ADDRESS_W
            | PID_TAG_SENT_REPRESENTING_NAME_W
            | PID_TAG_SENT_REPRESENTING_ENTRY_ID
            | PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W
            | PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W
            | PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W
            | PID_TAG_DISPLAY_TO_W
            | PID_TAG_DISPLAY_CC_W
            | PID_TAG_DISPLAY_BCC_W
            | PID_TAG_HAS_ATTACHMENTS
            | PID_TAG_RTF_IN_SYNC
            | PID_TAG_BODY_W
            | PID_TAG_RTF_COMPRESSED
            | PID_TAG_BODY_HTML_W
            | PID_TAG_HTML_BINARY
            | PID_TAG_NATIVE_BODY
            | PID_TAG_INTERNET_CODEPAGE
            | PID_TAG_MESSAGE_LOCALE_ID
            | PID_TAG_CONVERSATION_INDEX
            | PID_TAG_ENTRY_ID
            | PID_TAG_INSTANCE_KEY
            | PID_TAG_SOURCE_KEY
            | PID_TAG_SEARCH_KEY
            | PID_TAG_PARENT_SOURCE_KEY
            | PID_TAG_CHANGE_KEY
            | PID_TAG_PREDECESSOR_CHANGE_LIST
            | PID_TAG_CHANGE_NUMBER
            | PID_TAG_INTERNET_MESSAGE_ID_W
            | PID_TAG_TRANSPORT_MESSAGE_HEADERS_W
            | PID_LID_REMINDER_SET_TAG
            | PID_NAME_KEYWORDS_TAG
            | PID_NAME_CONTENT_CLASS_W_TAG
    ) || property_ids_match(storage_tag, PID_NAME_KEYWORDS_TAG)
}

pub(super) fn get_hierarchy_table_response(request: &RopRequest, row_count: u32) -> Vec<u8> {
    rop_get_hierarchy_table_response(request, row_count)
}

pub(super) fn get_contents_table_response(request: &RopRequest, row_count: u32) -> Vec<u8> {
    rop_get_contents_table_response(request, row_count)
}

pub(super) fn get_attachment_table_response(request: &RopRequest) -> Vec<u8> {
    rop_get_attachment_table_response(request)
}

pub(super) fn get_receive_folder_table_response(request: &RopRequest) -> Vec<u8> {
    rop_get_receive_folder_table_response(request)
}

pub(super) fn append_receive_folder_table_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    has_private_logon_handle: bool,
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    if !has_private_logon_handle {
        responses.extend_from_slice(&rop_error_response(
            0x68,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x68",
        row_count = 3u32,
        first_message_class = "IPM.Appointment",
        first_folder_id = format!("0x{CALENDAR_FOLDER_ID:016x}"),
        calendar_row_present = true,
        message_class_wire_type = "String8",
        property_row_wire_shape =
            "PidTagFolderId,PidTagMessageClass,PidTagLastModificationTime",
        message = "rca debug mapi receive folder table"
    );
    responses.extend_from_slice(&get_receive_folder_table_response(request));
    session.record_receive_folder_verification_passed();
}

pub(super) fn append_receive_folder_table_dispatch_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let has_private_logon_handle = private_logon_request_handle(session, handle_slots, request);
    append_receive_folder_table_response(
        principal,
        session,
        has_private_logon_handle,
        request,
        responses,
    );
}

pub(super) fn get_permissions_table_response(request: &RopRequest) -> Vec<u8> {
    rop_get_permissions_table_response(request)
}

pub(super) fn get_rules_table_response(request: &RopRequest) -> Vec<u8> {
    rop_get_rules_table_response(request)
}

pub(super) fn seek_row_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    rop_seek_row_bookmark_response(request, object, mailboxes, emails, snapshot, mailbox_guid)
}

pub(super) fn create_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    rop_create_bookmark_response(request, object, mailboxes, emails, snapshot, mailbox_guid)
}

pub(super) fn free_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
) -> Vec<u8> {
    rop_free_bookmark_response(request, object)
}

pub(super) fn append_free_bookmark_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let object = input_object_mut(session, handle_slots, request);
    responses.extend_from_slice(&free_bookmark_response(request, object));
}

pub(super) fn query_columns_all_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    rop_query_columns_all_response(request, object, snapshot)
}

pub(super) fn set_columns_response(request: &RopRequest) -> Vec<u8> {
    rop_set_columns_response(request)
}

pub(super) fn sort_table_response(request: &RopRequest) -> Vec<u8> {
    rop_sort_table_response(request)
}

pub(super) fn restrict_response(request: &RopRequest) -> Vec<u8> {
    rop_restrict_response(request)
}

pub(super) fn get_search_criteria_response(
    request: &RopRequest,
    restriction: &[u8],
    folder_ids: &[u64],
    search_flags: u32,
) -> Vec<u8> {
    rop_get_search_criteria_response(request, restriction, folder_ids, search_flags)
}

pub(super) fn get_status_response(request: &RopRequest, object: Option<&MapiObject>) -> Vec<u8> {
    rop_get_status_response(request, object)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn append_table_control_response(
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    match RopId::from_u8(request.rop_id) {
        Some(RopId::GetStatus) => responses.extend_from_slice(&get_status_response(
            request,
            input_object(session, handle_slots, request),
        )),
        Some(RopId::QueryPosition) => {
            let calendar_normal_query_position_context = match input_object(
                session,
                handle_slots,
                request,
            ) {
                Some(MapiObject::ContentsTable {
                    folder_id,
                    associated,
                    columns,
                    position,
                    restriction,
                    sort_orders,
                    ..
                }) if *folder_id == CALENDAR_FOLDER_ID && !*associated => {
                    let descriptor_columns =
                        outlook_view_descriptor_visible_property_tags(*folder_id, snapshot);
                    let descriptor_projection = format_calendar_event_query_position_summary(
                        *folder_id,
                        *associated,
                        *position,
                        1,
                        sort_orders,
                        restriction.as_ref(),
                        &descriptor_columns,
                        snapshot,
                    );
                    Some(format!(
                            "handle={};input_index={};position_before={};columns={};sort={};restriction={};view_descriptor_columns={};view_descriptor_row_projection={}",
                            format_optional_debug_handle(input_handle(handle_slots, request)),
                            request.input_handle_index().unwrap_or(0),
                            position,
                            format_debug_property_tags(columns),
                            format_debug_sort_orders(sort_orders),
                            format_debug_restriction_option(restriction.as_ref()),
                            format_debug_property_tags(&descriptor_columns),
                            descriptor_projection
                        ))
                }
                _ => None,
            };
            let response = query_position_response(
                request,
                input_object(session, handle_slots, request),
                mailboxes,
                emails,
                snapshot,
                principal.account_id,
            );
            log_mapi_query_position_debug(
                principal,
                request_id,
                request,
                input_object(session, handle_slots, request),
                &response,
                mailboxes,
                emails,
                snapshot,
            );
            if let Some(context) = calendar_normal_query_position_context {
                let position = response
                    .get(6..10)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u32::from_le_bytes)
                    .unwrap_or(0);
                let row_count = response
                    .get(10..14)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u32::from_le_bytes)
                    .unwrap_or(0);
                session.record_outlook_view_failure_trace_event(format!(
                    "calendar_normal_query_position:{context};response_position={position};response_row_count={row_count}"
                ));
            }
            responses.extend_from_slice(&response);
        }
        Some(RopId::SeekRow) => {
            let before_position =
                input_object(session, handle_slots, request).and_then(table_position);
            let selected_named_property_context = format_contents_table_named_property_context(
                session,
                input_object(session, handle_slots, request),
            );
            let response = seek_row_response(
                request,
                input_object_mut(session, handle_slots, request),
                mailboxes,
                emails,
                snapshot,
                principal.account_id,
            );
            log_outlook_contents_table_seek_row(
                principal,
                request,
                input_object(session, handle_slots, request),
                &selected_named_property_context,
                snapshot,
                before_position,
                &response,
            );
            responses.extend_from_slice(&response);
        }
        Some(RopId::SeekRowBookmark) => responses.extend_from_slice(&seek_row_bookmark_response(
            request,
            input_object_mut(session, handle_slots, request),
            mailboxes,
            emails,
            snapshot,
            principal.account_id,
        )),
        Some(RopId::SeekRowFractional) => {
            responses.extend_from_slice(&seek_row_fractional_response(
                request,
                input_object_mut(session, handle_slots, request),
                mailboxes,
                emails,
                snapshot,
                principal.account_id,
            ))
        }
        Some(RopId::CreateBookmark) => responses.extend_from_slice(&create_bookmark_response(
            request,
            input_object_mut(session, handle_slots, request),
            mailboxes,
            emails,
            snapshot,
            principal.account_id,
        )),
        Some(RopId::QueryColumnsAll) => responses.extend_from_slice(&query_columns_all_response(
            request,
            input_object(session, handle_slots, request),
            snapshot,
        )),
        Some(RopId::ExpandRow) => responses.extend_from_slice(&expand_row_response(
            request,
            input_object_mut(session, handle_slots, request),
            mailboxes,
            emails,
            snapshot,
        )),
        Some(RopId::CollapseRow) => responses.extend_from_slice(&collapse_row_response(
            request,
            input_object_mut(session, handle_slots, request),
            mailboxes,
            emails,
            snapshot,
        )),
        Some(RopId::GetCollapseState) => responses.extend_from_slice(&get_collapse_state_response(
            request,
            input_object(session, handle_slots, request),
        )),
        Some(RopId::SetCollapseState) => responses.extend_from_slice(&set_collapse_state_response(
            request,
            input_object_mut(session, handle_slots, request),
        )),
        _ => {}
    }
}

pub(super) fn seek_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    rop_seek_row_response(request, object, mailboxes, emails, snapshot, mailbox_guid)
}

pub(super) fn seek_row_fractional_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    rop_seek_row_fractional_response(request, object, mailboxes, emails, snapshot, mailbox_guid)
}

pub(super) fn collapse_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    rop_collapse_row_response(request, object, mailboxes, emails, snapshot)
}

pub(super) fn get_collapse_state_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Vec<u8> {
    rop_get_collapse_state_response(request, object)
}

pub(super) fn set_collapse_state_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
) -> Vec<u8> {
    rop_set_collapse_state_response(request, object)
}

pub(super) fn expand_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    rop_expand_row_response(request, object, mailboxes, emails, snapshot)
}

pub(super) fn query_position_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    rop_query_position_response(request, object, mailboxes, emails, snapshot, mailbox_guid)
}

pub(super) fn query_rows_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    rop_query_rows_response(request, object, mailboxes, emails, snapshot, mailbox_guid)
}

pub(super) fn find_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    rop_find_row_response(request, object, mailboxes, emails, snapshot, mailbox_guid)
}
