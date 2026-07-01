use super::*;

pub(super) fn query_rows_request_is_valid(request: &RopRequest) -> bool {
    let Some(flags) = request.payload.first().copied() else {
        return false;
    };
    if flags & !0x03 != 0 {
        return false;
    }
    matches!(request.payload.get(1).copied(), Some(0x00 | 0x01))
        && request.payload.get(2..4).is_some()
}

pub(super) fn query_rows_response_columns(
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u32> {
    match object {
        Some(MapiObject::HierarchyTable {
            folder_id, columns, ..
        }) if is_queryable_hierarchy_folder(*folder_id)
            || snapshot.public_folder_for_id(*folder_id).is_some() =>
        {
            if columns.is_empty() {
                default_hierarchy_columns()
            } else {
                columns.clone()
            }
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            ..
        }) => {
            if !columns.is_empty() {
                return columns.clone();
            }
            if !*associated
                && (is_contact_contents_folder(*folder_id)
                    || *folder_id == CONTACTS_SEARCH_FOLDER_ID
                    || snapshot
                        .collaboration_folder_for_id(*folder_id)
                        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Contacts))
            {
                default_contact_property_tags()
            } else if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                default_navigation_shortcut_property_tags()
            } else if *associated && *folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                default_conversation_action_property_tags()
            } else if *associated && *folder_id == FREEBUSY_DATA_FOLDER_ID {
                default_message_property_tags()
            } else if *associated
                && (*folder_id == CALENDAR_FOLDER_ID
                    || snapshot
                        .collaboration_folder_for_id(*folder_id)
                        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar))
            {
                default_calendar_configuration_property_tags()
            } else if *associated && should_use_associated_config_table(*folder_id, snapshot, None)
            {
                default_associated_config_columns()
            } else {
                default_contents_columns()
            }
        }
        Some(MapiObject::AttachmentTable { columns, .. }) => {
            if columns.is_empty() {
                default_attachment_columns()
            } else {
                columns.clone()
            }
        }
        Some(MapiObject::PermissionTable { columns, .. }) => {
            if columns.is_empty() {
                default_permission_columns()
            } else {
                columns.clone()
            }
        }
        Some(MapiObject::RuleTable { columns, .. }) => {
            if columns.is_empty() {
                default_rule_columns()
            } else {
                columns.clone()
            }
        }
        _ => Vec::new(),
    }
}
