use super::*;

pub(super) fn private_create_folder_is_existing_response_flag() -> bool {
    false
}

pub(super) async fn hard_delete_folder_contents<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<(Vec<u64>, bool), u32> {
    let mailbox = role_for_folder_id(folder_id)
        .and_then(|role| mailboxes.iter().find(|mailbox| mailbox.role == role))
        .or_else(|| {
            mailboxes.iter().find(|mailbox| {
                crate::mapi::identity::mapped_mapi_object_id(&mailbox.id) == Some(folder_id)
            })
        })
        .ok_or(0x8004_010Fu32)?;

    if !snapshot
        .folder_access_for_principal(folder_id, principal.account_id)
        .map(|access| access.may_delete)
        .unwrap_or(true)
    {
        return Err(0x8007_0005);
    }

    let mut partial_completion = false;
    let mut changed_folder_ids = Vec::new();
    let message_ids = emails
        .iter()
        .filter(|email| email_matches_folder(email, folder_id, mailboxes))
        .map(|email| email.id)
        .collect::<Vec<_>>();
    let attempted_count = message_ids.len();
    let mut succeeded_count = 0usize;
    let mut failed_count = 0usize;

    for message_id in message_ids {
        if store
            .delete_jmap_email_from_mailbox(
                principal.account_id,
                mailbox.id,
                message_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-hard-delete-folder-contents".to_string(),
                    subject: format!("folder:{} message:{}", mailbox.id, message_id),
                },
            )
            .await
            .is_err()
        {
            partial_completion = true;
            failed_count += 1;
        } else {
            if changed_folder_ids.is_empty() {
                changed_folder_ids.push(folder_id);
            }
            succeeded_count += 1;
        }
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        mailbox = %principal.email,
        folder_id = %format!("{folder_id:#018x}"),
        folder_role = debug_role_for_folder_id(folder_id),
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
        message = "rca debug mapi hard delete folder contents"
    );
    record_mapi_folder_purge_metrics(
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
    );
    Ok((changed_folder_ids, partial_completion))
}

pub(super) async fn hard_delete_mailbox_tree_contents<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<(Vec<u64>, bool), u32> {
    let root_mailbox = role_for_folder_id(folder_id)
        .and_then(|role| mailboxes.iter().find(|mailbox| mailbox.role == role))
        .or_else(|| {
            mailboxes.iter().find(|mailbox| {
                crate::mapi::identity::mapped_mapi_object_id(&mailbox.id) == Some(folder_id)
            })
        })
        .ok_or(0x8004_010Fu32)?;

    let mut target_mailboxes = Vec::new();
    for mailbox in mailboxes {
        let mut current = Some(mailbox.id);
        let mut visited = HashSet::new();
        while let Some(current_id) = current {
            if current_id == root_mailbox.id {
                target_mailboxes.push(mailbox);
                break;
            }
            if !visited.insert(current_id) {
                break;
            }
            current = mailboxes
                .iter()
                .find(|candidate| candidate.id == current_id)
                .and_then(|candidate| candidate.parent_id);
        }
    }

    let target_folder_ids = target_mailboxes
        .iter()
        .map(|mailbox| {
            (
                crate::mapi::identity::mapped_mapi_object_id(&mailbox.id)
                    .unwrap_or_else(|| mapi_folder_id(mailbox)),
                mailbox.id,
            )
        })
        .collect::<Vec<_>>();

    for (target_folder_id, _) in &target_folder_ids {
        if !snapshot
            .folder_access_for_principal(*target_folder_id, principal.account_id)
            .map(|access| access.may_delete)
            .unwrap_or(true)
        {
            return Err(0x8007_0005);
        }
    }

    let mut partial_completion = false;
    let mut changed_folder_ids = Vec::new();
    let mut attempted_count = 0usize;
    let mut succeeded_count = 0usize;
    let mut failed_count = 0usize;
    for (target_folder_id, mailbox_id) in target_folder_ids {
        let message_ids = emails
            .iter()
            .filter(|email| email_matches_folder(email, target_folder_id, mailboxes))
            .map(|email| email.id)
            .collect::<Vec<_>>();
        attempted_count += message_ids.len();
        for message_id in message_ids {
            if store
                .delete_jmap_email_from_mailbox(
                    principal.account_id,
                    mailbox_id,
                    message_id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-hard-delete-folder-tree-contents".to_string(),
                        subject: format!("folder:{mailbox_id} message:{message_id}"),
                    },
                )
                .await
                .is_err()
            {
                partial_completion = true;
                failed_count += 1;
            } else {
                if !changed_folder_ids.contains(&target_folder_id) {
                    changed_folder_ids.push(target_folder_id);
                }
                succeeded_count += 1;
            }
        }
    }
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        mailbox = %principal.email,
        folder_id = %format!("{folder_id:#018x}"),
        folder_role = debug_role_for_folder_id(folder_id),
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
        message = "rca debug mapi hard delete folder tree contents"
    );
    record_mapi_folder_purge_metrics(
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
    );
    Ok((changed_folder_ids, partial_completion))
}

pub(super) fn collaboration_folder_handle_properties(
    folder: &crate::mapi_store::MapiCollaborationFolder,
) -> HashMap<u32, MapiValue> {
    [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ENTRY_ID,
        PID_TAG_RECORD_KEY,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_FOLDER_ID,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_DELETED_COUNT_TOTAL,
        PID_TAG_SUBFOLDERS,
        PID_TAG_ACCESS,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        PID_TAG_DEFAULT_VIEW_ENTRY_ID,
        PID_TAG_FOLDER_FORM_FLAGS,
        PID_TAG_FOLDER_WEBVIEWINFO,
        PID_TAG_FOLDER_XVIEWINFO_E,
        PID_TAG_FOLDER_VIEWS_ONLY,
        PID_TAG_DEFAULT_FORM_NAME_W,
        PID_TAG_FOLDER_FORM_STORAGE,
        PID_TAG_ACL_MEMBER_NAME_W,
        PID_TAG_FOLDER_VIEWLIST_FLAGS,
        PID_TAG_ARCHIVE_TAG,
        PID_TAG_POLICY_TAG,
        PID_TAG_RETENTION_PERIOD,
        PID_TAG_RETENTION_FLAGS,
        PID_TAG_ARCHIVE_PERIOD,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_TAG_LOCAL_COMMIT_TIME_MAX,
        PID_TAG_HIER_REV,
        PID_TAG_HIERARCHY_CHANGE_NUMBER,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ]
    .into_iter()
    .filter_map(|tag| collaboration_folder_property_value(folder, tag).map(|value| (tag, value)))
    .collect()
}

pub(super) fn create_folder_existing_mailbox_satisfies_deleted_advertised_request(
    session: &MapiSession,
    parent_folder_id: u64,
    display_name: &str,
) -> bool {
    advertised_special_folder_id_for_create(parent_folder_id, display_name)
        .map(|folder_id| session.advertised_special_folder_was_deleted(folder_id))
        .unwrap_or(false)
}

pub(super) fn advertised_special_folder_delete_uses_session_tombstone(folder_id: u64) -> bool {
    folder_id == QUICK_STEP_SETTINGS_FOLDER_ID
}

pub(super) fn advertised_special_folder_delete_is_noop(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CONTACTS_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
    )
}

pub(super) fn synthetic_folder_allows_create_message(folder_id: u64) -> bool {
    matches!(
        folder_id,
        INBOX_FOLDER_ID
            | DRAFTS_FOLDER_ID
            | SENT_FOLDER_ID
            | TRASH_FOLDER_ID
            | OUTBOX_FOLDER_ID
            | NOTES_FOLDER_ID
            | JOURNAL_FOLDER_ID
            | FREEBUSY_DATA_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
            | QUICK_STEP_SETTINGS_FOLDER_ID
    )
}

pub(super) fn advertised_special_folder_container_class(folder_id: u64) -> Option<&'static str> {
    role_for_folder_id(folder_id)?;
    Some(match folder_id {
        CALENDAR_FOLDER_ID => "IPF.Appointment",
        CONTACTS_FOLDER_ID | SUGGESTED_CONTACTS_FOLDER_ID | CONTACTS_SEARCH_FOLDER_ID => {
            "IPF.Contact"
        }
        QUICK_CONTACTS_FOLDER_ID => "IPF.Contact.MOC.QuickContacts",
        IM_CONTACT_LIST_FOLDER_ID => "IPF.Contact.MOC.ImContactList",
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => "IPF.Task",
        NOTES_FOLDER_ID => "IPF.StickyNote",
        JOURNAL_FOLDER_ID => "IPF.Journal",
        RSS_FEEDS_FOLDER_ID => "IPF.Note.OutlookHomepage",
        _ => "IPF.Note",
    })
}

pub(super) async fn folder_properties_for_open<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> HashMap<u32, MapiValue>
where
    S: ExchangeStore,
{
    let mut properties =
        folder_properties_for_open_from_mailboxes(principal, folder_id, mailboxes, snapshot);
    if !session.search_folder_definition_was_deleted(folder_id) {
        if let Some(definition) = session.search_folder_definition(folder_id) {
            properties.extend(search_folder_handle_properties(
                definition,
                folder_id,
                principal.account_id,
            ));
        }
    }
    if folder_id == IPM_SUBTREE_FOLDER_ID {
        if let Ok(Some(ost_id)) = store
            .fetch_mapi_ipm_subtree_ost_id(principal.account_id)
            .await
        {
            properties.insert(PID_TAG_OST_OSTID, MapiValue::Binary(ost_id));
        }
    }
    if let Ok(values) = store
        .fetch_mapi_folder_profile_property_values(
            principal.account_id,
            folder_id,
            &[PID_TAG_EXTENDED_FOLDER_FLAGS],
        )
        .await
    {
        for value in values {
            if value.property_tag == PID_TAG_EXTENDED_FOLDER_FLAGS {
                properties.insert(
                    PID_TAG_EXTENDED_FOLDER_FLAGS,
                    MapiValue::Binary(value.property_value),
                );
            }
        }
    }
    properties
}

pub(super) fn folder_properties_for_open_from_mailboxes(
    principal: &AccountPrincipal,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
) -> HashMap<u32, MapiValue> {
    let mut properties = HashMap::new();
    let open_folder_property_tags = [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_ENTRY_ID,
        PID_TAG_RECORD_KEY,
        PID_TAG_INSTANCE_KEY,
        PID_TAG_FOLDER_ID,
        PID_TAG_PARENT_FOLDER_ID,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_ASSOCIATED_CONTENT_COUNT,
        PID_TAG_DELETED_COUNT_TOTAL,
        PID_TAG_SUBFOLDERS,
        PID_TAG_ACCESS,
        PID_TAG_RIGHTS,
        PID_TAG_EXTENDED_FOLDER_FLAGS,
        PID_TAG_DEFAULT_VIEW_ENTRY_ID,
        PID_TAG_FOLDER_FORM_FLAGS,
        PID_TAG_FOLDER_WEBVIEWINFO,
        PID_TAG_FOLDER_XVIEWINFO_E,
        PID_TAG_FOLDER_VIEWS_ONLY,
        PID_TAG_DEFAULT_FORM_NAME_W,
        PID_TAG_FOLDER_FORM_STORAGE,
        PID_TAG_ACL_MEMBER_NAME_W,
        PID_TAG_FOLDER_VIEWLIST_FLAGS,
        PID_TAG_ARCHIVE_TAG,
        PID_TAG_POLICY_TAG,
        PID_TAG_RETENTION_PERIOD,
        PID_TAG_RETENTION_FLAGS,
        PID_TAG_ARCHIVE_PERIOD,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_TAG_LOCAL_COMMIT_TIME_MAX,
        PID_TAG_HIER_REV,
        PID_TAG_HIERARCHY_CHANGE_NUMBER,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_CHANGE_NUMBER,
    ];
    let mailbox = folder_row_for_id(folder_id, mailboxes);
    if let Some(mailbox) = mailbox {
        for property_tag in open_folder_property_tags {
            if let Some(value) = mailbox_property_value_with_context_for_account(
                mailbox,
                mailboxes,
                property_tag,
                principal.account_id,
            ) {
                properties.insert(property_tag, value);
            }
        }
    }
    if let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) {
        properties.extend(collaboration_folder_handle_properties(folder));
    }
    if let Some(folder) = snapshot.public_folder_for_id(folder_id) {
        for property_tag in open_folder_property_tags {
            if let Some(value) = public_folder_property_value(folder, property_tag) {
                properties.insert(property_tag, value);
            }
        }
    }
    if let Some(definition) = snapshot.search_folder_definition_for_folder_id(folder_id) {
        properties.extend(search_folder_handle_properties(
            definition,
            folder_id,
            principal.account_id,
        ));
    }
    if is_advertised_special_folder(folder_id) {
        for property_tag in open_folder_property_tags {
            if property_tag == PID_TAG_PARENT_SOURCE_KEY
                && matches!(folder_id, ROOT_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID)
            {
                continue;
            }
            if !properties.contains_key(&property_tag) {
                if let Some(value) =
                    special_folder_property_value(folder_id, property_tag, principal.account_id)
                {
                    properties.insert(property_tag, value);
                }
            }
        }
    }
    if mailbox.is_none() && is_advertised_special_folder(folder_id) {
        let (content_count, unread_count) = snapshot_message_counts_for_folder(snapshot, folder_id);
        properties.insert(PID_TAG_CONTENT_COUNT, MapiValue::U32(content_count));
        properties.insert(PID_TAG_CONTENT_UNREAD_COUNT, MapiValue::U32(unread_count));
    }
    if folder_id == INBOX_FOLDER_ID {
        if let Some(value) =
            special_folder_property_value(folder_id, PID_TAG_DISPLAY_NAME_W, principal.account_id)
        {
            properties.insert(PID_TAG_DISPLAY_NAME_W, value);
        }
    }
    properties.insert(
        PID_TAG_ASSOCIATED_CONTENT_COUNT,
        MapiValue::U32(associated_folder_message_count(folder_id, snapshot)),
    );
    properties
}

pub(super) fn folder_local_default_named_view_is_supported(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> bool {
    snapshot
        .default_folder_named_view_message(folder_id, message_id)
        .is_some_and(|_| {
            let container_class = snapshot
                .collaboration_folder_for_id(folder_id)
                .map(|folder| collaboration_folder_message_class(folder.kind))
                .or_else(|| advertised_special_folder_container_class(folder_id));
            container_class.is_some_and(|container_class| {
                default_view_supported_folder(folder_id, container_class)
            })
        })
}

pub(super) fn snapshot_message_counts_for_folder(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
) -> (u32, u32) {
    let emails = snapshot.emails();
    let count = folder_message_count(folder_id, &[], &emails, snapshot);
    let unread = emails
        .iter()
        .filter(|email| snapshot_email_belongs_to_folder(email, folder_id) && email.unread)
        .count();
    (count, unread.min(u32::MAX as usize) as u32)
}

fn snapshot_email_belongs_to_folder(email: &JmapEmail, folder_id: u64) -> bool {
    email_role_folder_id(&email.mailbox_role) == Some(folder_id)
        || email
            .mailbox_states
            .iter()
            .any(|state| email_role_folder_id(&state.role) == Some(folder_id))
}

fn email_role_folder_id(role: &str) -> Option<u64> {
    crate::mapi_store::reserved_folder_counter_for_role(role)
        .map(crate::mapi::identity::mapi_store_id)
}

pub(super) fn mailbox_parent_folder_id_for_dispatch(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
) -> u64 {
    if mailbox.role == "__mapi_collaboration_calendar" {
        return IPM_SUBTREE_FOLDER_ID;
    }
    mailbox
        .parent_id
        .and_then(|parent_id| mailboxes.iter().find(|candidate| candidate.id == parent_id))
        .map(mapi_folder_id)
        .unwrap_or(IPM_SUBTREE_FOLDER_ID)
}

pub(super) fn mailbox_is_trash_or_descendant(mailbox_id: Uuid, mailboxes: &[JmapMailbox]) -> bool {
    let mut current = Some(mailbox_id);
    let mut visited = HashSet::new();
    while let Some(id) = current {
        if !visited.insert(id) {
            return false;
        }
        let Some(mailbox) = mailboxes.iter().find(|candidate| candidate.id == id) else {
            return false;
        };
        if mailbox.role == "trash" {
            return true;
        }
        current = mailbox.parent_id;
    }
    false
}
