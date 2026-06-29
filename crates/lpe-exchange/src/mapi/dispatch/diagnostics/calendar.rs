use super::super::*;

pub(in crate::mapi::dispatch) fn log_calendar_folder_contract(
    principal: &AccountPrincipal,
    folder_id: u64,
    mailbox_folder_found: bool,
    collaboration_folder_found: bool,
    advertised_special_folder: bool,
    snapshot: &MapiMailStoreSnapshot,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) {
    if folder_id != CALENDAR_FOLDER_ID {
        return;
    }
    let entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        principal.account_id,
        CALENDAR_FOLDER_ID,
    )
    .unwrap_or_default();
    let decoded_entry_id = crate::mapi::identity::object_id_from_folder_entry_id(&entry_id);
    let decoded_entry_id_hex = decoded_entry_id
        .map(|folder_id| format!("0x{folder_id:016x}"))
        .unwrap_or_default();
    let source_key = mapi_mailstore::source_key_for_store_id(CALENDAR_FOLDER_ID);
    let parent_source_key = mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID);
    let calendar_folder = snapshot.collaboration_folder_for_id(CALENDAR_FOLDER_ID);
    let calendar_collection_count = snapshot
        .collaboration_folders()
        .iter()
        .filter(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
        .count();
    let calendar_access = snapshot.folder_access_for_principal(folder_id, principal.account_id);
    log_calendar_identity_chain(
        principal,
        "open_folder",
        folder_id,
        None,
        None,
        Some(snapshot),
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x02",
        folder_id = "0x0000000000100001",
        expected_parent_folder_id = "0x0000000000040001",
        expected_container_class = "IPF.Appointment",
        expected_item_message_class = "IPM.Appointment",
        default_post_message_class_property_tag = "0x36e5001f",
        default_post_message_class_property_name = "PidTagDefaultPostMessageClass",
        default_post_message_class_value = "IPM.Appointment",
        default_post_message_class_projected = true,
        default_post_message_class_projected_in_hierarchy_ics = true,
        default_entry_id_bytes = entry_id.len(),
        default_entry_id_preview = %hex_preview(&entry_id, 24),
        default_entry_id_decoded_folder_id = %decoded_entry_id_hex,
        default_entry_id_decodes_to_calendar = decoded_entry_id == Some(CALENDAR_FOLDER_ID),
        source_key = %bytes_to_hex(&source_key),
        parent_source_key = %bytes_to_hex(&parent_source_key),
        mailbox_folder_found = mailbox_folder_found,
        collaboration_folder_found = collaboration_folder_found,
        advertised_special_folder = advertised_special_folder,
        canonical_calendar_collection_count = calendar_collection_count,
        canonical_calendar_collection_id =
            calendar_folder.map(|folder| folder.collection.id.as_str()).unwrap_or(""),
        canonical_calendar_collection_name =
            calendar_folder.map(|folder| folder.collection.display_name.as_str()).unwrap_or(""),
        canonical_calendar_item_count =
            calendar_folder.map(|folder| folder.item_count).unwrap_or(0),
        projected_calendar_event_count = snapshot.events_for_folder(CALENDAR_FOLDER_ID).len(),
        projected_folder_content_count =
            folder_message_count(folder_id, mailboxes, emails, snapshot),
        mapi_folder_access_mask = %format!("0x{MAPI_FOLDER_ACCESS:08x}"),
        acl_access_row_present = calendar_access.is_some(),
        acl_may_read = calendar_access.map(|access| access.may_read).unwrap_or(true),
        acl_may_write = calendar_access.map(|access| access.may_write).unwrap_or(true),
        acl_may_delete = calendar_access.map(|access| access.may_delete).unwrap_or(true),
        message = "rca debug mapi calendar folder contract"
    );
}

pub(in crate::mapi::dispatch) fn log_calendar_hierarchy_query_rows_contract(
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) {
    let Some(MapiObject::HierarchyTable {
        folder_id, columns, ..
    }) = object
    else {
        return;
    };
    if *folder_id != IPM_SUBTREE_FOLDER_ID && *folder_id != ROOT_FOLDER_ID {
        return;
    }
    let requested_entry_id = columns.contains(&PID_TAG_ENTRY_ID);
    let requested_instance_key = columns.contains(&PID_TAG_INSTANCE_KEY);
    let requested_source_key = columns.contains(&PID_TAG_SOURCE_KEY);
    let requested_folder_id = columns.contains(&PID_TAG_FOLDER_ID);
    let requested_container_class = columns.contains(&PID_TAG_CONTAINER_CLASS_W);
    let requested_default_post_class = columns
        .iter()
        .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W);
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        principal.account_id,
        CALENDAR_FOLDER_ID,
    )
    .unwrap_or_default();
    let calendar_source_key = mapi_mailstore::source_key_for_store_id(CALENDAR_FOLDER_ID);
    let decoded_entry_id =
        crate::mapi::identity::object_id_from_folder_entry_id(&calendar_entry_id);
    let decoded_source_key = crate::mapi::identity::object_id_from_source_key(&calendar_source_key);
    let exact_property_set_can_reopen_calendar = requested_entry_id
        && requested_source_key
        && requested_folder_id
        && decoded_entry_id == Some(CALENDAR_FOLDER_ID)
        && decoded_source_key == Some(CALENDAR_FOLDER_ID);
    let calendar_row_in_scope = *folder_id == IPM_SUBTREE_FOLDER_ID;
    let calendar_folder = snapshot.collaboration_folder_for_id(CALENDAR_FOLDER_ID);

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x15",
        hierarchy_root_folder_id = %format!("0x{folder_id:016x}"),
        hierarchy_root_role = debug_role_for_folder_id(*folder_id),
        requested_property_tag_count = columns.len(),
        requested_property_tags = %format_debug_property_tags(columns),
        requested_entry_id,
        requested_instance_key,
        requested_source_key,
        requested_folder_id,
        requested_container_class,
        requested_default_post_class,
        calendar_row_in_scope,
        calendar_row_projected = calendar_row_in_scope,
        calendar_canonical_collection_present = calendar_folder.is_some(),
        calendar_entry_id_bytes = calendar_entry_id.len(),
        calendar_entry_id_preview = %hex_preview(&calendar_entry_id, 24),
        calendar_entry_id_decoded_folder_id =
            %format_optional_folder_id(decoded_entry_id),
        calendar_entry_id_decodes_to_calendar =
            decoded_entry_id == Some(CALENDAR_FOLDER_ID),
        calendar_source_key = %bytes_to_hex(&calendar_source_key),
        calendar_source_key_decoded_folder_id =
            %format_optional_folder_id(decoded_source_key),
        calendar_source_key_decodes_to_calendar =
            decoded_source_key == Some(CALENDAR_FOLDER_ID),
        calendar_folder_id_property_value = %format!("0x{CALENDAR_FOLDER_ID:016x}"),
        exact_property_set_can_reopen_calendar,
        expected_container_class = "IPF.Appointment",
        expected_default_post_message_class = "IPM.Appointment",
        "rca debug mapi calendar hierarchy query requested property contract"
    );
}

pub(in crate::mapi::dispatch) fn log_calendar_identity_chain(
    principal: &AccountPrincipal,
    stage: &str,
    observed_folder_id: u64,
    checkpoint_mailbox_id: Option<Uuid>,
    sync_type: Option<u8>,
    snapshot: Option<&MapiMailStoreSnapshot>,
) {
    if observed_folder_id != CALENDAR_FOLDER_ID {
        return;
    }
    let default_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        principal.account_id,
        CALENDAR_FOLDER_ID,
    )
    .unwrap_or_default();
    let default_entry_id_decoded =
        crate::mapi::identity::object_id_from_folder_entry_id(&default_entry_id);
    let source_key = mapi_mailstore::source_key_for_store_id(CALENDAR_FOLDER_ID);
    let source_key_decoded = crate::mapi::identity::object_id_from_source_key(&source_key);
    let parent_source_key = mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID);
    let parent_source_key_decoded =
        crate::mapi::identity::object_id_from_source_key(&parent_source_key);
    let expected_checkpoint_mailbox_id =
        mapi_mailstore::virtual_special_mailbox(CALENDAR_FOLDER_ID).map(|mailbox| mailbox.id);
    let checkpoint_mailbox_id_matches_expected =
        checkpoint_mailbox_id.is_some() && checkpoint_mailbox_id == expected_checkpoint_mailbox_id;
    let checkpoint_identity_ok =
        checkpoint_mailbox_id.is_none() || checkpoint_mailbox_id_matches_expected;
    let calendar_identity_chain_complete = default_entry_id_decoded == Some(CALENDAR_FOLDER_ID)
        && source_key_decoded == Some(CALENDAR_FOLDER_ID)
        && parent_source_key_decoded == Some(IPM_SUBTREE_FOLDER_ID)
        && checkpoint_identity_ok;
    let calendar_folder =
        snapshot.and_then(|snapshot| snapshot.collaboration_folder_for_id(CALENDAR_FOLDER_ID));
    let projected_calendar_event_count = snapshot
        .map(|snapshot| snapshot.events_for_folder(CALENDAR_FOLDER_ID).len())
        .unwrap_or_default();
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        stage = stage,
        default_folder_property_tag = "0x36d00102",
        default_folder_property_name = "PidTagIpmAppointmentEntryId",
        default_entry_id_bytes = default_entry_id.len(),
        default_entry_id_preview = %hex_preview(&default_entry_id, 24),
        default_entry_id_decoded_folder_id =
            %format_optional_folder_id(default_entry_id_decoded),
        observed_folder_id = %format!("0x{observed_folder_id:016x}"),
        source_key = %bytes_to_hex(&source_key),
        source_key_decoded_folder_id = %format_optional_folder_id(source_key_decoded),
        parent_source_key = %bytes_to_hex(&parent_source_key),
        parent_source_key_decoded_folder_id =
            %format_optional_folder_id(parent_source_key_decoded),
        replica_guid = %bytes_to_hex(&crate::mapi::identity::STORE_REPLICA_GUID),
        replid = 1u16,
        sync_type = %sync_type.map(|value| format!("0x{value:02x}")).unwrap_or_default(),
        checkpoint_mailbox_id = %checkpoint_mailbox_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        expected_checkpoint_mailbox_id = %expected_checkpoint_mailbox_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        checkpoint_mailbox_id_matches_expected,
        checkpoint_identity_ok,
        canonical_calendar_collection_present = calendar_folder.is_some(),
        canonical_calendar_collection_id =
            calendar_folder.map(|folder| folder.collection.id.as_str()).unwrap_or(""),
        projected_calendar_event_count,
        calendar_identity_chain_complete,
        calendar_identity_chain_key = %format!(
            "entry={};open=0x{observed_folder_id:016x};source={};parent={};checkpoint={}",
            format_optional_folder_id(default_entry_id_decoded),
            format_optional_folder_id(source_key_decoded),
            format_optional_folder_id(parent_source_key_decoded),
            checkpoint_mailbox_id.map(|id| id.to_string()).unwrap_or_default(),
        ),
        message = "rca debug mapi calendar identity chain"
    );
}

pub(in crate::mapi::dispatch) fn format_calendar_required_property_tags(
    has_configuration_objects: bool,
    has_appointment_objects: bool,
) -> String {
    let mut tags = Vec::new();
    if has_configuration_objects {
        tags.extend([
            PID_TAG_ROAMING_DATATYPES,
            PID_TAG_ROAMING_DICTIONARY,
            PID_TAG_ROAMING_XML_STREAM,
        ]);
    }
    if has_appointment_objects {
        tags.extend([
            PID_TAG_START_DATE,
            PID_TAG_END_DATE,
            PID_LID_COMMON_START_TAG,
            PID_LID_COMMON_END_TAG,
            PID_LID_BUSY_STATUS_TAG,
            PID_LID_LOCATION_W_TAG,
            PID_LID_APPOINTMENT_START_WHOLE_TAG,
            PID_LID_APPOINTMENT_END_WHOLE_TAG,
            PID_LID_APPOINTMENT_DURATION_TAG,
            PID_LID_TIME_ZONE_STRUCT_TAG,
            PID_LID_TIME_ZONE_DESCRIPTION_W_TAG,
            PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG,
            PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG,
            PID_LID_GLOBAL_OBJECT_ID_TAG,
            PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG,
        ]);
    }
    format_debug_property_tags(&tags)
}
