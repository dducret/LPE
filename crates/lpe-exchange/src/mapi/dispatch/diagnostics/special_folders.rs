use super::super::*;

pub(in crate::mapi::dispatch) fn log_special_folder_contract(
    principal: &AccountPrincipal,
    request_id: &str,
    folder_id: u64,
    mailbox_folder_found: bool,
    collaboration_folder_found: bool,
    advertised_special_folder: bool,
    snapshot: &MapiMailStoreSnapshot,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) {
    if folder_id == CALENDAR_FOLDER_ID || !is_rca_special_contract_folder(folder_id) {
        return;
    }
    let entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(principal.account_id, folder_id)
            .unwrap_or_default();
    let decoded_entry_id = crate::mapi::identity::object_id_from_folder_entry_id(&entry_id);
    let decoded_entry_id_hex = decoded_entry_id
        .map(|folder_id| format!("0x{folder_id:016x}"))
        .unwrap_or_default();
    let source_key = mapi_mailstore::source_key_for_store_id(folder_id);
    let expected_parent_folder_id = expected_special_folder_parent_id(folder_id);
    let parent_source_key = mapi_mailstore::source_key_for_store_id(expected_parent_folder_id);
    let collaboration_folder = snapshot.collaboration_folder_for_id(folder_id);
    let canonical_collection_kind = collaboration_folder
        .map(|folder| format!("{:?}", folder.kind))
        .unwrap_or_default();
    let folder_access = snapshot.folder_access_for_principal(folder_id, principal.account_id);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x02",
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = role_for_folder_id(folder_id).unwrap_or(""),
        expected_parent_folder_id = %format!("0x{expected_parent_folder_id:016x}"),
        expected_container_class = expected_special_folder_container_class(folder_id),
        expected_item_message_class = expected_special_folder_item_message_class(folder_id),
        default_entry_id_bytes = entry_id.len(),
        default_entry_id_preview = %hex_preview(&entry_id, 24),
        default_entry_id_decoded_folder_id = %decoded_entry_id_hex,
        default_entry_id_matches_requested_folder = decoded_entry_id == Some(folder_id),
        source_key = %bytes_to_hex(&source_key),
        parent_source_key = %bytes_to_hex(&parent_source_key),
        mailbox_folder_found = mailbox_folder_found,
        collaboration_folder_found = collaboration_folder_found,
        advertised_special_folder = advertised_special_folder,
        canonical_collection_present = collaboration_folder.is_some(),
        canonical_collection_kind = %canonical_collection_kind,
        canonical_collection_id =
            collaboration_folder.map(|folder| folder.collection.id.as_str()).unwrap_or(""),
        canonical_collection_name =
            collaboration_folder.map(|folder| folder.collection.display_name.as_str()).unwrap_or(""),
        canonical_item_count = collaboration_folder.map(|folder| folder.item_count).unwrap_or(0),
        projected_special_object_count =
            special_sync_objects_for(folder_id, 0x01, snapshot, principal).len(),
        projected_folder_content_count =
            folder_message_count(folder_id, mailboxes, emails, snapshot),
        mapi_folder_access_mask = %format!("0x{MAPI_FOLDER_ACCESS:08x}"),
        acl_access_row_present = folder_access.is_some(),
        acl_may_read = folder_access.map(|access| access.may_read).unwrap_or(true),
        acl_may_write = folder_access.map(|access| access.may_write).unwrap_or(true),
        acl_may_delete = folder_access.map(|access| access.may_delete).unwrap_or(true),
        message = "rca debug mapi special folder contract"
    );
}

pub(in crate::mapi::dispatch) fn log_calendar_special_sync_objects(
    principal: &AccountPrincipal,
    request_id: &str,
    folder_id: u64,
    sync_type: u8,
    sync_flags: u16,
    sync_extra_flags: u32,
    sync_property_tags: &[u32],
    objects: &[mapi_mailstore::SpecialMessageSyncFact],
) {
    if folder_id != CALENDAR_FOLDER_ID || sync_type != 0x01 {
        return;
    }
    let item_ids = objects
        .iter()
        .map(|object| format!("0x{:016x}", object.item_id))
        .collect::<Vec<_>>()
        .join(",");
    let source_keys = objects
        .iter()
        .map(|object| bytes_to_hex(&mapi_mailstore::special_message_source_key(object)))
        .collect::<Vec<_>>()
        .join(",");
    let canonical_ids = objects
        .iter()
        .map(|object| object.canonical_id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let message_classes = objects
        .iter()
        .map(|object| object.message_class.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let subject_lengths = objects
        .iter()
        .map(|object| object.subject.chars().count().to_string())
        .collect::<Vec<_>>()
        .join(",");
    let body_lengths = objects
        .iter()
        .map(|object| {
            object.body_text.as_deref().map_or_else(
                || "absent".to_string(),
                |body_text| body_text.chars().count().to_string(),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let message_sizes = objects
        .iter()
        .map(|object| object.message_size.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let property_tag_count = objects
        .iter()
        .map(|object| object.named_properties.len())
        .sum::<usize>();
    let property_tags = objects
        .iter()
        .flat_map(|object| object.named_properties.iter().map(|(tag, _)| *tag))
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",");
    let property_shapes = objects
        .iter()
        .flat_map(|object| {
            object
                .named_properties
                .iter()
                .map(|(tag, value)| format!("0x{tag:08x}:{}", special_property_shape(value)))
        })
        .collect::<Vec<_>>()
        .join(",");
    let configuration_objects = objects
        .iter()
        .filter(|object| is_calendar_configuration_object(object))
        .collect::<Vec<_>>();
    let dictionary_configuration_objects = configuration_objects
        .iter()
        .filter(|object| {
            crate::mapi_store::is_outlook_configuration_message_class_name(
                &object.message_class,
                "IPM.Configuration.Calendar",
            ) && special_binary_property_len(object, PID_TAG_ROAMING_DICTIONARY).is_some()
        })
        .count();
    let xml_configuration_objects = configuration_objects
        .iter()
        .filter(|object| special_binary_property_len(object, PID_TAG_ROAMING_XML_STREAM).is_some())
        .count();
    let appointment_objects = objects
        .iter()
        .filter(|object| object.message_class == "IPM.Appointment")
        .collect::<Vec<_>>();
    let configuration_required_tags = [
        PID_TAG_ROAMING_DATATYPES,
        PID_TAG_ROAMING_DICTIONARY,
        PID_TAG_ROAMING_XML_STREAM,
    ];
    let appointment_required_tags = [
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
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
    ];
    let sync_property_tags = format_debug_property_tags(sync_property_tags);
    let sync_property_filter_mode = if sync_property_tags.is_empty() {
        "all-properties"
    } else if sync_flags & 0x0080 != 0 {
        "only-specified-properties"
    } else {
        "except-specified-properties"
    };
    for object in &appointment_objects {
        let source_key = mapi_mailstore::special_message_sync_source_key(object, sync_flags);
        let parent_source_key =
            mapi_mailstore::special_message_sync_parent_source_key(object, sync_flags);
        let change_key = mapi_mailstore::special_message_change_key(object);
        let predecessor_change_list =
            mapi_mailstore::special_message_predecessor_change_list(object);
        let entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
            principal.account_id,
            object.folder_id,
            object.item_id,
        );
        let entry_id_target = entry_id.as_deref().and_then(|entry_id| {
            crate::mapi::identity::object_ids_from_message_entry_id(principal.account_id, entry_id)
        });
        let parent_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
            principal.account_id,
            object.folder_id,
        );
        let parent_entry_id_target = parent_entry_id
            .as_deref()
            .and_then(crate::mapi::identity::object_id_from_folder_entry_id);
        let property_tags = object
            .named_properties
            .iter()
            .map(|(tag, _)| format!("0x{tag:08x}"))
            .collect::<Vec<_>>()
            .join(",");
        let property_shapes = object
            .named_properties
            .iter()
            .map(|(tag, value)| format!("0x{tag:08x}:{}", special_property_shape(value)))
            .collect::<Vec<_>>()
            .join(",");
        let mut named_property_definitions = object
            .named_property_definitions
            .iter()
            .map(|(property_id, property)| match &property.kind {
                crate::mapi::properties::MapiNamedPropertyKind::Lid(lid) => format!(
                    "0x{property_id:04x}:guid={},lid=0x{lid:08x}",
                    bytes_to_hex(&property.guid),
                ),
                crate::mapi::properties::MapiNamedPropertyKind::Name(_) => format!(
                    "0x{property_id:04x}:guid={},name=redacted",
                    bytes_to_hex(&property.guid),
                ),
            })
            .collect::<Vec<_>>();
        named_property_definitions.sort_unstable();
        let named_property_definitions = named_property_definitions.join(",");
        let missing_required_property_tags = appointment_required_tags
            .iter()
            .filter(|tag| {
                !object
                    .named_properties
                    .iter()
                    .any(|(present, _)| present == *tag)
            })
            .map(|tag| format!("0x{tag:08x}"))
            .collect::<Vec<_>>()
            .join(",");
        let (global_object_id_contract, global_object_id_contract_ok) =
            calendar_global_object_id_contract(object, PID_LID_GLOBAL_OBJECT_ID_TAG);
        let (clean_global_object_id_contract, clean_global_object_id_contract_ok) =
            calendar_global_object_id_contract(object, PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_rop_id = "0x70",
            folder_id = %format!("0x{folder_id:016x}"),
            sync_type = "0x01",
            calendar_item_id = %format!("0x{:016x}", object.item_id),
            calendar_canonical_id = %object.canonical_id,
            calendar_message_class = %object.message_class,
            calendar_subject_char_count = object.subject.chars().count(),
            calendar_body_char_count = object.body_text.as_deref().map(str::chars).map(Iterator::count),
            calendar_message_size = object.message_size,
            calendar_last_modified_filetime = object.last_modified_filetime,
            calendar_sync_flags = %format!("0x{sync_flags:04x}"),
            calendar_sync_extra_flags = %format!("0x{sync_extra_flags:08x}"),
            calendar_sync_property_filter_mode = sync_property_filter_mode,
            calendar_sync_property_tags = %sync_property_tags,
            calendar_projected_source_key = %bytes_to_hex(&source_key),
            calendar_projected_parent_source_key = %bytes_to_hex(&parent_source_key),
            calendar_projected_change_number = mapi_mailstore::special_message_change_number(object),
            calendar_projected_change_key_bytes = change_key.len(),
            calendar_projected_change_key_preview = %hex_preview(&change_key, 48),
            calendar_projected_predecessor_change_list_bytes = predecessor_change_list.len(),
            calendar_projected_predecessor_change_list_preview = %hex_preview(&predecessor_change_list, 48),
            calendar_projected_entry_id_bytes = entry_id.as_ref().map_or(0, Vec::len),
            calendar_projected_entry_id_preview = %entry_id.as_deref().map(|value| hex_preview(value, 48)).unwrap_or_default(),
            calendar_projected_entry_id_target_folder_id = %entry_id_target.map(|(folder_id, _)| format!("0x{folder_id:016x}")).unwrap_or_default(),
            calendar_projected_entry_id_target_item_id = %entry_id_target.map(|(_, item_id)| format!("0x{item_id:016x}")).unwrap_or_default(),
            calendar_projected_entry_id_round_trip_matches = entry_id_target == Some((object.folder_id, object.item_id)),
            calendar_projected_parent_entry_id_bytes = parent_entry_id.as_ref().map_or(0, Vec::len),
            calendar_projected_parent_entry_id_preview = %parent_entry_id.as_deref().map(|value| hex_preview(value, 48)).unwrap_or_default(),
            calendar_projected_parent_entry_id_target_folder_id = %parent_entry_id_target.map(|folder_id| format!("0x{folder_id:016x}")).unwrap_or_default(),
            calendar_projected_parent_entry_id_matches = parent_entry_id_target == Some(object.folder_id),
            calendar_projected_instance_key = %bytes_to_hex(&crate::mapi::identity::instance_key_for_object_id(object.item_id)),
            calendar_property_count = object.named_properties.len(),
            calendar_property_tags = %property_tags,
            calendar_property_shapes = %property_shapes,
            calendar_named_property_definitions = %named_property_definitions,
            calendar_missing_required_property_tags = %missing_required_property_tags,
            calendar_required_properties_complete = missing_required_property_tags.is_empty(),
            calendar_start_end_order_ok = calendar_sync_object_start_end_order_ok(object),
            calendar_global_object_id_bytes = special_binary_property_len(object, PID_LID_GLOBAL_OBJECT_ID_TAG).unwrap_or_default(),
            calendar_global_object_id_third_party_contract = global_object_id_contract,
            calendar_global_object_id_third_party_contract_ok = global_object_id_contract_ok,
            calendar_clean_global_object_id_bytes = special_binary_property_len(object, PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG).unwrap_or_default(),
            calendar_clean_global_object_id_third_party_contract = clean_global_object_id_contract,
            calendar_clean_global_object_id_third_party_contract_ok = clean_global_object_id_contract_ok,
            message = "rca debug mapi calendar FastTransfer appointment projection"
        );
    }
    let missing_configuration_tags = if configuration_objects.is_empty() {
        String::new()
    } else {
        configuration_required_tags
            .iter()
            .copied()
            .filter(|tag| {
                !configuration_objects.iter().any(|object| match *tag {
                    PID_TAG_ROAMING_DICTIONARY => {
                        crate::mapi_store::is_outlook_configuration_message_class_name(
                            &object.message_class,
                            "IPM.Configuration.Calendar",
                        ) && object
                            .named_properties
                            .iter()
                            .any(|(present, _)| present == tag)
                    }
                    PID_TAG_ROAMING_XML_STREAM => {
                        (crate::mapi_store::is_outlook_configuration_message_class_name(
                            &object.message_class,
                            "IPM.Configuration.CategoryList",
                        ) || crate::mapi_store::is_outlook_configuration_message_class_name(
                            &object.message_class,
                            "IPM.Configuration.WorkHours",
                        )) && object
                            .named_properties
                            .iter()
                            .any(|(present, _)| present == tag)
                    }
                    _ => object
                        .named_properties
                        .iter()
                        .any(|(present, _)| present == tag),
                })
            })
            .map(|tag| format!("0x{tag:08x}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let missing_appointment_tags = if appointment_objects.is_empty() {
        String::new()
    } else {
        appointment_required_tags
            .iter()
            .copied()
            .filter(|tag| {
                !appointment_objects.iter().any(|object| {
                    object
                        .named_properties
                        .iter()
                        .any(|(present, _)| present == tag)
                })
            })
            .map(|tag| format!("0x{tag:08x}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let missing_required_tags = [
        missing_configuration_tags.as_str(),
        missing_appointment_tags.as_str(),
    ]
    .into_iter()
    .filter(|tags| !tags.is_empty())
    .collect::<Vec<_>>()
    .join(",");
    let start_end_order_ok = appointment_objects
        .iter()
        .all(|object| calendar_sync_object_start_end_order_ok(object));
    let global_object_id_lengths = appointment_objects
        .iter()
        .map(|object| {
            special_binary_property_len(object, PID_LID_GLOBAL_OBJECT_ID_TAG)
                .map(|len| len.to_string())
                .unwrap_or_else(|| "missing".to_string())
        })
        .collect::<Vec<_>>()
        .join(",");
    let clean_global_object_id_lengths = appointment_objects
        .iter()
        .map(|object| {
            special_binary_property_len(object, PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG)
                .map(|len| len.to_string())
                .unwrap_or_else(|| "missing".to_string())
        })
        .collect::<Vec<_>>()
        .join(",");
    let global_object_id_contracts = appointment_objects
        .iter()
        .map(|object| calendar_global_object_id_contract(object, PID_LID_GLOBAL_OBJECT_ID_TAG))
        .collect::<Vec<_>>();
    let clean_global_object_id_contracts = appointment_objects
        .iter()
        .map(|object| {
            calendar_global_object_id_contract(object, PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG)
        })
        .collect::<Vec<_>>();
    let global_object_id_contracts_ok = global_object_id_contracts
        .iter()
        .all(|(_, contract_ok)| *contract_ok);
    let clean_global_object_id_contracts_ok = clean_global_object_id_contracts
        .iter()
        .all(|(_, contract_ok)| *contract_ok);
    let global_object_id_contracts = global_object_id_contracts
        .iter()
        .map(|(contract, _)| *contract)
        .collect::<Vec<_>>()
        .join(",");
    let clean_global_object_id_contracts = clean_global_object_id_contracts
        .iter()
        .map(|(contract, _)| *contract)
        .collect::<Vec<_>>()
        .join(",");
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x70",
        folder_id = "0x0000000000100001",
        sync_type = "0x01",
        calendar_object_count = objects.len(),
        calendar_item_ids = %item_ids,
        calendar_source_keys = %source_keys,
        calendar_canonical_ids = %canonical_ids,
        calendar_message_classes = %message_classes,
        calendar_subject_char_counts = %subject_lengths,
        calendar_body_char_counts = %body_lengths,
        calendar_message_sizes = %message_sizes,
        calendar_property_tag_count = property_tag_count,
        calendar_property_tags = %property_tags,
        calendar_property_shapes = %property_shapes,
        calendar_configuration_object_count = configuration_objects.len(),
        calendar_dictionary_configuration_object_count = dictionary_configuration_objects,
        calendar_xml_configuration_object_count = xml_configuration_objects,
        calendar_appointment_object_count = appointment_objects.len(),
        calendar_required_property_tags =
            %format_calendar_required_property_tags(
                !configuration_objects.is_empty(),
                !appointment_objects.is_empty()
            ),
        calendar_configuration_required_property_tags =
            %format_debug_property_tags(&configuration_required_tags),
        calendar_appointment_required_property_tags =
            %format_debug_property_tags(&appointment_required_tags),
        calendar_missing_configuration_property_tags = %missing_configuration_tags,
        calendar_missing_appointment_property_tags = %missing_appointment_tags,
        calendar_missing_required_property_tags = %missing_required_tags,
        calendar_required_properties_complete = missing_required_tags.is_empty(),
        calendar_start_end_order_ok = start_end_order_ok,
        calendar_global_object_id_lengths = %global_object_id_lengths,
        calendar_clean_global_object_id_lengths = %clean_global_object_id_lengths,
        calendar_global_object_id_third_party_contracts = %global_object_id_contracts,
        calendar_clean_global_object_id_third_party_contracts = %clean_global_object_id_contracts,
        calendar_global_object_id_third_party_contracts_ok = global_object_id_contracts_ok,
        calendar_clean_global_object_id_third_party_contracts_ok = clean_global_object_id_contracts_ok,
        message = "rca debug mapi calendar special sync objects"
    );
}

pub(in crate::mapi::dispatch) fn log_special_sync_objects(
    principal: &AccountPrincipal,
    folder_id: u64,
    sync_type: u8,
    objects: &[mapi_mailstore::SpecialMessageSyncFact],
) {
    if folder_id == CALENDAR_FOLDER_ID || sync_type != 0x01 || objects.is_empty() {
        return;
    }
    let item_ids = objects
        .iter()
        .map(|object| format!("0x{:016x}", object.item_id))
        .collect::<Vec<_>>()
        .join(",");
    let source_keys = objects
        .iter()
        .map(|object| bytes_to_hex(&mapi_mailstore::source_key_for_store_id(object.item_id)))
        .collect::<Vec<_>>()
        .join(",");
    let canonical_ids = objects
        .iter()
        .map(|object| object.canonical_id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let message_classes = objects
        .iter()
        .map(|object| object.message_class.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let subject_lengths = objects
        .iter()
        .map(|object| object.subject.chars().count().to_string())
        .collect::<Vec<_>>()
        .join(",");
    let body_lengths = objects
        .iter()
        .map(|object| {
            object.body_text.as_deref().map_or_else(
                || "absent".to_string(),
                |body_text| body_text.chars().count().to_string(),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let property_tag_count = objects
        .iter()
        .map(|object| object.named_properties.len())
        .sum::<usize>();
    let property_tags = objects
        .iter()
        .flat_map(|object| object.named_properties.iter().map(|(tag, _)| *tag))
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",");
    let property_shapes = objects
        .iter()
        .flat_map(|object| {
            object
                .named_properties
                .iter()
                .map(|(tag, value)| format!("0x{tag:08x}:{}", special_property_shape(value)))
        })
        .collect::<Vec<_>>()
        .join(",");
    let associated_count = objects.iter().filter(|object| object.associated).count();
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x70",
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = role_for_folder_id(folder_id).unwrap_or(""),
        sync_type = "0x01",
        special_object_count = objects.len(),
        special_associated_object_count = associated_count,
        special_item_ids = %item_ids,
        special_source_keys = %source_keys,
        special_canonical_ids = %canonical_ids,
        special_message_classes = %message_classes,
        special_subject_char_counts = %subject_lengths,
        special_body_char_counts = %body_lengths,
        special_property_tag_count = property_tag_count,
        special_property_tags = %property_tags,
        special_property_shapes = %property_shapes,
        message = "rca debug mapi special sync objects"
    );
}

fn is_rca_special_contract_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CONTACTS_FOLDER_ID
            | JOURNAL_FOLDER_ID
            | NOTES_FOLDER_ID
            | TASKS_FOLDER_ID
            | REMINDERS_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
            | SEARCH_FOLDER_ID
            | CONTACTS_SEARCH_FOLDER_ID
            | TODO_SEARCH_FOLDER_ID
            | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
            | QUICK_STEP_SETTINGS_FOLDER_ID
    )
}

pub(in crate::mapi::dispatch) fn expected_special_folder_parent_id(folder_id: u64) -> u64 {
    match folder_id {
        REMINDERS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => ROOT_FOLDER_ID,
        _ => IPM_SUBTREE_FOLDER_ID,
    }
}

pub(in crate::mapi::dispatch) fn expected_special_folder_item_message_class(
    folder_id: u64,
) -> &'static str {
    match folder_id {
        CONTACTS_FOLDER_ID
        | SUGGESTED_CONTACTS_FOLDER_ID
        | QUICK_CONTACTS_FOLDER_ID
        | IM_CONTACT_LIST_FOLDER_ID
        | CONTACTS_SEARCH_FOLDER_ID => "IPM.Contact",
        JOURNAL_FOLDER_ID => "IPM.Activity",
        NOTES_FOLDER_ID => "IPM.StickyNote",
        SEARCH_FOLDER_ID => "IPM.Note",
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => "IPM.Task",
        REMINDERS_FOLDER_ID => "Outlook.Reminder",
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => "IPM.Configuration",
        QUICK_STEP_SETTINGS_FOLDER_ID => "IPM.Configuration",
        _ => "",
    }
}

fn calendar_sync_object_start_end_order_ok(
    object: &mapi_mailstore::SpecialMessageSyncFact,
) -> bool {
    let start = special_i64_property(object, PID_TAG_START_DATE)
        .or_else(|| special_i64_property(object, PID_LID_COMMON_START_TAG))
        .or_else(|| special_i64_property(object, PID_LID_APPOINTMENT_START_WHOLE_TAG));
    let end = special_i64_property(object, PID_TAG_END_DATE)
        .or_else(|| special_i64_property(object, PID_LID_COMMON_END_TAG))
        .or_else(|| special_i64_property(object, PID_LID_APPOINTMENT_END_WHOLE_TAG));
    match (start, end) {
        (Some(start), Some(end)) => start < end,
        _ => false,
    }
}

pub(in crate::mapi::dispatch) fn is_calendar_configuration_object(
    object: &mapi_mailstore::SpecialMessageSyncFact,
) -> bool {
    object.associated
        && (crate::mapi_store::is_outlook_configuration_message_class_name(
            &object.message_class,
            "IPM.Configuration.Calendar",
        ) || crate::mapi_store::is_outlook_configuration_message_class_name(
            &object.message_class,
            "IPM.Configuration.CategoryList",
        ) || crate::mapi_store::is_outlook_configuration_message_class_name(
            &object.message_class,
            "IPM.Configuration.WorkHours",
        ))
}

fn special_i64_property(
    object: &mapi_mailstore::SpecialMessageSyncFact,
    property_tag: u32,
) -> Option<i64> {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| match (*tag == property_tag, value) {
            (true, mapi_mailstore::SpecialMessagePropertyValue::I64(value)) => Some(*value),
            _ => None,
        })
}

fn special_binary_property_len(
    object: &mapi_mailstore::SpecialMessageSyncFact,
    property_tag: u32,
) -> Option<usize> {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| match (*tag == property_tag, value) {
            (true, mapi_mailstore::SpecialMessagePropertyValue::Binary(value)) => Some(value.len()),
            _ => None,
        })
}

fn calendar_global_object_id_contract(
    object: &mapi_mailstore::SpecialMessageSyncFact,
    property_tag: u32,
) -> (&'static str, bool) {
    let Some(value) = object.named_properties.iter().find_map(|(tag, value)| {
        match (*tag == property_tag, value) {
            (true, mapi_mailstore::SpecialMessagePropertyValue::Binary(value)) => Some(value),
            _ => None,
        }
    }) else {
        return ("missing", false);
    };
    third_party_global_object_id_contract(value)
}

fn third_party_global_object_id_contract(value: &[u8]) -> (&'static str, bool) {
    const GLOBAL_OBJECT_ID_HEADER: [u8; 16] = [
        0x04, 0x00, 0x00, 0x00, 0x82, 0x00, 0xE0, 0x00, 0x74, 0xC5, 0xB7, 0x10, 0x1A, 0x82, 0xE0,
        0x08,
    ];
    const VCAL_UID_PREFIX: &[u8] = b"vCal-Uid\x01\x00\x00\x00";

    if value.len() < 40 || value[..16] != GLOBAL_OBJECT_ID_HEADER {
        return ("non_third_party", true);
    }
    let data = &value[40..];
    if !data.starts_with(b"vCal-Uid") {
        return ("non_third_party", true);
    }
    if data.len() < VCAL_UID_PREFIX.len() {
        return ("third_party_invalid_prefix", false);
    }
    if &data[..VCAL_UID_PREFIX.len()] != VCAL_UID_PREFIX {
        return ("third_party_invalid_prefix", false);
    }
    if value[16..20].iter().any(|byte| *byte != 0) {
        return ("third_party_invalid_date", false);
    }
    if value[20..28].iter().any(|byte| *byte != 0) {
        return ("third_party_invalid_creation_time", false);
    }
    if value[28..36].iter().any(|byte| *byte != 0) {
        return ("third_party_invalid_reserved", false);
    }
    let declared_size = u32::from_le_bytes([value[36], value[37], value[38], value[39]]) as usize;
    if declared_size != data.len() {
        return ("third_party_invalid_size", false);
    }
    ("third_party_valid", true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn third_party_global_object_id_contract_flags_nonzero_creation_time() {
        let mut value = vec![
            0x04, 0x00, 0x00, 0x00, 0x82, 0x00, 0xE0, 0x00, 0x74, 0xC5, 0xB7, 0x10, 0x1A, 0x82,
            0xE0, 0x08,
        ];
        value.extend_from_slice(&[0; 20]);
        let data = b"vCal-Uid\x01\x00\x00\x00calendar-probe";
        value.extend_from_slice(&(data.len() as u32).to_le_bytes());
        value.extend_from_slice(data);

        assert_eq!(
            third_party_global_object_id_contract(&value),
            ("third_party_valid", true)
        );

        value[20] = 1;
        assert_eq!(
            third_party_global_object_id_contract(&value),
            ("third_party_invalid_creation_time", false)
        );
    }
}

fn special_property_shape(value: &mapi_mailstore::SpecialMessagePropertyValue) -> String {
    match value {
        mapi_mailstore::SpecialMessagePropertyValue::Binary(value) => {
            format!("binary:bytes={}", value.len())
        }
        mapi_mailstore::SpecialMessagePropertyValue::Bool(value) => {
            format!("bool={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::Guid(value) => {
            format!("guid={}", bytes_to_hex(value))
        }
        mapi_mailstore::SpecialMessagePropertyValue::I32(value) => {
            format!("i32={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::I64(value) => {
            format!("i64={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::U32(value) => {
            format!("u32={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::U64(value) => {
            format!("u64={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::String(value) => {
            format!("string:chars={}", value.chars().count())
        }
        mapi_mailstore::SpecialMessagePropertyValue::MultiI32(values) => {
            format!("multi_i32:count={}", values.len())
        }
        mapi_mailstore::SpecialMessagePropertyValue::MultiBinary(values) => {
            format!("multi_binary:count={}", values.len())
        }
        mapi_mailstore::SpecialMessagePropertyValue::MultiString(values) => {
            format!("multistring:count={}", values.len())
        }
        mapi_mailstore::SpecialMessagePropertyValue::Time(value) => {
            format!("time:chars={}", value.chars().count())
        }
    }
}
