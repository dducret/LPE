use super::super::*;

pub(in crate::mapi::dispatch) fn default_folder_identification_contract_for_debug(
    principal: &AccountPrincipal,
) -> String {
    let tags = [
        PID_TAG_VALID_FOLDER_MASK,
        PID_TAG_IPM_SUBTREE_ENTRY_ID,
        PID_TAG_IPM_OUTBOX_ENTRY_ID,
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID,
        PID_TAG_IPM_SENTMAIL_ENTRY_ID,
        PID_TAG_VIEWS_ENTRY_ID,
        PID_TAG_COMMON_VIEWS_ENTRY_ID,
        PID_TAG_FINDER_ENTRY_ID,
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
        PID_TAG_IPM_CONTACT_ENTRY_ID,
        PID_TAG_IPM_JOURNAL_ENTRY_ID,
        PID_TAG_IPM_NOTE_ENTRY_ID,
        PID_TAG_IPM_TASK_ENTRY_ID,
        PID_TAG_REM_ONLINE_ENTRY_ID,
        PID_TAG_IPM_DRAFTS_ENTRY_ID,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX,
        PID_TAG_FREE_BUSY_ENTRY_IDS,
    ];
    tags.into_iter()
        .filter_map(|tag| {
            let value = special_folder_identification_property_value(principal.account_id, tag)?;
            if tag == PID_TAG_VALID_FOLDER_MASK {
                return Some(format!(
                    "{tag:#010x}:PidTagValidFolderMask:{}",
                    mapi_value_debug_u32_from_value(&value)
                ));
            }
            if tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX {
                return Some(format!(
                    "{tag:#010x}:PidTagAdditionalRenEntryIdsEx:{}",
                    mapi_value_debug_shape(&value)
                ));
            }
            Some(default_folder_entry_id_values_for_debug(&[(tag, value)]))
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(in crate::mapi::dispatch) fn log_default_folder_discovery_contract(
    principal: &AccountPrincipal,
    request_id: &str,
    stage: &str,
    request_rop_id: &str,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) {
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id,
        stage,
        default_folder_identification_contract =
            %default_folder_identification_contract_for_debug(principal),
        default_folder_hierarchy_projection =
            %default_folder_hierarchy_projection_for_debug(
                principal,
                mailboxes,
                emails,
                snapshot
            ),
        message = "rca debug mapi default folder discovery contract"
    );
}

pub(in crate::mapi::dispatch) fn default_folder_hierarchy_projection_for_debug(
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> String {
    default_folder_discovery_specs()
        .iter()
        .map(|(name, tag, folder_id)| {
            let entry_id = special_folder_identification_property_value(principal.account_id, *tag)
                .and_then(|value| match value {
                    MapiValue::Binary(bytes) => Some(bytes),
                    _ => None,
                })
                .unwrap_or_default();
            let entry_id_decoded =
                crate::mapi::identity::object_id_from_folder_entry_id(&entry_id);
            let source_key = mapi_mailstore::source_key_for_store_id(*folder_id);
            let source_key_decoded = crate::mapi::identity::object_id_from_source_key(&source_key);
            let parent_folder_id = expected_special_folder_parent_id(*folder_id);
            let parent_source_key = mapi_mailstore::source_key_for_store_id(parent_folder_id);
            let parent_source_key_decoded =
                crate::mapi::identity::object_id_from_source_key(&parent_source_key);
            let mailbox_row_present = folder_row_for_id(*folder_id, mailboxes).is_some()
                || mapi_mailstore::virtual_special_mailbox(*folder_id).is_some();
            let collaboration_row_present =
                snapshot.collaboration_folder_for_id(*folder_id).is_some();
            format!(
                "{name}:tag=0x{tag:08x};folder=0x{folder_id:016x};entry_id_bytes={};entry_id_decoded={};entry_id_matches={};source_key_len={};source_key_decoded={};source_key_matches={};parent=0x{parent_folder_id:016x};parent_source_key_decoded={};parent_matches={};container={};mailbox_row_present={};collaboration_row_present={};projected_content_count={}",
                entry_id.len(),
                format_optional_folder_id(entry_id_decoded),
                entry_id_decoded == Some(*folder_id),
                source_key.len(),
                format_optional_folder_id(source_key_decoded),
                source_key_decoded == Some(*folder_id),
                format_optional_folder_id(parent_source_key_decoded),
                parent_source_key_decoded == Some(parent_folder_id),
                expected_special_folder_container_class(*folder_id),
                mailbox_row_present,
                collaboration_row_present,
                folder_message_count(*folder_id, mailboxes, emails, snapshot)
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(in crate::mapi::dispatch) fn default_folder_entry_id_values_for_debug(
    values: &[(u32, MapiValue)],
) -> String {
    values
        .iter()
        .filter_map(|(tag, value)| {
            let storage_tag = canonical_property_storage_tag(*tag);
            if storage_tag == PID_TAG_DEFAULT_VIEW_ENTRY_ID {
                return Some(default_view_entry_id_for_debug(storage_tag, value));
            }
            if storage_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS {
                return Some(indexed_special_folder_entry_ids_for_debug(
                    storage_tag,
                    "PidTagAdditionalRenEntryIds",
                    value,
                    &[
                        CONFLICTS_FOLDER_ID,
                        SYNC_ISSUES_FOLDER_ID,
                        LOCAL_FAILURES_FOLDER_ID,
                        SERVER_FAILURES_FOLDER_ID,
                        JUNK_FOLDER_ID,
                    ],
                ));
            }
            if storage_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX {
                return Some(additional_ren_entry_ids_ex_for_debug(value));
            }
            if storage_tag == PID_TAG_FREE_BUSY_ENTRY_IDS {
                return Some(indexed_special_folder_entry_ids_for_debug(
                    storage_tag,
                    "PidTagFreeBusyEntryIds",
                    value,
                    &[0, 0, 0, FREEBUSY_DATA_FOLDER_ID],
                ));
            }
            let expected_folder_id = default_folder_entry_id_expected_folder_id(storage_tag)?;
            let property_name = default_folder_entry_id_property_name(storage_tag);
            let MapiValue::Binary(bytes) = value else {
                return Some(format!(
                    "{storage_tag:#010x}:{property_name}:value_type={}",
                    mapi_value_debug_shape(value)
                ));
            };
            let decoded_folder_id =
                crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes).unwrap_or(0);
            let decoded_name = if decoded_folder_id == 0 {
                "invalid"
            } else {
                post_hierarchy_probe_folder_name(decoded_folder_id)
            };
            Some(format!(
                "{storage_tag:#010x}:{property_name}:bytes={}:decoded_folder_id=0x{decoded_folder_id:016x}:decoded_name={decoded_name}:expected_folder_id=0x{expected_folder_id:016x}:matches_expected={}",
                bytes.len(),
                decoded_folder_id == expected_folder_id
            ))
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi::dispatch) fn default_folder_getprops_response_values_for_debug(
    property_tags: &[u32],
    response: &[u8],
) -> String {
    if response.get(6).copied() != Some(0) {
        return "not-standard-row".to_string();
    }
    let mut cursor = Cursor::new(response.get(7..).unwrap_or_default());
    let mut values = Vec::new();
    for tag in property_tags {
        let storage_tag = canonical_property_storage_tag(*tag);
        let parsed = parse_property_value_for_tag(&mut cursor, *tag);
        if !is_default_folder_identification_property_tag(storage_tag) {
            if let Ok(value) = parsed {
                values.push(format!(
                    "{storage_tag:#010x}:{}:{}",
                    set_property_debug_name(storage_tag),
                    mapi_value_debug_shape(&value)
                ));
            }
            continue;
        }
        match parsed {
            Ok(value) => values.push(default_folder_getprops_value_for_debug(storage_tag, &value)),
            Err(error) => values.push(format!(
                "{storage_tag:#010x}:{}:parse_error={error}",
                default_folder_entry_id_property_name(storage_tag)
            )),
        }
    }
    values.join(",")
}

pub(in crate::mapi::dispatch) fn default_folder_getprops_value_for_debug(
    tag: u32,
    value: &MapiValue,
) -> String {
    let storage_tag = canonical_property_storage_tag(tag);
    if storage_tag == PID_TAG_VALID_FOLDER_MASK {
        return format!(
            "{storage_tag:#010x}:PidTagValidFolderMask:{}",
            mapi_value_debug_u32_from_value(value)
        );
    }
    let decoded = default_folder_entry_id_values_for_debug(&[(storage_tag, value.clone())]);
    if decoded.is_empty() {
        format!(
            "{storage_tag:#010x}:{}:{}",
            default_folder_entry_id_property_name(storage_tag),
            mapi_value_debug_shape(value)
        )
    } else {
        decoded
    }
}

fn default_view_entry_id_for_debug(storage_tag: u32, value: &MapiValue) -> String {
    let MapiValue::Binary(bytes) = value else {
        return format!(
            "{storage_tag:#010x}:PidTagDefaultViewEntryId:value_type={}",
            mapi_value_debug_shape(value)
        );
    };
    match default_view_entry_id_target_for_debug(bytes) {
        Some((folder_id, message_id)) => format!(
            "{storage_tag:#010x}:PidTagDefaultViewEntryId:bytes={}:decoded_folder_id=0x{folder_id:016x}:decoded_folder_name={}:decoded_message_id=0x{message_id:016x}",
            bytes.len(),
            post_hierarchy_probe_folder_name(folder_id)
        ),
        None => format!(
            "{storage_tag:#010x}:PidTagDefaultViewEntryId:bytes={}:decode=not_message_entry_id:preview={}",
            bytes.len(),
            hex_preview(bytes, 32)
        ),
    }
}

fn default_view_entry_id_target_for_debug(entry_id: &[u8]) -> Option<(u64, u64)> {
    if entry_id.len() != 70
        || entry_id[0..4] != [0, 0, 0, 0]
        || entry_id[20..22] != 0x0007u16.to_le_bytes()
        || entry_id[44..46] != [0, 0]
        || entry_id[68..70] != [0, 0]
    {
        return None;
    }
    let folder_counter = crate::mapi::identity::global_counter_from_globcnt(&entry_id[38..44])?;
    let message_counter = crate::mapi::identity::global_counter_from_globcnt(&entry_id[62..68])?;
    Some((
        crate::mapi::identity::mapi_store_id(folder_counter),
        crate::mapi::identity::mapi_store_id(message_counter),
    ))
}

fn additional_ren_entry_ids_ex_for_debug(value: &MapiValue) -> String {
    let MapiValue::Binary(bytes) = value else {
        return format!(
            "{PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX:#010x}:PidTagAdditionalRenEntryIdsEx:value_type={}",
            mapi_value_debug_shape(value)
        );
    };
    match decode_additional_ren_entry_ids_ex_for_debug(bytes) {
        Ok(entries) => format!(
            "{PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX:#010x}:PidTagAdditionalRenEntryIdsEx:bytes={}:entry_count={}:{}",
            bytes.len(),
            entries.len(),
            entries.join(";")
        ),
        Err(error) => format!(
            "{PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX:#010x}:PidTagAdditionalRenEntryIdsEx:bytes={}:parse_error={error}",
            bytes.len()
        ),
    }
}

fn decode_additional_ren_entry_ids_ex_for_debug(bytes: &[u8]) -> Result<Vec<String>> {
    let mut offset = 0usize;
    let mut entries = Vec::new();
    while offset + 4 <= bytes.len() {
        let persist_id = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
        let data_size = u16::from_le_bytes([bytes[offset + 2], bytes[offset + 3]]) as usize;
        offset += 4;
        if persist_id == 0 {
            return Ok(entries);
        }
        let block_start = offset;
        let block_end = block_start
            .checked_add(data_size)
            .ok_or_else(|| anyhow::anyhow!("persist block size overflow"))?;
        if block_end > bytes.len() {
            return Err(anyhow::anyhow!(
                "persist_id=0x{persist_id:04x}:truncated_block:size={data_size}:remaining={}",
                bytes.len().saturating_sub(block_start)
            ));
        }

        let mut decoded_folder_id = None;
        let mut element_summaries = Vec::new();
        while offset + 4 <= block_end {
            let element_id = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
            let element_size = u16::from_le_bytes([bytes[offset + 2], bytes[offset + 3]]) as usize;
            offset += 4;
            if element_id == 0 {
                break;
            }
            let element_end = offset
                .checked_add(element_size)
                .ok_or_else(|| anyhow::anyhow!("persist element size overflow"))?;
            if element_end > block_end {
                return Err(anyhow::anyhow!(
                    "persist_id=0x{persist_id:04x}:element_id=0x{element_id:04x}:truncated_element:size={element_size}:remaining={}",
                    block_end.saturating_sub(offset)
                ));
            }
            if element_id == 0x0001 {
                decoded_folder_id = crate::mapi::identity::object_id_from_folder_identifier_bytes(
                    &bytes[offset..element_end],
                );
            }
            element_summaries.push(format!("element=0x{element_id:04x}:bytes={element_size}"));
            offset = element_end;
        }
        offset = block_end;

        let expected_folder_id = additional_ren_entry_ids_ex_expected_folder_id(persist_id);
        let decoded_name = decoded_folder_id
            .map(post_hierarchy_probe_folder_name)
            .unwrap_or("invalid");
        entries.push(format!(
            "persist_id=0x{persist_id:04x}:persist_name={}:data_bytes={data_size}:decoded_folder_id={}:decoded_name={decoded_name}:expected_folder_id={}:matches_expected={}:{}",
            additional_ren_entry_ids_ex_persist_name(persist_id),
            decoded_folder_id
                .map(|folder_id| format!("0x{folder_id:016x}"))
                .unwrap_or_else(|| "invalid".to_string()),
            format_expected_folder_id_for_debug(expected_folder_id),
            decoded_folder_id == Some(expected_folder_id),
            element_summaries.join("+")
        ));
    }

    if offset == bytes.len() {
        Ok(entries)
    } else {
        Err(anyhow::anyhow!(
            "trailing_bytes_without_sentinel={}",
            bytes.len().saturating_sub(offset)
        ))
    }
}

fn additional_ren_entry_ids_ex_expected_folder_id(persist_id: u16) -> u64 {
    match persist_id {
        0x8001 => RSS_FEEDS_FOLDER_ID,
        0x8002 => TRACKED_MAIL_PROCESSING_FOLDER_ID,
        0x8004 => TODO_SEARCH_FOLDER_ID,
        0x8006 => CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
        0x8007 => QUICK_STEP_SETTINGS_FOLDER_ID,
        0x8008 => SUGGESTED_CONTACTS_FOLDER_ID,
        0x8009 => CONTACTS_SEARCH_FOLDER_ID,
        0x800A => IM_CONTACT_LIST_FOLDER_ID,
        0x800B => QUICK_CONTACTS_FOLDER_ID,
        0x800F => ARCHIVE_FOLDER_ID,
        _ => 0,
    }
}

fn additional_ren_entry_ids_ex_persist_name(persist_id: u16) -> &'static str {
    match persist_id {
        0x8001 => "rss_subscriptions",
        0x8002 => "send_and_track",
        0x8004 => "todo_search",
        0x8006 => "conversation_actions",
        0x8007 => "quick_step_settings",
        0x8008 => "suggested_contacts",
        0x8009 => "contact_search",
        0x800A => "buddylist_pdls",
        0x800B => "buddylist_contacts",
        0x800F => "archive",
        _ => "unknown",
    }
}

fn indexed_special_folder_entry_ids_for_debug(
    storage_tag: u32,
    property_name: &'static str,
    value: &MapiValue,
    expected_folder_ids: &[u64],
) -> String {
    let MapiValue::MultiBinary(values) = value else {
        return format!(
            "{storage_tag:#010x}:{property_name}:value_type={}",
            mapi_value_debug_shape(value)
        );
    };
    let mut summaries = Vec::new();
    for (index, bytes) in values.iter().enumerate() {
        let expected_folder_id = expected_folder_ids.get(index).copied().unwrap_or(0);
        summaries.push(format_indexed_special_folder_entry_id(
            index,
            bytes,
            expected_folder_id,
        ));
    }
    if values.len() < expected_folder_ids.len() {
        summaries.push(format!(
            "omitted_preserved_indexes={}",
            (values.len()..expected_folder_ids.len())
                .map(|index| index.to_string())
                .collect::<Vec<_>>()
                .join("+")
        ));
    }
    format!(
        "{storage_tag:#010x}:{property_name}:count={}:{}",
        values.len(),
        summaries.join(";")
    )
}

fn format_indexed_special_folder_entry_id(
    index: usize,
    bytes: &[u8],
    expected_folder_id: u64,
) -> String {
    if bytes.is_empty() {
        return format!(
            "index={index}:bytes=0:expected_folder_id={}:matches_expected={}",
            format_expected_folder_id_for_debug(expected_folder_id),
            expected_folder_id == 0
        );
    }
    let decoded_folder_id =
        crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes).unwrap_or(0);
    let decoded_name = if decoded_folder_id == 0 {
        "invalid"
    } else {
        post_hierarchy_probe_folder_name(decoded_folder_id)
    };
    format!(
        "index={index}:bytes={}:decoded_folder_id=0x{decoded_folder_id:016x}:decoded_name={decoded_name}:expected_folder_id={}:matches_expected={}",
        bytes.len(),
        format_expected_folder_id_for_debug(expected_folder_id),
        decoded_folder_id == expected_folder_id
    )
}
