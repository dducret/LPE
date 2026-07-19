use super::*;

pub(crate) struct FastTransferDebugProperty {
    tag: u32,
    value: Vec<u8>,
    next_offset: usize,
}

#[derive(Default)]
pub(crate) struct ContentTransferFaiDebugSummary {
    pub(crate) fai_items: Vec<ContentTransferFaiItemDebug>,
    pub(crate) final_idset_given_summary: String,
    pub(crate) final_cnset_seen_fai_summary: String,
    final_cnset_seen_fai_counters: Vec<u64>,
}

#[derive(Default)]
pub(crate) struct ContentTransferFaiItemDebug {
    pub(crate) item_id: Option<u64>,
    pub(crate) global_counter: Option<u64>,
    pub(crate) change_number: Option<u64>,
    pub(crate) subject: String,
    pub(crate) message_class: String,
    pub(crate) entry_id_len: usize,
    pub(crate) record_key_len: usize,
    pub(crate) change_key_len: usize,
    pub(crate) predecessor_change_list_len: usize,
    pub(crate) source_key_len: usize,
    pub(crate) parent_source_key_len: usize,
    pub(crate) source_key_hex: String,
    pub(crate) parent_source_key_hex: String,
    pub(crate) associated: Option<bool>,
    pub(crate) message_flags: Option<u32>,
    pub(crate) property_tags: Vec<u32>,
    pub(crate) property_value_shapes: Vec<(u32, String)>,
    pub(crate) change_number_in_final_cnset_fai: bool,
    pub(crate) item_start_offset: usize,
    pub(crate) item_end_offset: usize,
    pub(crate) item_byte_len: usize,
    pub(crate) message_start_marker_offset: Option<usize>,
    pub(crate) message_end_marker_offset: Option<usize>,
    pub(crate) property_list_start_offset: Option<usize>,
    pub(crate) property_list_end_offset: Option<usize>,
    pub(crate) attachment_marker_count: usize,
    pub(crate) recipient_marker_count: usize,
    pub(crate) payload_preview_hex: String,
    pub(crate) payload_tail_hex: String,
}

#[derive(Default)]
struct ContentTransferMessageDebug {
    source_key: Vec<u8>,
    parent_source_key: Vec<u8>,
    change_key: Vec<u8>,
    predecessor_change_list: Vec<u8>,
    record_key_len: usize,
    item_id: Option<u64>,
    global_counter: Option<u64>,
    change_number: Option<u64>,
    associated: bool,
    associated_present: bool,
    message_flags: Option<u32>,
    subject: String,
    message_class: String,
    entry_id_len: usize,
    property_tags: Vec<u32>,
    property_value_shapes: Vec<(u32, String)>,
    item_start_offset: usize,
    message_start_marker_offset: Option<usize>,
    property_list_start_offset: Option<usize>,
    property_list_end_offset: Option<usize>,
}

pub(crate) fn decode_hierarchy_transfer_debug_summary(
    bytes: &[u8],
) -> Result<HierarchyTransferDebugSummary, String> {
    let mut offset = 0;
    let mut current_folder: Option<HierarchyTransferFolderDebug> = None;
    let mut seen_source_keys = Vec::<Vec<u8>>::new();
    let mut emitted_property_tags = BTreeSet::new();
    let mut summary = HierarchyTransferDebugSummary::default();
    let mut in_final_state = false;

    while offset < bytes.len() {
        let tag = read_debug_u32(bytes, offset)?;
        if hierarchy_debug_marker(tag) {
            summary.marker_tags.push(tag);
            match tag {
                INCR_SYNC_CHG => {
                    if let Some(folder) = current_folder.take() {
                        finish_hierarchy_debug_folder(folder, &mut seen_source_keys, &mut summary);
                    }
                    current_folder = Some(HierarchyTransferFolderDebug::default());
                }
                INCR_SYNC_STATE_BEGIN => {
                    if let Some(folder) = current_folder.take() {
                        finish_hierarchy_debug_folder(folder, &mut seen_source_keys, &mut summary);
                    }
                    summary.final_state_present = true;
                    in_final_state = true;
                }
                INCR_SYNC_STATE_END => {
                    in_final_state = false;
                }
                INCR_SYNC_END => {
                    if let Some(folder) = current_folder.take() {
                        finish_hierarchy_debug_folder(folder, &mut seen_source_keys, &mut summary);
                    }
                    summary.stream_end_marker_seen = true;
                    offset += 4;
                    if offset != bytes.len() {
                        return Err("trailing bytes after IncrSyncEnd".into());
                    }
                    break;
                }
                _ => unreachable!(),
            }
            offset += 4;
            continue;
        }

        let property = parse_debug_fast_transfer_property(bytes, offset)?;
        offset = property.next_offset;
        summary.property_count += 1;
        emitted_property_tags.insert(property.tag);

        if in_final_state && current_folder.is_none() {
            collect_final_state_debug_property(&property, &mut summary);
        }

        if let Some(folder) = current_folder.as_mut() {
            folder.property_tags.push(property.tag);
            match property.tag {
                PID_TAG_PARENT_SOURCE_KEY => folder.parent_source_key = Some(property.value),
                PID_TAG_SOURCE_KEY => folder.source_key = Some(property.value),
                PID_TAG_CHANGE_KEY => folder.change_key = Some(property.value),
                PID_TAG_PREDECESSOR_CHANGE_LIST => {
                    folder.predecessor_change_list = Some(property.value)
                }
                PID_TAG_DISPLAY_NAME_W => {
                    folder.display_name = decode_debug_utf16z(&property.value)
                }
                PID_TAG_CONTAINER_CLASS_W => {
                    folder.container_class = decode_debug_utf16z(&property.value)
                }
                PID_TAG_FOLDER_ID => folder.folder_id = decode_debug_object_id(&property.value),
                PID_TAG_PARENT_FOLDER_ID => {
                    folder.parent_folder_id = decode_debug_object_id(&property.value)
                }
                PID_TAG_LAST_MODIFICATION_TIME => {
                    folder.last_modification_time = decode_debug_u64(&property.value)
                }
                PID_TAG_CHANGE_NUMBER => {
                    folder.change_number = decode_debug_change_number(&property.value)
                }
                PID_TAG_CONTENT_COUNT => folder.content_count = decode_debug_i32(&property.value),
                PID_TAG_CONTENT_UNREAD_COUNT => {
                    folder.content_unread_count = decode_debug_i32(&property.value)
                }
                PID_TAG_FOLDER_TYPE => folder.folder_type = decode_debug_i32(&property.value),
                PID_TAG_LOCAL_COMMIT_TIME_MAX => {
                    folder.local_commit_time_max = decode_debug_u64(&property.value)
                }
                PID_TAG_DELETED_COUNT_TOTAL => {
                    folder.deleted_count_total = decode_debug_i32(&property.value)
                }
                PID_TAG_MESSAGE_SIZE => folder.message_size = decode_debug_i32(&property.value),
                PID_TAG_ACCESS => folder.access = decode_debug_i32(&property.value),
                PID_TAG_SUBFOLDERS => folder.subfolders = decode_debug_bool(&property.value),
                _ => {}
            }
        } else if !in_final_state {
            return Err(format!(
                "property 0x{:08x} appears outside folderChange or final state",
                property.tag
            ));
        }
    }

    if let Some(folder) = current_folder.take() {
        finish_hierarchy_debug_folder(folder, &mut seen_source_keys, &mut summary);
    }
    summary.emitted_property_tags = emitted_property_tags.into_iter().collect();
    finalize_hierarchy_debug_summary(&mut summary);
    Ok(summary)
}

pub(crate) fn decode_content_transfer_fai_debug_summary(
    bytes: &[u8],
) -> Result<ContentTransferFaiDebugSummary, String> {
    let mut offset = 0;
    let mut current_message: Option<ContentTransferMessageDebug> = None;
    let mut in_final_state = false;
    let mut summary = ContentTransferFaiDebugSummary::default();

    while offset < bytes.len() {
        let tag = read_debug_u32(bytes, offset)?;
        if content_debug_marker(tag) {
            match tag {
                INCR_SYNC_CHG => {
                    finish_content_fai_debug_message(
                        current_message.take(),
                        &summary.final_cnset_seen_fai_counters,
                        &mut summary.fai_items,
                        bytes,
                        offset,
                    );
                    current_message = Some(ContentTransferMessageDebug {
                        item_start_offset: offset,
                        ..ContentTransferMessageDebug::default()
                    });
                    in_final_state = false;
                }
                INCR_SYNC_MESSAGE => {
                    if let Some(message) = current_message.as_mut() {
                        message.message_start_marker_offset = Some(offset);
                        message.property_list_start_offset = Some(offset + 4);
                    }
                    in_final_state = false;
                }
                NEW_ATTACH | START_EMBED | END_EMBED | END_ATTACH => {
                    in_final_state = false;
                }
                START_RECIP | END_TO_RECIP => {
                    in_final_state = false;
                }
                INCR_SYNC_PROGRESS_MODE | INCR_SYNC_PROGRESS_PER_MSG => {
                    finish_content_fai_debug_message(
                        current_message.take(),
                        &summary.final_cnset_seen_fai_counters,
                        &mut summary.fai_items,
                        bytes,
                        offset,
                    );
                    in_final_state = false;
                }
                INCR_SYNC_DEL | INCR_SYNC_READ => {
                    finish_content_fai_debug_message(
                        current_message.take(),
                        &summary.final_cnset_seen_fai_counters,
                        &mut summary.fai_items,
                        bytes,
                        offset,
                    );
                    in_final_state = false;
                }
                INCR_SYNC_STATE_BEGIN => {
                    finish_content_fai_debug_message(
                        current_message.take(),
                        &summary.final_cnset_seen_fai_counters,
                        &mut summary.fai_items,
                        bytes,
                        offset,
                    );
                    in_final_state = true;
                }
                INCR_SYNC_STATE_END => {
                    in_final_state = false;
                }
                INCR_SYNC_END => {
                    finish_content_fai_debug_message(
                        current_message.take(),
                        &summary.final_cnset_seen_fai_counters,
                        &mut summary.fai_items,
                        bytes,
                        offset,
                    );
                    offset += 4;
                    if offset != bytes.len() {
                        return Err("trailing bytes after IncrSyncEnd".into());
                    }
                    break;
                }
                _ => unreachable!(),
            }
            offset += 4;
            continue;
        }

        let property_offset = offset;
        let property = parse_debug_fast_transfer_property(bytes, offset)?;
        offset = property.next_offset;
        if in_final_state {
            match property.tag {
                META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY => {
                    summary.final_idset_given_summary =
                        format_replguid_globset_debug(&property.value);
                }
                META_TAG_CNSET_SEEN_FAI => {
                    summary.final_cnset_seen_fai_summary =
                        format_replguid_globset_debug(&property.value);
                    summary.final_cnset_seen_fai_counters =
                        replguid_globset_counters(&property.value).unwrap_or_default();
                }
                _ => {}
            }
            continue;
        }

        let Some(message) = current_message.as_mut() else {
            continue;
        };
        message
            .property_list_start_offset
            .get_or_insert(property_offset);
        message.property_tags.push(property.tag);
        if content_fai_debug_value_shape_property(property.tag) {
            message.property_value_shapes.push((
                property.tag,
                fast_transfer_value_shape(property.tag, &property.value),
            ));
        }
        match property.tag {
            PID_TAG_SOURCE_KEY => {
                message.global_counter = counter_from_xid(&property.value);
                message.item_id =
                    counter_from_xid(&property.value).map(crate::mapi::identity::mapi_store_id);
                message.source_key = property.value.clone();
            }
            PID_TAG_CHANGE_KEY => {
                message.change_number = counter_from_xid(&property.value);
                message.change_key = property.value.clone();
            }
            PID_TAG_PREDECESSOR_CHANGE_LIST => {
                message.predecessor_change_list = property.value.clone()
            }
            PID_TAG_ASSOCIATED => {
                message.associated_present = true;
                message.associated = decode_debug_bool(&property.value).unwrap_or_default()
            }
            PID_TAG_MESSAGE_FLAGS => {
                message.message_flags = decode_debug_i32(&property.value).map(|value| value as u32)
            }
            PID_TAG_MID => message.item_id = decode_debug_object_id(&property.value),
            PID_TAG_CHANGE_NUMBER => {
                message.change_number = decode_debug_change_number(&property.value)
            }
            PID_TAG_PARENT_SOURCE_KEY => message.parent_source_key = property.value.clone(),
            PID_TAG_ENTRY_ID => message.entry_id_len = property.value.len(),
            PID_TAG_RECORD_KEY => message.record_key_len = property.value.len(),
            PID_TAG_SUBJECT_W => {
                message.subject = decode_debug_utf16z(&property.value).unwrap_or_default()
            }
            PID_TAG_NORMALIZED_SUBJECT_A => {
                message.subject = decode_debug_string8z(&property.value).unwrap_or_default()
            }
            PID_TAG_NORMALIZED_SUBJECT_W => {
                message.subject = decode_debug_utf16z(&property.value).unwrap_or_default()
            }
            PID_TAG_MESSAGE_CLASS_W => {
                message.message_class = decode_debug_utf16z(&property.value).unwrap_or_default()
            }
            _ => {}
        }
    }

    for item in &mut summary.fai_items {
        item.change_number_in_final_cnset_fai = item.change_number.is_some_and(|change_number| {
            summary
                .final_cnset_seen_fai_counters
                .contains(&change_number)
        });
    }
    Ok(summary)
}

fn finish_content_fai_debug_message(
    message: Option<ContentTransferMessageDebug>,
    final_cnset_seen_fai_counters: &[u64],
    fai_items: &mut Vec<ContentTransferFaiItemDebug>,
    bytes: &[u8],
    item_end_offset: usize,
) {
    let Some(message) = message else {
        return;
    };
    if !message.associated {
        return;
    }
    let source_key_len = message.source_key.len();
    let parent_source_key_len = message.parent_source_key.len();
    let item_start_offset = message.item_start_offset.min(bytes.len());
    let item_end_offset = item_end_offset.min(bytes.len()).max(item_start_offset);
    let item_payload = &bytes[item_start_offset..item_end_offset];
    fai_items.push(ContentTransferFaiItemDebug {
        source_key_hex: format_debug_hex(&message.source_key),
        parent_source_key_hex: format_debug_hex(&message.parent_source_key),
        change_number_in_final_cnset_fai: message
            .change_number
            .is_some_and(|change_number| final_cnset_seen_fai_counters.contains(&change_number)),
        item_id: message.item_id,
        global_counter: message.global_counter,
        change_number: message.change_number,
        subject: message.subject,
        message_class: message.message_class,
        entry_id_len: message.entry_id_len,
        record_key_len: message.record_key_len,
        change_key_len: message.change_key.len(),
        predecessor_change_list_len: message.predecessor_change_list.len(),
        source_key_len,
        parent_source_key_len,
        associated: message.associated_present.then_some(message.associated),
        message_flags: message.message_flags,
        property_tags: message.property_tags,
        property_value_shapes: message.property_value_shapes,
        item_start_offset,
        item_end_offset,
        item_byte_len: item_end_offset.saturating_sub(item_start_offset),
        message_start_marker_offset: message.message_start_marker_offset,
        message_end_marker_offset: Some(item_end_offset),
        property_list_start_offset: message.property_list_start_offset,
        property_list_end_offset: message.property_list_end_offset.or(Some(item_end_offset)),
        attachment_marker_count: 0,
        recipient_marker_count: 0,
        payload_preview_hex: format_debug_hex_preview(item_payload, 32),
        payload_tail_hex: format_debug_hex_tail(item_payload, 32),
    });
}

pub(super) fn content_fai_debug_value_shape_property(tag: u32) -> bool {
    matches!(
        tag,
        PID_TAG_SOURCE_KEY
            | PID_TAG_PARENT_SOURCE_KEY
            | PID_TAG_ENTRY_ID
            | PID_TAG_RECORD_KEY
            | PID_TAG_SEARCH_KEY
            | PID_TAG_CHANGE_KEY
            | PID_TAG_PREDECESSOR_CHANGE_LIST
            | PID_TAG_MESSAGE_CLASS_W
            | PID_TAG_SUBJECT_W
            | PID_TAG_NORMALIZED_SUBJECT_A
            | PID_TAG_NORMALIZED_SUBJECT_W
            | PID_TAG_ASSOCIATED
            | PID_TAG_MESSAGE_FLAGS
            | PID_TAG_MESSAGE_SIZE
            | PID_TAG_LAST_MODIFICATION_TIME
    ) || content_fai_debug_configuration_property(tag)
}

pub(super) fn content_fai_debug_configuration_property(tag: u32) -> bool {
    matches!(
        tag,
        0x6841_0003
            | 0x6842_0048
            | 0x6842_0102
            | 0x6847_0003
            | 0x6849_0003
            | 0x684A_0003
            | 0x684B_0102
            | 0x684C_0102
            | 0x684D_0102
            | 0x684E_0102
            | 0x684F_0048
            | 0x684F_0102
            | 0x6850_0048
            | 0x6850_0102
            | 0x6851_001F
            | 0x6852_0003
            | 0x6853_0003
            | 0x6854_0102
            | 0x6890_0102
            | 0x6891_0102
            | 0x6892_0003
            | 0x7C06_0003
            | 0x7C07_0102
            | 0x7C08_0102
            | 0x7C09_0102
    ) || matches!(
        tag >> 16,
        0x6802
            | 0x6834
            | 0x6835
            | 0x6836
            | 0x6837
            | 0x6838
            | 0x6839
            | 0x683A
            | 0x683B
            | 0x683C
            | 0x683D
    )
}

pub(super) fn fast_transfer_value_shape(tag: u32, value: &[u8]) -> String {
    match tag & 0x0000_FFFF {
        0x0002 => decode_debug_i16(value)
            .map(|value| format!("i16={value}"))
            .unwrap_or_else(|| format!("i16:invalid_bytes={}", value.len())),
        0x0003 => decode_debug_i32(value)
            .map(|value| format!("i32={value}"))
            .unwrap_or_else(|| format!("i32:invalid_bytes={}", value.len())),
        0x000B => decode_debug_bool(value)
            .map(|value| format!("bool={value}"))
            .unwrap_or_else(|| format!("bool:invalid_bytes={}", value.len())),
        0x0014 => decode_debug_u64(value)
            .map(|value| format!("i64={value}"))
            .unwrap_or_else(|| format!("i64:invalid_bytes={}", value.len())),
        0x0040 => decode_debug_u64(value)
            .map(|value| format!("filetime={value}"))
            .unwrap_or_else(|| format!("filetime:invalid_bytes={}", value.len())),
        0x0048 => format!("guid={}", format_debug_hex(value)),
        0x001E => decode_debug_string8z(value)
            .map(|value| format!("string8:chars={}", value.chars().count()))
            .unwrap_or_else(|| format!("string8:invalid_bytes={}", value.len())),
        0x001F => decode_debug_utf16z(value)
            .map(|value| format!("string:chars={}", value.chars().count()))
            .unwrap_or_else(|| format!("string:invalid_bytes={}", value.len())),
        0x0102 => format!(
            "binary:bytes={};preview={}",
            value.len(),
            format_debug_hex_preview(value, 16)
        ),
        0x101E | 0x101F => format!("multistring:bytes={}", value.len()),
        _ => format!("bytes={}", value.len()),
    }
}

pub(super) fn collect_final_state_debug_property(
    property: &FastTransferDebugProperty,
    summary: &mut HierarchyTransferDebugSummary,
) {
    summary.final_state_property_tags.push(property.tag);
    summary
        .final_state_property_lengths
        .push(property.value.len());
    match property.tag {
        META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY => {
            summary.final_state_idset_given_len = property.value.len();
            summary.final_state_idset_given_summary =
                Some(format_replguid_globset_debug(&property.value));
            summary.final_state_idset_given_counters =
                replguid_globset_counters(&property.value).unwrap_or_default();
        }
        META_TAG_CNSET_SEEN => {
            summary.final_state_cnset_seen_len = property.value.len();
            summary.final_state_cnset_seen_summary =
                Some(format_replguid_globset_debug(&property.value));
            summary.final_state_cnset_seen_counters =
                replguid_globset_counters(&property.value).unwrap_or_default();
        }
        META_TAG_CNSET_SEEN_FAI => {
            summary.final_state_cnset_seen_fai_summary =
                Some(format_replguid_globset_debug(&property.value));
        }
        META_TAG_CNSET_READ => {
            summary.final_state_cnset_read_summary =
                Some(format_replguid_globset_debug(&property.value));
        }
        _ => {}
    }
}

pub(super) fn finalize_hierarchy_debug_summary(summary: &mut HierarchyTransferDebugSummary) {
    summary.final_state_expected_property_order_ok = matches!(
        summary.final_state_property_tags.as_slice(),
        [META_TAG_IDSET_GIVEN, META_TAG_CNSET_SEEN]
            | [
                META_TAG_IDSET_GIVEN,
                META_TAG_CNSET_SEEN,
                META_TAG_CNSET_SEEN_FAI,
                META_TAG_CNSET_READ
            ]
    );
    let source_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.source_counter)
        .collect::<Vec<_>>();
    let change_counters = summary
        .rows
        .iter()
        .filter_map(|row| row.change_counter)
        .collect::<Vec<_>>();
    summary.final_state_idset_given_includes_all_expected_folder_source_counters =
        source_counters.len() == summary.folder_change_count
            && counters_include_all(&summary.final_state_idset_given_counters, &source_counters);
    summary.final_state_cnset_seen_includes_all_expected_folder_change_counters =
        change_counters.len() == summary.folder_change_count
            && counters_include_all(&summary.final_state_cnset_seen_counters, &change_counters);
}

pub(super) fn counters_include_all(haystack: &[u64], needles: &[u64]) -> bool {
    let haystack = haystack.iter().copied().collect::<BTreeSet<_>>();
    needles.iter().all(|counter| haystack.contains(counter))
}

pub(super) fn finish_hierarchy_debug_folder(
    folder: HierarchyTransferFolderDebug,
    seen_source_keys: &mut Vec<Vec<u8>>,
    summary: &mut HierarchyTransferDebugSummary,
) {
    summary.folder_change_count += 1;
    let parent_source_key_present = folder.parent_source_key.is_some();
    let parent_source_key = folder.parent_source_key.unwrap_or_default();
    if !parent_source_key.is_empty() {
        summary.nonzero_parent_source_key_count += 1;
        if !hierarchy_debug_known_parent_source_key(&parent_source_key)
            && !seen_source_keys
                .iter()
                .any(|source_key| source_key.as_slice() == parent_source_key.as_slice())
        {
            summary.parent_before_child_violations += 1;
        }
    } else if parent_source_key_present {
        summary.zero_length_parent_source_key_count += 1;
    }
    let source_key = folder.source_key.unwrap_or_default();
    if !source_key.is_empty() {
        summary.source_key_lengths.push(source_key.len());
        seen_source_keys.push(source_key.clone());
    }
    let change_key = folder.change_key.unwrap_or_default();
    if !change_key.is_empty() {
        summary.change_key_lengths.push(change_key.len());
    }
    let predecessor_change_list = folder.predecessor_change_list.unwrap_or_default();
    let missing_core_property_tags = missing_hierarchy_core_property_tags(&folder.property_tags);
    let parent_source_key_index =
        property_position(&folder.property_tags, PID_TAG_PARENT_SOURCE_KEY);
    let source_key_index = property_position(&folder.property_tags, PID_TAG_SOURCE_KEY);
    let last_modification_time_index =
        property_position(&folder.property_tags, PID_TAG_LAST_MODIFICATION_TIME);
    let change_key_index = property_position(&folder.property_tags, PID_TAG_CHANGE_KEY);
    let predecessor_change_list_index =
        property_position(&folder.property_tags, PID_TAG_PREDECESSOR_CHANGE_LIST);
    let display_name_index = property_position(&folder.property_tags, PID_TAG_DISPLAY_NAME_W);
    let container_class_index = property_position(&folder.property_tags, PID_TAG_CONTAINER_CLASS_W);
    let subfolders_index = property_position(&folder.property_tags, PID_TAG_SUBFOLDERS);
    let identity_properties_before_display_name =
        hierarchy_identity_properties_before_display_name(&folder.property_tags);
    let row = HierarchyTransferRowDebug {
        row_index: summary.folder_change_count,
        display_name: folder.display_name.unwrap_or_default(),
        container_class: folder.container_class.unwrap_or_default(),
        folder_id: folder.folder_id,
        parent_folder_id: folder.parent_folder_id,
        source_key_len: source_key.len(),
        parent_source_key_len: parent_source_key.len(),
        change_key_len: change_key.len(),
        source_counter: counter_from_xid(&source_key),
        change_counter: counter_from_xid(&change_key),
        predecessor_change_list_len: predecessor_change_list.len(),
        last_modification_time: folder.last_modification_time,
        change_number: folder.change_number,
        content_count: folder.content_count,
        content_unread_count: folder.content_unread_count,
        folder_type: folder.folder_type,
        local_commit_time_max: folder.local_commit_time_max,
        deleted_count_total: folder.deleted_count_total,
        message_size: folder.message_size,
        access: folder.access,
        subfolders: folder.subfolders,
        source_key_hex: format_debug_hex(&source_key),
        parent_source_key_hex: format_debug_hex(&parent_source_key),
        change_key_hex: format_debug_hex(&change_key),
        property_tags: folder.property_tags,
        missing_core_property_tags,
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_rop_id = "0x70",
        row_index = row.row_index,
        display_name = %row.display_name,
        container_class = %row.container_class,
        folder_id = row.folder_id.map(format_u64_hex).unwrap_or_default(),
        parent_folder_id = row.parent_folder_id.map(format_u64_hex).unwrap_or_default(),
        source_key_len = row.source_key_len,
        parent_source_key_len = row.parent_source_key_len,
        change_key_len = row.change_key_len,
        predecessor_change_list_len = row.predecessor_change_list_len,
        last_modification_time = row.last_modification_time.unwrap_or_default(),
        change_number = row.change_number.unwrap_or_default(),
        change_number_present = row.change_number.is_some(),
        content_count = row.content_count.unwrap_or_default(),
        content_count_present = row.content_count.is_some(),
        content_unread_count = row.content_unread_count.unwrap_or_default(),
        content_unread_count_present = row.content_unread_count.is_some(),
        folder_type = row.folder_type.unwrap_or_default(),
        folder_type_present = row.folder_type.is_some(),
        local_commit_time_max = row.local_commit_time_max.unwrap_or_default(),
        local_commit_time_max_present = row.local_commit_time_max.is_some(),
        deleted_count_total = row.deleted_count_total.unwrap_or_default(),
        deleted_count_total_present = row.deleted_count_total.is_some(),
        message_size = row.message_size.unwrap_or_default(),
        message_size_present = row.message_size.is_some(),
        access = row.access.unwrap_or_default(),
        access_present = row.access.is_some(),
        subfolders = row.subfolders.unwrap_or_default(),
        subfolders_present = row.subfolders.is_some(),
        source_key_hex = %row.source_key_hex,
        parent_source_key_hex = %row.parent_source_key_hex,
        change_key_hex = %row.change_key_hex,
        property_count = row.property_tags.len(),
        first_property_tag = %row.property_tags.first().map(|tag| format!("0x{tag:08x}")).unwrap_or_default(),
        first_property_name = row.property_tags.first().map(|tag| property_tag_debug_name(*tag)).unwrap_or_default(),
        last_property_tag = %row.property_tags.last().map(|tag| format!("0x{tag:08x}")).unwrap_or_default(),
        last_property_name = row.property_tags.last().map(|tag| property_tag_debug_name(*tag)).unwrap_or_default(),
        parent_source_key_property_index = parent_source_key_index,
        source_key_property_index = source_key_index,
        last_modification_time_property_index = last_modification_time_index,
        change_key_property_index = change_key_index,
        predecessor_change_list_property_index = predecessor_change_list_index,
        display_name_property_index = display_name_index,
        container_class_property_index = container_class_index,
        subfolders_property_index = subfolders_index,
        identity_properties_before_display_name,
        emitted_property_tags = %format_property_tags(&row.property_tags),
        emitted_property_names = %format_property_tag_names(&row.property_tags),
        missing_core_property_tags = %format_property_tags(&row.missing_core_property_tags),
        missing_core_property_names = %format_property_tag_names(&row.missing_core_property_tags),
        "rca debug mapi hierarchy transfer row semantics"
    );
    summary.rows.push(row);
}

pub(super) fn missing_hierarchy_core_property_tags(property_tags: &[u32]) -> Vec<u32> {
    [
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_SUBFOLDERS,
    ]
    .into_iter()
    .filter(|tag| !property_tags.contains(tag))
    .collect()
}

pub(super) fn property_position(property_tags: &[u32], property_tag: u32) -> usize {
    property_tags
        .iter()
        .position(|tag| *tag == property_tag)
        .map(|index| index + 1)
        .unwrap_or_default()
}

pub(crate) fn hierarchy_identity_properties_before_display_name(property_tags: &[u32]) -> bool {
    let display_name = property_position(property_tags, PID_TAG_DISPLAY_NAME_W);
    if display_name == 0 {
        return false;
    }
    [
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
    ]
    .into_iter()
    .all(|tag| {
        let position = property_position(property_tags, tag);
        position != 0 && position < display_name
    })
}

pub(super) fn decode_debug_i32(bytes: &[u8]) -> Option<i32> {
    (bytes.len() == 4).then(|| i32::from_le_bytes(bytes.try_into().unwrap()))
}

pub(super) fn decode_debug_i16(bytes: &[u8]) -> Option<i16> {
    (bytes.len() == 2).then(|| i16::from_le_bytes(bytes.try_into().unwrap()))
}

pub(super) fn decode_debug_u64(bytes: &[u8]) -> Option<u64> {
    (bytes.len() == 8).then(|| u64::from_le_bytes(bytes.try_into().unwrap()))
}

pub(super) fn decode_debug_object_id(bytes: &[u8]) -> Option<u64> {
    crate::mapi::identity::object_id_from_wire_id(bytes).or_else(|| decode_debug_u64(bytes))
}

pub(super) fn decode_debug_change_number(bytes: &[u8]) -> Option<u64> {
    crate::mapi::identity::object_id_from_wire_id(bytes)
        .and_then(crate::mapi::identity::global_counter_from_store_id)
        .or_else(|| decode_debug_u64(bytes))
}

pub(super) fn decode_debug_bool(bytes: &[u8]) -> Option<bool> {
    (bytes.len() == 2).then(|| u16::from_le_bytes(bytes.try_into().unwrap()) != 0)
}

pub(super) fn decode_debug_utf16z(bytes: &[u8]) -> Option<String> {
    if bytes.len() % 2 != 0 {
        return None;
    }
    let mut units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    if units.last() == Some(&0) {
        units.pop();
    }
    String::from_utf16(&units).ok()
}

pub(super) fn decode_debug_string8z(bytes: &[u8]) -> Option<String> {
    let value = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    String::from_utf8(value.to_vec()).ok()
}

pub(super) fn format_debug_hex(bytes: &[u8]) -> String {
    hex_lower(bytes)
}

pub(super) fn format_debug_hex_preview(bytes: &[u8], max_len: usize) -> String {
    format_debug_hex(&bytes[..bytes.len().min(max_len)])
}

pub(super) fn format_debug_hex_tail(bytes: &[u8], max_len: usize) -> String {
    let start = bytes.len().saturating_sub(max_len);
    format_debug_hex(&bytes[start..])
}

pub(super) fn format_u64_hex(value: u64) -> String {
    format!("0x{value:016x}")
}

pub(super) fn format_property_tag_names(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| property_tag_debug_name(*tag))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn property_tag_debug_name(tag: u32) -> &'static str {
    match tag {
        PID_TAG_DISPLAY_NAME_W => "PidTagDisplayName",
        PID_TAG_CONTENT_COUNT => "PidTagContentCount",
        PID_TAG_CONTENT_UNREAD_COUNT => "PidTagContentUnreadCount",
        PID_TAG_SUBFOLDERS => "PidTagSubfolders",
        PID_TAG_FOLDER_TYPE => "PidTagFolderType",
        PID_TAG_CONTAINER_CLASS_W => "PidTagContainerClass",
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => "PidTagDefaultPostMessageClass",
        PID_TAG_MESSAGE_SIZE => "PidTagMessageSize",
        PID_TAG_MESSAGE_CLASS_W => "PidTagMessageClass",
        PID_TAG_SUBJECT_W => "PidTagSubject",
        PID_TAG_NORMALIZED_SUBJECT_A | PID_TAG_NORMALIZED_SUBJECT_W => "PidTagNormalizedSubject",
        PID_TAG_ENTRY_ID => "PidTagEntryId",
        PID_TAG_RECORD_KEY => "PidTagRecordKey",
        PID_TAG_SEARCH_KEY => "PidTagSearchKey",
        PID_TAG_ASSOCIATED => "PidTagAssociated",
        PID_TAG_MID => "PidTagMid",
        PID_TAG_LAST_MODIFICATION_TIME => "PidTagLastModificationTime",
        PID_TAG_ACCESS => "PidTagAccess",
        PID_TAG_SOURCE_KEY => "PidTagSourceKey",
        PID_TAG_PARENT_SOURCE_KEY => "PidTagParentSourceKey",
        PID_TAG_CHANGE_KEY => "PidTagChangeKey",
        PID_TAG_PREDECESSOR_CHANGE_LIST => "PidTagPredecessorChangeList",
        PID_TAG_LOCAL_COMMIT_TIME_MAX => "PidTagLocalCommitTimeMax",
        PID_TAG_DELETED_COUNT_TOTAL => "PidTagDeletedCountTotal",
        PID_TAG_FOLDER_ID => "PidTagFolderId",
        PID_TAG_PARENT_FOLDER_ID => "PidTagParentFolderId",
        PID_TAG_CHANGE_NUMBER => "PidTagChangeNumber",
        META_TAG_IDSET_GIVEN | META_TAG_IDSET_GIVEN_BINARY => "MetaTagIdsetGiven",
        META_TAG_IDSET_READ => "MetaTagIdsetRead",
        META_TAG_IDSET_UNREAD => "MetaTagIdsetUnread",
        META_TAG_CNSET_SEEN => "MetaTagCnsetSeen",
        META_TAG_CNSET_SEEN_FAI => "MetaTagCnsetSeenFAI",
        META_TAG_CNSET_READ => "MetaTagCnsetRead",
        0x6841_0003 => "PidTagViewDescriptorViewMode",
        0x6842_0048 | 0x6842_0102 => "PidTagWlinkGroupHeaderId",
        0x6847_0003 => "PidTagWlinkSaveStamp",
        0x6849_0003 => "PidTagWlinkType",
        0x684A_0003 => "PidTagWlinkFlags",
        0x684B_0102 => "PidTagWlinkOrdinal",
        0x684C_0102 => "PidTagWlinkEntryId",
        0x684D_0102 => "PidTagWlinkRecordKey",
        0x684E_0102 => "PidTagWlinkStoreEntryId",
        0x684F_0048 | 0x684F_0102 => "PidTagWlinkFolderType",
        0x6850_0048 | 0x6850_0102 => "PidTagWlinkGroupClsid",
        0x6851_001F => "PidTagWlinkGroupName",
        0x6852_0003 => "PidTagWlinkSection",
        0x6853_0003 => "PidTagWlinkCalendarColor",
        0x6854_0102 => "PidTagWlinkAddressBookEid",
        0x6890_0102 => "PidTagWlinkClientId",
        0x6891_0102 => "PidTagWlinkAddressBookStoreEid",
        0x6892_0003 => "PidTagWlinkROGroupType",
        0x7C06_0003 => "PidTagRoamingDatatypes",
        0x7C07_0102 => "PidTagRoamingDictionary",
        0x7C08_0102 => "PidTagRoamingXmlStream",
        0x7C09_0102 => "PidTagRoamingBinary",
        tag if matches!(
            tag >> 16,
            0x6802
                | 0x6834
                | 0x6835
                | 0x6836
                | 0x6837
                | 0x6838
                | 0x6839
                | 0x683A
                | 0x683B
                | 0x683C
                | 0x683D
        ) =>
        {
            "CommonViewsOrConfigurationProperty"
        }
        _ => "unknown",
    }
}

pub(super) fn hierarchy_debug_known_parent_source_key(source_key: &[u8]) -> bool {
    source_key == source_key_for_store_id(crate::mapi::identity::ROOT_FOLDER_ID).as_slice()
        || source_key
            == source_key_for_store_id(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID).as_slice()
}

pub(super) fn hierarchy_debug_marker(tag: u32) -> bool {
    matches!(
        tag,
        INCR_SYNC_CHG | INCR_SYNC_STATE_BEGIN | INCR_SYNC_STATE_END | INCR_SYNC_END
    )
}

pub(super) fn content_debug_marker(tag: u32) -> bool {
    matches!(
        tag,
        INCR_SYNC_CHG
            | INCR_SYNC_MESSAGE
            | NEW_ATTACH
            | START_EMBED
            | END_EMBED
            | START_RECIP
            | END_TO_RECIP
            | END_ATTACH
            | INCR_SYNC_PROGRESS_MODE
            | INCR_SYNC_PROGRESS_PER_MSG
            | INCR_SYNC_DEL
            | INCR_SYNC_READ
            | INCR_SYNC_STATE_BEGIN
            | INCR_SYNC_STATE_END
            | INCR_SYNC_END
    )
}

pub(super) fn fast_transfer_marker_debug_name(tag: u32) -> &'static str {
    match tag {
        INCR_SYNC_CHG => "IncrSyncChg",
        INCR_SYNC_MESSAGE => "IncrSyncMessage",
        NEW_ATTACH => "NewAttach",
        START_EMBED => "StartEmbed",
        END_EMBED => "EndEmbed",
        START_RECIP => "StartRecip",
        END_TO_RECIP => "EndToRecip",
        END_ATTACH => "EndAttach",
        INCR_SYNC_DEL => "IncrSyncDel",
        INCR_SYNC_READ => "IncrSyncRead",
        INCR_SYNC_STATE_BEGIN => "IncrSyncStateBegin",
        INCR_SYNC_STATE_END => "IncrSyncStateEnd",
        INCR_SYNC_END => "IncrSyncEnd",
        INCR_SYNC_PROGRESS_MODE => "IncrSyncProgressMode",
        INCR_SYNC_PROGRESS_PER_MSG => "IncrSyncProgressPerMsg",
        _ => "unknown",
    }
}

pub(super) fn parse_debug_fast_transfer_property(
    bytes: &[u8],
    offset: usize,
) -> Result<FastTransferDebugProperty, String> {
    let tag = read_debug_u32(bytes, offset)?;
    let property_type = tag & 0x0000_FFFF;
    let value_start = fast_transfer_property_value_start(bytes, tag, offset + 4)?;
    let (value_start, value_len) = match property_type {
        _ if tag == META_TAG_IDSET_GIVEN => {
            let len = read_debug_u32(bytes, value_start)? as usize;
            (value_start + 4, len)
        }
        0x0002 => (value_start, 2),
        0x0003 => (value_start, 4),
        0x000B => (value_start, 2),
        0x0014 | 0x0040 => (value_start, 8),
        0x0048 => (value_start, 16),
        0x001E | 0x001F | 0x0102 => {
            let len = read_debug_u32(bytes, value_start)? as usize;
            (value_start + 4, len)
        }
        0x101E | 0x101F => {
            let count = read_debug_u32(bytes, value_start)? as usize;
            let mut end = value_start + 4;
            for _ in 0..count {
                let len = read_debug_u32(bytes, end)? as usize;
                end = end.saturating_add(4).saturating_add(len);
                let _ = read_debug_slice(bytes, end.saturating_sub(len), len)?;
            }
            (value_start, end.saturating_sub(value_start))
        }
        _ => {
            return Err(format!(
                "unsupported FastTransfer property type in 0x{tag:08x}"
            ))
        }
    };
    let value = read_debug_slice(bytes, value_start, value_len)?.to_vec();
    Ok(FastTransferDebugProperty {
        tag,
        value,
        next_offset: value_start + value_len,
    })
}

pub(super) fn read_debug_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let slice = read_debug_slice(bytes, offset, 4)?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

pub(super) fn read_debug_slice(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8], String> {
    bytes
        .get(offset..offset.saturating_add(len))
        .ok_or_else(|| format!("FastTransfer atom at offset {offset} overruns stream"))
}

pub(super) fn format_usize_list(values: &[usize]) -> String {
    values
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_property_tags(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_property_value_shapes(shapes: &[(u32, String)]) -> String {
    shapes
        .iter()
        .map(|(tag, shape)| format!("{}:{shape}", property_tag_debug_name(*tag)))
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn replguid_globset_debug_summary(value: &[u8]) -> String {
    format_replguid_globset_debug(value)
}

pub(crate) fn final_sync_state_debug_summary(value: &[u8]) -> String {
    match decode_hierarchy_transfer_debug_summary(value) {
        Ok(summary) => format!(
            "bytes={};property_tags={};expected_order={};idset={};cnset={};cnset_fai={};cnset_read={}",
            value.len(),
            format_property_tags(&summary.final_state_property_tags),
            summary.final_state_expected_property_order_ok,
            summary
                .final_state_idset_given_summary
                .as_deref()
                .unwrap_or("missing"),
            summary
                .final_state_cnset_seen_summary
                .as_deref()
                .unwrap_or("missing"),
            summary
                .final_state_cnset_seen_fai_summary
                .as_deref()
                .unwrap_or("not_applicable"),
            summary
                .final_state_cnset_read_summary
                .as_deref()
                .unwrap_or("not_applicable")
        ),
        Err(error) => format!(
            "bytes={};preview={};parse_error={error}",
            value.len(),
            format_debug_hex(&value[..value.len().min(32)])
        ),
    }
}

pub(crate) fn format_marker_tags(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("{}:0x{tag:08x}", fast_transfer_marker_debug_name(*tag)))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_replguid_globset_debug(value: &[u8]) -> String {
    let preview = format_debug_hex(&value[..value.len().min(32)]);
    let Some(replica_guid) = value.get(..16) else {
        return format!(
            "bytes={};preview={preview};parse_error=missing_replica_guid",
            value.len()
        );
    };

    let (ranges, parse_error) = match decode_globset_ranges(value, 16) {
        Ok(ranges) => (ranges, String::new()),
        Err(error) => (Vec::new(), error),
    };

    let range_summary = ranges
        .iter()
        .take(8)
        .map(|(low, high)| {
            if low == high {
                low.to_string()
            } else {
                format!("{low}-{high}")
            }
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "bytes={};replica_guid={};range_count={};ranges={range_summary};preview={preview};parse_error={parse_error}",
        value.len(),
        format_debug_hex(replica_guid),
        ranges.len()
    )
}

pub(crate) fn replguid_globset_counters(value: &[u8]) -> Result<Vec<u64>, String> {
    if value.len() < 17 {
        return Err("missing_replica_guid".to_string());
    }
    if value[..16] != STORE_REPLICA_GUID {
        return Err("unexpected_replica_guid".to_string());
    }

    let ranges = decode_globset_ranges(value, 16)?;
    let mut counters = BTreeSet::new();
    for (low, high) in ranges {
        for counter in low..=high {
            counters.insert(counter);
        }
    }
    Ok(counters.into_iter().collect())
}

pub(super) fn decode_globset_ranges(
    value: &[u8],
    mut offset: usize,
) -> Result<Vec<(u64, u64)>, String> {
    let mut stack = Vec::new();
    let mut push_lengths = Vec::new();
    let mut ranges = Vec::new();
    while offset < value.len() {
        let command = value[offset];
        offset += 1;
        match command {
            GLOBSET_END_COMMAND => {
                if offset != value.len() {
                    return Err("trailing_bytes_after_end".to_string());
                }
                if !stack.is_empty() {
                    return Err("non_empty_stack_at_end".to_string());
                }
                return Ok(ranges);
            }
            1..=6 => {
                let push_len = command as usize;
                let end = offset.saturating_add(push_len);
                let Some(bytes) = value.get(offset..end) else {
                    return Err("truncated_push".to_string());
                };
                offset = end;
                if stack.len().saturating_add(push_len) > 6 {
                    return Err("push_overflows_globcnt".to_string());
                }
                stack.extend_from_slice(bytes);
                if stack.len() == 6 {
                    let counter = globcnt_slice_to_u64(&stack)
                        .ok_or_else(|| "invalid_push_globcnt".to_string())?;
                    ranges.push((counter, counter));
                    stack.truncate(stack.len().saturating_sub(push_len));
                } else {
                    push_lengths.push(push_len);
                }
            }
            GLOBSET_POP_COMMAND => {
                let Some(pop_len) = push_lengths.pop() else {
                    return Err("pop_without_push".to_string());
                };
                if pop_len > stack.len() {
                    return Err("pop_underflows_stack".to_string());
                }
                stack.truncate(stack.len() - pop_len);
            }
            GLOBSET_BITMASK_COMMAND => {
                if stack.len() != 5 {
                    return Err("bitmask_requires_five_byte_stack".to_string());
                }
                let Some(starting_value) = value.get(offset).copied() else {
                    return Err("truncated_bitmask_start".to_string());
                };
                let Some(bitmask) = value.get(offset + 1).copied() else {
                    return Err("truncated_bitmask".to_string());
                };
                offset += 2;
                let mut values = vec![starting_value];
                for bit in 0..8 {
                    if bitmask & (1 << bit) != 0 {
                        let value = u16::from(starting_value) + 1 + bit;
                        if value > u16::from(u8::MAX) {
                            return Err("bitmask_value_overflow".to_string());
                        }
                        values.push(value as u8);
                    }
                }
                for (low, high) in coalesced_u8_ranges(values) {
                    let mut low_bytes = stack.clone();
                    low_bytes.push(low);
                    let mut high_bytes = stack.clone();
                    high_bytes.push(high);
                    let low = globcnt_slice_to_u64(&low_bytes)
                        .ok_or_else(|| "invalid_bitmask_low".to_string())?;
                    let high = globcnt_slice_to_u64(&high_bytes)
                        .ok_or_else(|| "invalid_bitmask_high".to_string())?;
                    ranges.push((low, high));
                }
            }
            GLOBSET_RANGE_COMMAND => {
                let suffix_len = 6usize.saturating_sub(stack.len());
                let low = value
                    .get(offset..offset.saturating_add(suffix_len))
                    .ok_or_else(|| "truncated_range_low".to_string())?;
                let high: u64 = value
                    .get(offset.saturating_add(suffix_len)..offset.saturating_add(suffix_len * 2))
                    .and_then(|high| {
                        let mut bytes = stack.clone();
                        bytes.extend_from_slice(high);
                        globcnt_slice_to_u64(&bytes)
                    })
                    .ok_or_else(|| "truncated_or_invalid_range_high".to_string())?;
                let mut low_bytes = stack.clone();
                low_bytes.extend_from_slice(low);
                let low = globcnt_slice_to_u64(&low_bytes)
                    .ok_or_else(|| "truncated_or_invalid_range_low".to_string())?;
                if high < low {
                    return Err("invalid_range".to_string());
                }
                ranges.push((low, high));
                offset += suffix_len * 2;
            }
            _ => {
                return Err(format!(
                    "unsupported_command_0x{command:02x}_at_{}",
                    offset - 1
                ))
            }
        }
    }
    Err("missing_end_command".to_string())
}

pub(super) fn globcnt_slice_to_u64(bytes: &[u8]) -> Option<u64> {
    crate::mapi::identity::global_counter_from_globcnt(bytes)
}

pub(super) fn coalesced_u8_ranges(mut values: Vec<u8>) -> Vec<(u8, u8)> {
    values.sort_unstable();
    values.dedup();
    let mut ranges: Vec<(u8, u8)> = Vec::new();
    for value in values {
        match ranges.last_mut() {
            Some((_, high)) if value == high.saturating_add(1) => *high = value,
            _ => ranges.push((value, value)),
        }
    }
    ranges
}

pub(super) fn counter_from_xid(value: &[u8]) -> Option<u64> {
    if value.len() != 22 || value[..16] != STORE_REPLICA_GUID {
        return None;
    }
    crate::mapi::identity::global_counter_from_globcnt(value.get(16..22)?)
}
