use anyhow::Result;
use axum::http::HeaderMap;
pub(in crate::mapi) use lpe_domain::crypto::hex_lower as bytes_to_hex;
use lpe_mail_auth::AccountPrincipal;
use std::collections::HashMap;
use uuid::Uuid;

use crate::mapi::identity::{
    object_id_from_folder_identifier_bytes, object_id_from_source_key, object_id_from_wire_id,
    ARCHIVE_FOLDER_ID, CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID, CONFLICTS_FOLDER_ID,
    CONTACTS_FOLDER_ID, CONTACTS_SEARCH_FOLDER_ID, CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    CONVERSATION_HISTORY_FOLDER_ID, DEFERRED_ACTION_FOLDER_ID, DOCUMENT_LIBRARIES_FOLDER_ID,
    DRAFTS_FOLDER_ID, FREEBUSY_DATA_FOLDER_ID, IM_CONTACT_LIST_FOLDER_ID, INBOX_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID, JOURNAL_FOLDER_ID, JUNK_FOLDER_ID, LOCAL_FAILURES_FOLDER_ID,
    NOTES_FOLDER_ID, OUTBOX_FOLDER_ID, QUICK_CONTACTS_FOLDER_ID, QUICK_STEP_SETTINGS_FOLDER_ID,
    REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, RSS_FEEDS_FOLDER_ID, SCHEDULE_FOLDER_ID, SEARCH_FOLDER_ID,
    SENT_FOLDER_ID, SERVER_FAILURES_FOLDER_ID, SHORTCUTS_FOLDER_ID, SPOOLER_QUEUE_FOLDER_ID,
    SUGGESTED_CONTACTS_FOLDER_ID, SYNC_ISSUES_FOLDER_ID, TASKS_FOLDER_ID, TODO_SEARCH_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
};
use crate::mapi::nspi::{normalize_nspi_lookup_value, principal_legacy_dn_aliases};
use crate::mapi::properties::{MapiSortOrder, MapiValue};
use crate::mapi::rop::{
    is_rpc_header_ext_rop_buffer, private_logon_response_logon_flags,
    public_folder_logon_response_logon_flags, read_rop_request, rpc_header_ext_payload,
    split_rop_buffer, Cursor, RopLogonRequest,
};
use crate::mapi::session::read_handle_table;
use crate::mapi::session::{MapiObject, MapiSession};
use crate::mapi::store_adapter::MapiAccessPlan;
use crate::mapi::sync::PRIVATE_LOGON_SPECIAL_FOLDER_IDS;
use crate::mapi::tables::role_for_folder_id;
use crate::mapi::transport::{debug_payload_preview_hex, hex_preview, safe_header, MapiEndpoint};
use crate::mapi::wire::RopId;

use super::MAX_ROP_DEBUG_ENTRIES;

mod associated_config;
mod calendar;
mod common_views;
mod default_folders;
mod execute;
mod fast_transfer;
mod message;
mod named_properties;
mod open_folder;
mod post_hierarchy;
mod probes;
mod property_names;
mod property_responses;
mod recipients;
mod special_folders;
mod sync_upload;
mod table_queries;
mod values;

pub(super) use associated_config::*;
pub(super) use calendar::*;
pub(super) use common_views::*;
pub(super) use default_folders::*;
pub(super) use execute::*;
pub(super) use fast_transfer::*;
pub(super) use message::*;
pub(super) use named_properties::*;
pub(super) use open_folder::*;
pub(super) use post_hierarchy::*;
pub(super) use probes::*;
pub(super) use property_names::*;
pub(super) use property_responses::*;
pub(super) use recipients::*;
pub(super) use special_folders::*;
pub(super) use sync_upload::*;
pub(super) use table_queries::*;
pub(super) use values::*;

#[derive(Debug, Default)]
pub(super) struct RopRequestDebugSummary {
    pub(super) full_ids: Vec<u8>,
    pub(super) full_response_handle_indexes: Vec<Option<u8>>,
    pub(super) ids: Vec<u8>,
    pub(super) response_handle_indexes: Vec<Option<u8>>,
    pub(super) ids_csv: String,
    pub(super) names_csv: String,
    pub(super) tail_ids_csv: String,
    pub(super) tail_names_csv: String,
    pub(super) non_release_rops: String,
    pub(super) total_count: usize,
    pub(super) truncated: bool,
    pub(super) all_release: bool,
    pub(super) handle_count: usize,
    pub(super) handle_table_summary: String,
    pub(super) request_payload_bytes: usize,
    pub(super) handle_table_bytes: usize,
    pub(super) raw_frame_count: usize,
    pub(super) raw_frames: String,
    pub(super) extended: bool,
    pub(super) parse_error: String,
}

#[derive(Debug, Default)]
pub(super) struct FirstPostHierarchyProbeDebugSummary {
    pub(super) open_folder_request_count: usize,
    pub(super) open_folder_requests: String,
    pub(super) open_folder_response_shapes: String,
    pub(super) get_properties_specific_request_count: usize,
    pub(super) get_properties_specific_requests: String,
    pub(super) get_properties_specific_response_shapes: String,
    pub(super) set_properties_request_count: usize,
    pub(super) set_properties_requests: String,
    pub(super) set_properties_response_shapes: String,
    pub(super) parse_error: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct OpenFolderProbeRequest {
    pub(super) output_handle_index: u8,
    pub(super) folder_id: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct GetPropertiesSpecificProbeRequest {
    pub(super) input_handle_index: u8,
    pub(super) property_tags: Vec<u32>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct SetPropertiesProbeRequest {
    pub(super) input_handle_index: u8,
    pub(super) property_tags: Vec<u32>,
    pub(super) property_value_shapes: String,
    pub(super) associated_config_stream_summary: String,
    pub(super) default_folder_entry_id_values: String,
    pub(super) parse_error: String,
}

#[derive(Debug, Default)]
pub(super) struct RopResponseDebugSummary {
    pub(super) ids_csv: String,
    pub(super) names_csv: String,
    pub(super) results_csv: String,
    pub(super) count: usize,
    pub(super) handle_count: usize,
    pub(super) handle_table_summary: String,
    pub(super) extended: bool,
    pub(super) buffer_layout: String,
    pub(super) buffer_size_word: String,
    pub(super) response_payload_bytes: usize,
    pub(super) handle_table_bytes: usize,
    pub(super) frames: String,
    pub(super) parse_error: String,
}

#[derive(Debug, Default)]
pub(super) struct LogonResponseDebugSummary {
    pub(super) present: bool,
    pub(super) output_handle_index: String,
    pub(super) error_code: String,
    pub(super) logon_flags: String,
    pub(super) special_folder_ids: String,
    pub(super) special_folder_contract: String,
    pub(super) special_folder_contract_issues: String,
    pub(super) response_flags: String,
    pub(super) mailbox_guid: String,
    pub(super) replid: String,
    pub(super) replica_guid: String,
    pub(super) parse_error: String,
}

pub(in crate::mapi) fn debug_role_for_folder_id(folder_id: u64) -> &'static str {
    role_for_folder_id(folder_id).unwrap_or_else(|| post_hierarchy_probe_folder_name(folder_id))
}

pub(in crate::mapi) fn debug_container_class_for_folder_id(folder_id: u64) -> &'static str {
    match folder_id {
        COMMON_VIEWS_FOLDER_ID | SCHEDULE_FOLDER_ID | VIEWS_FOLDER_ID | FREEBUSY_DATA_FOLDER_ID => {
            ""
        }
        SEARCH_FOLDER_ID => "IPF.Note",
        CONTACTS_SEARCH_FOLDER_ID => "IPF.Contact",
        TODO_SEARCH_FOLDER_ID => "IPF.Task",
        REMINDERS_FOLDER_ID => "Outlook.Reminder",
        RSS_FEEDS_FOLDER_ID => "IPF.Note.OutlookHomepage",
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => "IPF.Configuration",
        QUICK_STEP_SETTINGS_FOLDER_ID => "IPF.Configuration",
        INBOX_FOLDER_ID
        | OUTBOX_FOLDER_ID
        | SENT_FOLDER_ID
        | TRASH_FOLDER_ID
        | DRAFTS_FOLDER_ID
        | JUNK_FOLDER_ID
        | ARCHIVE_FOLDER_ID
        | CONVERSATION_HISTORY_FOLDER_ID => "IPF.Note",
        _ => expected_special_folder_container_class(folder_id),
    }
}

pub(in crate::mapi) fn post_hierarchy_probe_folder_name(folder_id: u64) -> &'static str {
    match folder_id {
        ROOT_FOLDER_ID => "root",
        IPM_SUBTREE_FOLDER_ID => "ipm_subtree",
        DEFERRED_ACTION_FOLDER_ID => "deferred_action",
        SPOOLER_QUEUE_FOLDER_ID => "spooler_queue",
        INBOX_FOLDER_ID => "inbox",
        DRAFTS_FOLDER_ID => "drafts",
        SENT_FOLDER_ID => "sent",
        TRASH_FOLDER_ID => "trash",
        OUTBOX_FOLDER_ID => "outbox",
        COMMON_VIEWS_FOLDER_ID => "common_views",
        SCHEDULE_FOLDER_ID => "schedule",
        SEARCH_FOLDER_ID => "search",
        VIEWS_FOLDER_ID => "personal_views",
        SHORTCUTS_FOLDER_ID => "shortcuts",
        CALENDAR_FOLDER_ID => "calendar",
        CONTACTS_FOLDER_ID => "contacts",
        JOURNAL_FOLDER_ID => "journal",
        NOTES_FOLDER_ID => "notes",
        TASKS_FOLDER_ID => "tasks",
        REMINDERS_FOLDER_ID => "reminders",
        SUGGESTED_CONTACTS_FOLDER_ID => "suggested_contacts",
        QUICK_CONTACTS_FOLDER_ID => "quick_contacts",
        IM_CONTACT_LIST_FOLDER_ID => "im_contact_list",
        CONTACTS_SEARCH_FOLDER_ID => "contacts_search",
        DOCUMENT_LIBRARIES_FOLDER_ID => "document_libraries",
        SYNC_ISSUES_FOLDER_ID => "sync_issues",
        CONFLICTS_FOLDER_ID => "conflicts",
        LOCAL_FAILURES_FOLDER_ID => "local_failures",
        SERVER_FAILURES_FOLDER_ID => "server_failures",
        JUNK_FOLDER_ID => "junk",
        RSS_FEEDS_FOLDER_ID => "rss_feeds",
        TRACKED_MAIL_PROCESSING_FOLDER_ID => "tracked_mail_processing",
        TODO_SEARCH_FOLDER_ID => "todo_search",
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => "conversation_action_settings",
        QUICK_STEP_SETTINGS_FOLDER_ID => "quick_step_settings",
        ARCHIVE_FOLDER_ID => "archive",
        FREEBUSY_DATA_FOLDER_ID => "freebusy_data",
        CONVERSATION_HISTORY_FOLDER_ID => "conversation_history",
        _ => "other",
    }
}

pub(super) fn expected_special_folder_container_class(folder_id: u64) -> &'static str {
    match folder_id {
        CONTACTS_FOLDER_ID
        | SUGGESTED_CONTACTS_FOLDER_ID
        | QUICK_CONTACTS_FOLDER_ID
        | IM_CONTACT_LIST_FOLDER_ID
        | CONTACTS_SEARCH_FOLDER_ID => "IPF.Contact",
        CALENDAR_FOLDER_ID => "IPF.Appointment",
        JOURNAL_FOLDER_ID => "IPF.Journal",
        NOTES_FOLDER_ID => "IPF.StickyNote",
        SEARCH_FOLDER_ID => "IPF.Note",
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => "IPF.Task",
        REMINDERS_FOLDER_ID => "Outlook.Reminder",
        RSS_FEEDS_FOLDER_ID => "IPF.Note.OutlookHomepage",
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => "IPF.Configuration",
        QUICK_STEP_SETTINGS_FOLDER_ID => "IPF.Configuration",
        _ => "",
    }
}

pub(in crate::mapi) fn rop_ids_csv(rop_ids: &[u8]) -> String {
    rop_ids
        .iter()
        .map(|rop_id| rop_id_hex(*rop_id))
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi) fn rop_id_hex(rop_id: u8) -> String {
    format!("0x{rop_id:02x}")
}

pub(super) fn rop_names_csv(rop_ids: &[u8]) -> String {
    rop_ids
        .iter()
        .map(|rop_id| rop_name(*rop_id))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn rop_name(rop_id: u8) -> String {
    match RopId::from_u8(rop_id) {
        Some(id) => format!("{id:?}"),
        None => format!("Unknown0x{rop_id:02x}"),
    }
}

pub(super) fn rop_has_no_response(rop_id: u8) -> bool {
    matches!(rop_id, 0x01)
}

pub(super) fn summarize_non_release_request_rops(rop_ids: &[u8]) -> String {
    rop_ids
        .iter()
        .enumerate()
        .filter(|(_, rop_id)| **rop_id != RopId::Release.as_u8())
        .take(16)
        .map(|(index, rop_id)| format!("{index}:{}", rop_name(*rop_id)))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn summarize_request_rop_raw_frames(requests: &[u8]) -> (usize, String) {
    let mut cursor = Cursor::new(requests);
    let mut frames = Vec::new();
    while cursor.remaining() > 0 && frames.len() < MAX_ROP_DEBUG_ENTRIES {
        let start = cursor.position();
        let rop_id = requests.get(start).copied().unwrap_or_default();
        let logon_id = requests.get(start + 1).copied().unwrap_or_default();
        match read_rop_request(&mut cursor) {
            Ok(request) => {
                let end = cursor.position();
                frames.push(format!(
                    "0x{rop_id:02x}@{start}..{end}:len={}:logon={logon_id}:in={}:out={}:payload={}:preview={}",
                    end.saturating_sub(start),
                    request
                        .input_handle_index
                        .map(|index| index.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    request
                        .output_handle_index
                        .map(|index| index.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    request.payload.len(),
                    hex_preview(&requests[start..end], 16)
                ));
            }
            Err(error) => {
                let offset = cursor.position();
                frames.push(format!(
                    "0x{rop_id:02x}@{start}..{offset}:error={error}:remaining={}:next={}",
                    cursor.remaining(),
                    requests
                        .get(offset..)
                        .map(|bytes| hex_preview(bytes, 16))
                        .unwrap_or_default()
                ));
                break;
            }
        }
    }
    if cursor.remaining() > 0 {
        frames.push(format!(
            "trailing@{}:bytes={}:preview={}",
            cursor.position(),
            cursor.remaining(),
            requests
                .get(cursor.position()..)
                .map(|bytes| hex_preview(bytes, 16))
                .unwrap_or_default()
        ));
    }
    (frames.len(), frames.join("|"))
}

pub(super) fn summarize_handle_table(
    handle_table: &[u8],
    parse_error: &mut String,
) -> (usize, String) {
    match read_handle_table(handle_table) {
        Ok(handles) => {
            let handles_csv = handles
                .iter()
                .map(|handle| format!("0x{handle:08x}"))
                .collect::<Vec<_>>()
                .join(",");
            (
                handles.len(),
                format!("count={};handles={handles_csv}", handles.len()),
            )
        }
        Err(error) => {
            *parse_error = error.to_string();
            let count = handle_table.len() / 4;
            (
                count,
                format!(
                    "invalid;bytes={};best_effort_count={count}",
                    handle_table.len()
                ),
            )
        }
    }
}

pub(super) fn summarize_request_rop_buffer(rop_buffer: &[u8]) -> RopRequestDebugSummary {
    let mut summary = RopRequestDebugSummary {
        extended: is_rpc_header_ext_rop_buffer(rop_buffer),
        all_release: true,
        ..RopRequestDebugSummary::default()
    };
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        summary.parse_error = "invalid ROP buffer".to_string();
        return summary;
    };
    let handle_summary = summarize_handle_table(handle_table, &mut summary.parse_error);
    summary.handle_count = handle_summary.0;
    summary.handle_table_summary = handle_summary.1;
    summary.request_payload_bytes = requests.len();
    summary.handle_table_bytes = handle_table.len();

    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 {
        match read_rop_request(&mut cursor) {
            Ok(request) => {
                let rop_id = request.typed().rop_id();
                summary.total_count += 1;
                summary.all_release &= rop_id == RopId::Release.as_u8();
                summary.full_ids.push(rop_id);
                if !rop_has_no_response(rop_id) {
                    summary
                        .full_response_handle_indexes
                        .push(Some(request.response_handle_index()));
                }
                if summary.ids.len() < MAX_ROP_DEBUG_ENTRIES {
                    summary.ids.push(rop_id);
                    if !rop_has_no_response(rop_id) {
                        summary
                            .response_handle_indexes
                            .push(Some(request.response_handle_index()));
                    }
                } else {
                    summary.truncated = true;
                }
            }
            Err(error) => {
                let offset = cursor.position();
                let remaining = cursor.remaining();
                let preview = requests
                    .get(offset..)
                    .map(|bytes| hex_preview(bytes, 16))
                    .unwrap_or_default();
                summary.parse_error = format!(
                    "{};offset={offset};remaining={remaining};next={preview};parsed_rop_count={}",
                    error, summary.total_count
                );
                summary.all_release = false;
                break;
            }
        }
    }
    summary.ids_csv = rop_ids_csv(&summary.ids);
    summary.names_csv = rop_names_csv(&summary.ids);
    if summary.truncated {
        let tail_start = summary.full_ids.len().saturating_sub(16);
        let tail_ids = &summary.full_ids[tail_start..];
        summary.tail_ids_csv = rop_ids_csv(tail_ids);
        summary.tail_names_csv = rop_names_csv(tail_ids);
    }
    summary.non_release_rops = summarize_non_release_request_rops(&summary.full_ids);
    let raw = summarize_request_rop_raw_frames(requests);
    summary.raw_frame_count = raw.0;
    summary.raw_frames = raw.1;
    summary
}

pub(super) fn summarize_response_rop_buffer(
    rop_buffer: &[u8],
    request_rop_ids: &[u8],
) -> RopResponseDebugSummary {
    summarize_response_rop_buffer_with_optional_expected_handles(rop_buffer, request_rop_ids, None)
}

pub(super) fn summarize_response_rop_buffer_with_expected_handles(
    rop_buffer: &[u8],
    request_rop_ids: &[u8],
    expected_response_handle_indexes: &[Option<u8>],
) -> RopResponseDebugSummary {
    summarize_response_rop_buffer_with_optional_expected_handles(
        rop_buffer,
        request_rop_ids,
        Some(expected_response_handle_indexes),
    )
}

fn summarize_response_rop_buffer_with_optional_expected_handles(
    rop_buffer: &[u8],
    request_rop_ids: &[u8],
    expected_response_handle_indexes: Option<&[Option<u8>]>,
) -> RopResponseDebugSummary {
    let mut summary = RopResponseDebugSummary {
        extended: is_rpc_header_ext_rop_buffer(rop_buffer),
        buffer_layout: rop_buffer_layout_name(rop_buffer).to_string(),
        buffer_size_word: rop_buffer_size_word(rop_buffer)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "invalid".to_string()),
        ..RopResponseDebugSummary::default()
    };
    let Some((responses, handle_table)) = split_rop_buffer(rop_buffer) else {
        summary.parse_error = "invalid ROP buffer".to_string();
        return summary;
    };
    summary.response_payload_bytes = responses.len();
    summary.handle_table_bytes = handle_table.len();
    let handle_summary = summarize_handle_table(handle_table, &mut summary.parse_error);
    summary.handle_count = handle_summary.0;
    summary.handle_table_summary = handle_summary.1;

    let mut offset = 0usize;
    let mut ids = Vec::new();
    let mut results = Vec::new();
    let expected_ids = request_rop_ids
        .iter()
        .copied()
        .filter(|rop_id| !rop_has_no_response(*rop_id))
        .take(MAX_ROP_DEBUG_ENTRIES)
        .collect::<Vec<_>>();
    let mut frames = Vec::new();
    for (expected_index, expected_rop_id) in expected_ids.iter().copied().enumerate() {
        let expected_response_handle_index = expected_response_handle_indexes
            .and_then(|handles| handles.get(expected_index).copied().flatten());
        let next_expected_response_handle_index = expected_response_handle_indexes
            .and_then(|handles| handles.get(expected_index + 1).copied().flatten());
        let Some(found_offset) = next_response_rop_start_from(
            responses,
            offset,
            expected_rop_id,
            expected_response_handle_index,
            expected_ids.get(expected_index + 1).copied(),
        ) else {
            break;
        };
        offset = found_offset;
        let rop_id = responses[offset];
        ids.push(rop_id);
        let error_code = read_response_error_code(responses, offset);
        if let Some(error_code) = error_code {
            results.push(format!("{}:{error_code:#010x}", rop_id_hex(rop_id)));
        } else {
            results.push(format!("{}:truncated", rop_id_hex(rop_id)));
        }
        let next_expected_rop_id = expected_ids.get(expected_index + 1).copied();
        let following_expected_rop_id = expected_ids.get(expected_index + 2).copied();
        let frame_end = response_rop_frame_end(
            responses,
            offset,
            error_code,
            next_expected_rop_id,
            next_expected_response_handle_index,
            following_expected_rop_id,
        );
        frames.push(summarize_response_rop_frame(
            responses, offset, frame_end, error_code,
        ));
        offset = frame_end;
    }

    summary.count = ids.len();
    summary.ids_csv = rop_ids_csv(&ids);
    summary.names_csv = rop_names_csv(&ids);
    summary.results_csv = results.join(",");
    summary.frames = frames.join("|");
    summary
}

fn response_rop_frame_end(
    responses: &[u8],
    start: usize,
    error_code: Option<u32>,
    next_expected_rop_id: Option<u8>,
    next_expected_response_handle_index: Option<u8>,
    following_expected_rop_id: Option<u8>,
) -> usize {
    let rop_id = responses.get(start).copied().unwrap_or_default();
    let fixed_end =
        response_rop_fixed_frame_end(responses, start, rop_id, error_code).or_else(|| {
            match (rop_id, error_code) {
                (0x4F, Some(0)) => match responses.get(start + 7).copied() {
                    Some(0) => Some(start.saturating_add(8)),
                    Some(_) => next_response_rop_start_validated(
                        responses,
                        start.saturating_add(8),
                        next_expected_rop_id,
                        next_expected_response_handle_index,
                        following_expected_rop_id,
                    ),
                    None => None,
                },
                _ => None,
            }
        });
    fixed_end
        .filter(|end| *end <= responses.len())
        .or_else(|| next_response_rop_start(responses, start, next_expected_rop_id))
        .unwrap_or(responses.len())
}

fn response_rop_fixed_frame_end(
    responses: &[u8],
    start: usize,
    rop_id: u8,
    error_code: Option<u32>,
) -> Option<usize> {
    match (rop_id, error_code) {
        (0x02, Some(0)) => Some(start.saturating_add(8)),
        (0x06, Some(0)) => Some(start.saturating_add(7)),
        (0x04 | 0x05 | 0x21, Some(0)) => Some(start.saturating_add(10)),
        (0x0A | 0x79, Some(0)) => responses.get(start + 6..start + 8).and_then(|bytes| {
            let problem_count = u16::from_le_bytes(bytes.try_into().ok()?) as usize;
            Some(
                start
                    .saturating_add(8)
                    .saturating_add(problem_count.saturating_mul(10)),
            )
        }),
        (0x0C, Some(0)) => Some(start.saturating_add(15)),
        (0x12 | 0x13, Some(0)) => Some(start.saturating_add(7)),
        (0x18, Some(0)) => Some(start.saturating_add(11)),
        (0x2B, Some(0)) => Some(start.saturating_add(10)),
        (0x2C, Some(0)) => responses.get(start + 6..start + 8).and_then(|bytes| {
            let byte_count = u16::from_le_bytes(bytes.try_into().ok()?) as usize;
            Some(start.saturating_add(8).saturating_add(byte_count))
        }),
        (0x29, Some(0)) => Some(start.saturating_add(7)),
        (0x49, Some(0)) => responses.get(start + 8..start + 10).and_then(|bytes| {
            let byte_count = u16::from_le_bytes(bytes.try_into().ok()?) as usize;
            Some(start.saturating_add(10).saturating_add(byte_count))
        }),
        (0x56, Some(0)) => responses.get(start + 6..start + 8).and_then(|bytes| {
            let id_count = u16::from_le_bytes(bytes.try_into().ok()?) as usize;
            Some(
                start
                    .saturating_add(8)
                    .saturating_add(id_count.saturating_mul(2)),
            )
        }),
        (_, Some(code)) if code != 0 => Some(start.saturating_add(6)),
        (_, Some(_)) => None,
        (_, None) => None,
    }
}

fn next_response_rop_start_validated(
    responses: &[u8],
    search_start: usize,
    next_expected_rop_id: Option<u8>,
    next_expected_response_handle_index: Option<u8>,
    following_expected_rop_id: Option<u8>,
) -> Option<usize> {
    let next_expected_rop_id = next_expected_rop_id?;
    let mut cursor = search_start;
    while cursor < responses.len() {
        let found = responses
            .get(cursor..)?
            .iter()
            .position(|candidate| *candidate == next_expected_rop_id)?;
        let candidate_start = cursor + found;
        let error_code = read_response_error_code(responses, candidate_start);
        if !error_code.is_some_and(is_plausible_response_return_value) {
            cursor = candidate_start.saturating_add(1);
            continue;
        }
        if !response_handle_index_matches(
            responses,
            candidate_start,
            next_expected_response_handle_index,
        ) {
            cursor = candidate_start.saturating_add(1);
            continue;
        }
        if let Some(end) = response_rop_fixed_frame_end(
            responses,
            candidate_start,
            next_expected_rop_id,
            error_code,
        )
        .filter(|end| *end <= responses.len())
        {
            if following_expected_rop_id
                .and_then(|rop_id| responses.get(end).map(|candidate| *candidate == rop_id))
                .unwrap_or(true)
            {
                return Some(candidate_start);
            }
        }
        cursor = candidate_start.saturating_add(1);
    }
    None
}

fn next_response_rop_start_from(
    responses: &[u8],
    search_start: usize,
    expected_rop_id: u8,
    expected_response_handle_index: Option<u8>,
    following_expected_rop_id: Option<u8>,
) -> Option<usize> {
    let mut cursor = search_start;
    while cursor < responses.len() {
        let found = responses
            .get(cursor..)?
            .iter()
            .position(|candidate| *candidate == expected_rop_id)?;
        let candidate_start = cursor + found;
        let error_code = read_response_error_code(responses, candidate_start);
        if !error_code.is_some_and(is_plausible_response_return_value) {
            cursor = candidate_start.saturating_add(1);
            continue;
        }
        if !response_handle_index_matches(
            responses,
            candidate_start,
            expected_response_handle_index,
        ) {
            cursor = candidate_start.saturating_add(1);
            continue;
        }
        if let Some(end) =
            response_rop_fixed_frame_end(responses, candidate_start, expected_rop_id, error_code)
                .filter(|end| *end <= responses.len())
        {
            if following_expected_rop_id
                .and_then(|rop_id| responses.get(end).map(|candidate| *candidate == rop_id))
                .unwrap_or(true)
            {
                return Some(candidate_start);
            }
        } else {
            return Some(candidate_start);
        }
        cursor = candidate_start.saturating_add(1);
    }
    None
}

fn response_handle_index_matches(
    responses: &[u8],
    start: usize,
    expected_response_handle_index: Option<u8>,
) -> bool {
    expected_response_handle_index
        .and_then(|expected| responses.get(start + 1).map(|actual| *actual == expected))
        .unwrap_or(true)
}

fn next_response_rop_start(
    responses: &[u8],
    start: usize,
    next_expected_rop_id: Option<u8>,
) -> Option<usize> {
    next_expected_rop_id.and_then(|rop_id| {
        let mut cursor = start.saturating_add(1);
        while cursor < responses.len() {
            let found = responses
                .get(cursor..)?
                .iter()
                .position(|candidate| *candidate == rop_id)?;
            let candidate_start = cursor + found;
            if read_response_error_code(responses, candidate_start)
                .is_some_and(is_plausible_response_return_value)
            {
                return Some(candidate_start);
            }
            cursor = candidate_start.saturating_add(1);
        }
        None
    })
}

fn is_plausible_response_return_value(value: u32) -> bool {
    value == 0
        || value <= 0x0000_0fff
        || (0x0004_0001..=0x0004_ffff).contains(&value)
        || (0x8004_0000..=0x8004_ffff).contains(&value)
        || (0x8007_0000..=0x8007_ffff).contains(&value)
}

fn rop_buffer_size_word(rop_buffer: &[u8]) -> Option<u16> {
    let payload = rpc_header_ext_payload(rop_buffer).unwrap_or(rop_buffer);
    let bytes = payload.get(..2)?;
    Some(u16::from_le_bytes(bytes.try_into().ok()?))
}

fn rop_buffer_layout_name(rop_buffer: &[u8]) -> &'static str {
    let Some((responses, _handle_table)) = split_rop_buffer(rop_buffer) else {
        return "invalid";
    };
    let Some(size_word) = rop_buffer_size_word(rop_buffer).map(usize::from) else {
        return "invalid";
    };
    if is_rpc_header_ext_rop_buffer(rop_buffer) {
        if size_word == responses.len().saturating_add(2) {
            "rpc_header_ext_spec"
        } else {
            "rpc_header_ext_unknown"
        }
    } else if size_word == responses.len().saturating_add(2) {
        "spec"
    } else if size_word == responses.len() {
        "legacy"
    } else {
        "unknown"
    }
}

pub(super) fn summarize_logon_response_rop(
    rop_buffer: &[u8],
    request_rop_ids: &[u8],
) -> LogonResponseDebugSummary {
    if !request_rop_ids.contains(&0xFE) {
        return LogonResponseDebugSummary::default();
    }
    let mut summary = LogonResponseDebugSummary {
        present: true,
        ..LogonResponseDebugSummary::default()
    };
    let Some((responses, _handle_table)) = split_rop_buffer(rop_buffer) else {
        summary.parse_error = "invalid ROP buffer".to_string();
        return summary;
    };
    let Some(offset) = responses.iter().position(|rop_id| *rop_id == 0xFE) else {
        summary.parse_error = "missing RopLogon response".to_string();
        return summary;
    };
    let result = (|| -> Result<()> {
        let mut cursor = Cursor::new(&responses[offset..]);
        let rop_id = cursor.read_u8()?;
        if rop_id != 0xFE {
            return Err(anyhow::anyhow!("unexpected ROP response"));
        }
        summary.output_handle_index = cursor.read_u8()?.to_string();
        let error_code = cursor.read_u32()?;
        summary.error_code = format!("{error_code:#010x}");
        if error_code != 0 {
            return Ok(());
        }
        summary.logon_flags = format!("{:#04x}", cursor.read_u8()?);
        let mut folder_ids = Vec::with_capacity(PRIVATE_LOGON_SPECIAL_FOLDER_IDS.len());
        for _ in PRIVATE_LOGON_SPECIAL_FOLDER_IDS {
            let bytes = cursor.read_bytes(8)?;
            let folder_id = object_id_from_wire_id(bytes)
                .unwrap_or_else(|| u64::from_le_bytes(bytes.try_into().unwrap_or_default()));
            folder_ids.push(folder_id);
        }
        summary.special_folder_ids = folder_ids
            .iter()
            .map(|folder_id| format!("{folder_id:#018x}"))
            .collect::<Vec<_>>()
            .join(",");
        summary.special_folder_contract = format_logon_special_folder_contract(&folder_ids);
        summary.special_folder_contract_issues = logon_special_folder_contract_issues(&folder_ids);
        summary.response_flags = format!("{:#04x}", cursor.read_u8()?);
        summary.mailbox_guid = read_guid_le(&mut cursor)?;
        summary.replid = cursor.read_u16()?.to_string();
        summary.replica_guid = bytes_to_hex(cursor.read_bytes(16)?);
        cursor.read_bytes(8)?;
        read_u64(&mut cursor)?;
        cursor.read_u32()?;
        Ok(())
    })();
    if let Err(error) = result {
        summary.parse_error = error.to_string();
    }
    summary
}

fn read_u64(cursor: &mut Cursor<'_>) -> Result<u64> {
    let bytes = cursor.read_bytes(8)?;
    Ok(u64::from_le_bytes(bytes.try_into()?))
}

fn read_guid_le(cursor: &mut Cursor<'_>) -> Result<String> {
    let bytes = cursor.read_bytes(16)?;
    Ok(Uuid::from_bytes_le(bytes.try_into()?).to_string())
}

fn format_logon_special_folder_contract(folder_ids: &[u64]) -> String {
    logon_special_folder_contract_entries(folder_ids)
        .into_iter()
        .map(|(index, name, folder_id, expected)| {
            format!(
                "{index}:{name}=0x{folder_id:016x};expected=0x{expected:016x};matches={}",
                folder_id == expected
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(super) fn logon_special_folder_contract_issues(folder_ids: &[u64]) -> String {
    logon_special_folder_contract_entries(folder_ids)
        .into_iter()
        .filter_map(|(index, name, folder_id, expected)| {
            (folder_id != expected).then(|| {
                format!("{index}:{name}:got=0x{folder_id:016x}:expected=0x{expected:016x}")
            })
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn logon_special_folder_contract_entries(
    folder_ids: &[u64],
) -> Vec<(usize, &'static str, u64, u64)> {
    let names = [
        "root",
        "deferred_action",
        "spooler_queue",
        "ipm_subtree",
        "inbox",
        "outbox",
        "sent",
        "trash",
        "common_views",
        "schedule",
        "search",
        "personal_views",
        "shortcuts",
    ];
    PRIVATE_LOGON_SPECIAL_FOLDER_IDS
        .iter()
        .copied()
        .enumerate()
        .map(|(index, expected)| {
            (
                index,
                names.get(index).copied().unwrap_or("unknown"),
                folder_ids.get(index).copied().unwrap_or(0),
                expected,
            )
        })
        .collect()
}

pub(super) fn read_response_error_code(responses: &[u8], offset: usize) -> Option<u32> {
    let bytes = responses.get(offset + 2..offset + 6)?;
    Some(u32::from_le_bytes(bytes.try_into().ok()?))
}

pub(super) fn execute_response_framing_context(request_rop_ids: &[u8]) -> Option<&'static str> {
    if request_rop_ids.contains(&0x70) || request_rop_ids.contains(&0x4E) {
        return Some("hierarchy_sync");
    }
    if request_rop_ids
        .iter()
        .all(|rop_id| matches!(*rop_id, 0x01 | 0x02 | 0x07 | 0x49 | 0x56))
        && (request_rop_ids.contains(&0x49) || request_rop_ids.contains(&0x56))
        && request_rop_ids.contains(&0x02)
        && request_rop_ids.contains(&0x07)
    {
        return Some("named_props_openfolder_getprops");
    }
    if request_rop_ids
        .iter()
        .all(|rop_id| matches!(*rop_id, 0x0A | 0x79))
        && request_rop_ids
            .iter()
            .any(|rop_id| matches!(*rop_id, 0x0A | 0x79))
    {
        return Some("setprops");
    }
    if request_rop_ids
        .iter()
        .all(|rop_id| matches!(*rop_id, 0x01 | 0x03 | 0x07 | 0x0A | 0x79))
        && request_rop_ids.contains(&0x03)
        && request_rop_ids.contains(&0x07)
        && request_rop_ids
            .iter()
            .any(|rop_id| matches!(*rop_id, 0x0A | 0x79))
    {
        return Some("open_message_getprops_setprops");
    }
    if request_rop_ids
        .iter()
        .all(|rop_id| matches!(*rop_id, 0x01 | 0x06 | 0x07 | 0x0A | 0x0C | 0x29 | 0x79))
        && request_rop_ids.contains(&0x06)
        && request_rop_ids.contains(&0x0C)
        && request_rop_ids
            .iter()
            .any(|rop_id| matches!(*rop_id, 0x0A | 0x79))
    {
        return Some("create_message_setprops_save");
    }
    if request_rop_ids
        .iter()
        .all(|rop_id| matches!(*rop_id, 0x01 | 0x05 | 0x12 | 0x13 | 0x18 | 0x4F | 0x56))
        && request_rop_ids.contains(&0x05)
        && request_rop_ids.contains(&0x4F)
    {
        return Some("contents_table_probe");
    }
    if request_rop_ids.contains(&0x05)
        && (request_rop_ids.contains(&0x15) || request_rop_ids.contains(&0x4F))
    {
        return Some("contents_table_batch");
    }
    if request_rop_ids
        .iter()
        .all(|rop_id| matches!(*rop_id, 0x01 | 0x07))
        && request_rop_ids.contains(&0x07)
    {
        return Some("getprops_or_release_getprops");
    }
    if request_rop_ids
        .iter()
        .all(|rop_id| matches!(*rop_id, 0x01 | 0x02 | 0x07))
        && request_rop_ids.contains(&0x02)
        && request_rop_ids.contains(&0x07)
    {
        return Some("openfolder_getprops_probe");
    }
    if request_rop_ids.iter().all(|rop_id| matches!(*rop_id, 0x01))
        && request_rop_ids.contains(&0x01)
    {
        return Some("release_only");
    }
    None
}

pub(super) fn summarize_response_rop_frame(
    responses: &[u8],
    start: usize,
    end: usize,
    error_code: Option<u32>,
) -> String {
    let rop_id = responses.get(start).copied().unwrap_or_default();
    let output_handle_index = responses
        .get(start + 1)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    let result = error_code
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let preview_end = end.min(start.saturating_add(16));
    let preview = responses
        .get(start..preview_end)
        .map(bytes_to_hex)
        .unwrap_or_default();
    format!(
        "{}@{}..{}:len={}:out={}:rv={}:preview={}",
        rop_id_hex(rop_id),
        start,
        end,
        end.saturating_sub(start),
        output_handle_index,
        result,
        preview
    )
}

pub(super) fn execute_batch_has_same_save_getprops_not_found(
    request: &RopRequestDebugSummary,
    response: &RopResponseDebugSummary,
) -> bool {
    request.ids.contains(&0x06)
        && request.ids.contains(&0x0c)
        && request.ids.last().copied() == Some(0x07)
        && response.results_csv.contains("0x0c:0x00000000")
        && response.results_csv.ends_with("0x07:0x8004010f")
}

pub(super) fn format_debug_property_tags(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("{tag:#010x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_debug_sort_orders(sort_orders: &[MapiSortOrder]) -> String {
    sort_orders
        .iter()
        .map(|order| format!("{:#010x}:{}", order.property_tag, order.order))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_expected_folder_id_for_debug(folder_id: u64) -> String {
    if folder_id == 0 {
        "empty".to_string()
    } else {
        format!("0x{folder_id:016x}")
    }
}

pub(super) fn log_rop_logon_request_identity(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &RopLogonRequest,
) {
    let essdn = decode_logon_identity_bytes(&request.essdn);
    let normalized_essdn = normalize_nspi_lookup_value(&essdn);
    let aliases = principal_legacy_dn_aliases(principal);
    let essdn_matches_principal = !normalized_essdn.is_empty()
        && aliases
            .iter()
            .any(|alias| normalized_essdn == alias.to_ascii_lowercase());
    let logon_shape = format_logon_request_shape(request);
    let open_flags = logon_open_flags(request);
    let store_state = logon_store_state(request);
    let projected_response_logon_flags = projected_logon_response_flags(request.logon_flags);
    let dropped_request_logon_bits = request.logon_flags & !projected_response_logon_flags;

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0xfe",
        mapi_request_id = request_id,
        logon_flags = %format!("{:#04x}", request.logon_flags),
        logon_output_handle_index = request.output_handle_index,
        logon_prefix_bytes = request.prefix.len(),
        logon_prefix = %bytes_to_hex(&request.prefix),
        logon_open_flags = %format!("{open_flags:#010x}"),
        logon_store_state = %format!("{store_state:#010x}"),
        projected_response_logon_flags = %format!("{projected_response_logon_flags:#04x}"),
        dropped_request_logon_bits = %format!("{dropped_request_logon_bits:#04x}"),
        observed_outlook_logon_flags_0x09_path = request.logon_flags == 0x09,
        logon_request_shape = %logon_shape,
        logon_essdn = %essdn,
        logon_essdn_bytes = request.essdn.len(),
        principal_aliases = %aliases.join("|"),
        essdn_matches_principal,
        message = "rca debug mapi logon request identity"
    );
}

fn logon_open_flags(request: &RopLogonRequest) -> u32 {
    request
        .prefix
        .get(0..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .unwrap_or(0)
}

fn logon_store_state(request: &RopLogonRequest) -> u32 {
    request
        .prefix
        .get(4..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .unwrap_or(0)
}

fn projected_logon_response_flags(request_logon_flags: u8) -> u8 {
    if request_logon_flags & 0x01 != 0 {
        private_logon_response_logon_flags(request_logon_flags)
    } else {
        public_folder_logon_response_logon_flags(request_logon_flags)
    }
}

fn format_logon_request_shape(request: &RopLogonRequest) -> String {
    let projected_response_logon_flags = projected_logon_response_flags(request.logon_flags);
    let dropped_request_logon_bits = request.logon_flags & !projected_response_logon_flags;
    format!(
        "request_flags={:#04x};private={};open_flags={:#010x};store_state={:#010x};projected_response_flags={:#04x};dropped_request_bits={:#04x};observed_0x09_path={}",
        request.logon_flags,
        request.logon_flags & 0x01 != 0,
        logon_open_flags(request),
        logon_store_state(request),
        projected_response_logon_flags,
        dropped_request_logon_bits,
        request.logon_flags == 0x09
    )
}

#[cfg(test)]
mod logon_request_shape_tests {
    use super::*;

    #[test]
    fn formats_observed_outlook_logon_flags_0x09_path() {
        let request = RopLogonRequest {
            output_handle_index: 0,
            logon_flags: 0x09,
            prefix: vec![0x40, 0x00, 0x00, 0x21, 0x00, 0x00, 0x00, 0x00],
            essdn: Vec::new(),
        };

        assert_eq!(
            format_logon_request_shape(&request),
            "request_flags=0x09;private=true;open_flags=0x21000040;store_state=0x00000000;projected_response_flags=0x09;dropped_request_bits=0x00;observed_0x09_path=true"
        );
    }

    #[test]
    fn formats_initial_private_logon_open_flags_without_dropped_bits() {
        let request = RopLogonRequest {
            output_handle_index: 0,
            logon_flags: 0x01,
            prefix: vec![0x0c, 0x04, 0x00, 0x21, 0x00, 0x00, 0x00, 0x00],
            essdn: Vec::new(),
        };

        assert_eq!(
            format_logon_request_shape(&request),
            "request_flags=0x01;private=true;open_flags=0x2100040c;store_state=0x00000000;projected_response_flags=0x01;dropped_request_bits=0x00;observed_0x09_path=false"
        );
    }
}

fn decode_logon_identity_bytes(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    let bytes = bytes.split(|byte| *byte == 0).next().unwrap_or(bytes);
    String::from_utf8_lossy(bytes).trim().to_string()
}

pub(super) fn log_outlook_bootstrap_phase(
    principal: &AccountPrincipal,
    phase: &str,
    rop_id: &str,
    folder_id: Option<u64>,
    associated: bool,
    table_total_row_count: Option<u32>,
    returned_row_count: Option<u32>,
    output_handle: Option<u32>,
    default_folder_ids: &str,
) {
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = rop_id,
        phase,
        folder_id = %folder_id
            .map(|folder_id| format!("0x{folder_id:016x}"))
            .unwrap_or_default(),
        folder_role = folder_id.map(debug_role_for_folder_id).unwrap_or(""),
        associated,
        table_total_row_count = table_total_row_count.unwrap_or(0),
        returned_row_count = returned_row_count.unwrap_or(0),
        output_handle = output_handle.unwrap_or(0),
        default_folder_ids,
        "rca debug outlook bootstrap phase"
    );
}

pub(super) fn log_outlook_bootstrap_row_invariant(
    principal: &AccountPrincipal,
    phase: &str,
    folder_id: u64,
    associated: bool,
    summary: &str,
) {
    if summary.contains("issues=none") {
        tracing::debug!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x15",
            phase,
            folder_id = %format!("0x{folder_id:016x}"),
            folder_role = debug_role_for_folder_id(folder_id),
            associated,
            row_invariant_summary = summary,
            "rca debug outlook bootstrap row invariant"
        );
    } else {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x15",
            phase,
            folder_id = %format!("0x{folder_id:016x}"),
            folder_role = debug_role_for_folder_id(folder_id),
            associated,
            row_invariant_summary = summary,
            "rca debug outlook bootstrap row invariant"
        );
    }
}

pub(super) fn log_execute_request_start_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
    request_body_bytes: usize,
    request_rop_buffer: &[u8],
    request: &RopRequestDebugSummary,
) {
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let message = "rca debug mapi execute request start";

    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        client_request_id = %client_request_id,
        client_application = %client_application,
        client_info = %client_info,
        trace_id = %trace_id,
        package_name = crate::build_info::PACKAGE_NAME,
        package_version = crate::build_info::PACKAGE_VERSION,
        git_commit = crate::build_info::GIT_COMMIT,
        git_commit_full = crate::build_info::GIT_COMMIT_FULL,
        git_commit_time = crate::build_info::GIT_COMMIT_TIME,
        git_dirty = crate::build_info::GIT_DIRTY,
        build_unix_time = crate::build_info::BUILD_UNIX_TIME,
        target = crate::build_info::TARGET,
        profile = crate::build_info::PROFILE,
        body_bytes = request_body_bytes,
        request_rop_buffer_bytes = request_rop_buffer.len(),
        rop_ids = %request.ids_csv,
        rop_names = %request.names_csv,
        rop_count = request.total_count,
        rop_debug_entry_count = request.ids.len(),
        rop_debug_truncated = request.truncated,
        rop_tail_ids = %request.tail_ids_csv,
        rop_tail_names = %request.tail_names_csv,
        non_release_rops = %request.non_release_rops,
        all_rops_are_release = request.all_release,
        handle_count = request.handle_count,
        handle_table = %request.handle_table_summary,
        extended = request.extended,
        parse_error = %request.parse_error,
        message = message,
    );
}

pub(super) fn log_execute_store_access_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    _headers: &HeaderMap,
    request_id: &str,
    access_plan: &MapiAccessPlan,
) {
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let message = "rca debug mapi execute store access";

    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        full_snapshot = access_plan.requires_full_snapshot,
        object_id_count = access_plan.object_ids.len(),
        object_ids = %format_debug_object_ids(&access_plan.object_ids),
        content_query_count = access_plan.content_queries.len(),
        message = message,
    );
}

pub(in crate::mapi) fn format_debug_object_ids(object_ids: &[u64]) -> String {
    object_ids
        .iter()
        .map(|object_id| format!("{object_id:#018x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_optional_debug_handle(handle: Option<u32>) -> String {
    handle
        .map(|handle| handle.to_string())
        .unwrap_or_else(|| "none".to_string())
}

pub(super) fn format_handle_lineage_context(object: Option<&MapiObject>) -> String {
    match object {
        Some(MapiObject::Logon) => "logon=private_mailbox".to_string(),
        Some(MapiObject::PublicFolderLogon) => "logon=public_folder".to_string(),
        Some(MapiObject::Folder { folder_id, .. }) => format!(
            "folder=0x{folder_id:016x};role={};container={}",
            debug_role_for_folder_id(*folder_id),
            debug_container_class_for_folder_id(*folder_id)
        ),
        Some(object) => object
            .folder_id()
            .map(|folder_id| {
                format!(
                    "parent_folder=0x{folder_id:016x};parent_role={}",
                    debug_role_for_folder_id(folder_id)
                )
            })
            .unwrap_or_else(|| "context=none".to_string()),
        None => "context=missing_handle".to_string(),
    }
}

pub(super) fn mapi_object_debug_kind(object: Option<&MapiObject>) -> &'static str {
    match object {
        None => "none",
        Some(MapiObject::Logon) => "logon",
        Some(MapiObject::PublicFolderLogon) => "public_folder_logon",
        Some(MapiObject::Folder { .. }) => "folder",
        Some(MapiObject::Message { .. }) => "message",
        Some(MapiObject::Contact { .. }) => "contact",
        Some(MapiObject::Event { .. }) => "event",
        Some(MapiObject::Task { .. }) => "task",
        Some(MapiObject::Note { .. }) => "note",
        Some(MapiObject::JournalEntry { .. }) => "journal_entry",
        Some(MapiObject::ConversationAction { .. }) => "conversation_action",
        Some(MapiObject::NavigationShortcut { .. }) => "navigation_shortcut",
        Some(MapiObject::CommonViewNamedView { .. }) => "common_view_named_view",
        Some(MapiObject::SearchFolderDefinitionMessage { .. }) => {
            "search_folder_definition_message"
        }
        Some(MapiObject::AssociatedConfig { .. }) => "associated_config",
        Some(MapiObject::DelegateFreeBusyMessage { .. }) => "delegate_freebusy_message",
        Some(MapiObject::RecoverableItem { .. }) => "recoverable_item",
        Some(MapiObject::PublicFolderItem { .. }) => "public_folder_item",
        Some(MapiObject::PendingMessage { .. }) => "pending_message",
        Some(MapiObject::PendingAssociatedMessage { .. }) => "pending_associated_message",
        Some(MapiObject::PendingContact { .. }) => "pending_contact",
        Some(MapiObject::PendingEvent { .. }) => "pending_event",
        Some(MapiObject::PendingTask { .. }) => "pending_task",
        Some(MapiObject::PendingNote { .. }) => "pending_note",
        Some(MapiObject::PendingJournalEntry { .. }) => "pending_journal_entry",
        Some(MapiObject::PendingConversationAction { .. }) => "pending_conversation_action",
        Some(MapiObject::PendingNavigationShortcut { .. }) => "pending_navigation_shortcut",
        Some(MapiObject::HierarchyTable { .. }) => "hierarchy_table",
        Some(MapiObject::ContentsTable { .. }) => "contents_table",
        Some(MapiObject::AttachmentTable { .. }) => "attachment_table",
        Some(MapiObject::PermissionTable { .. }) => "permission_table",
        Some(MapiObject::RuleTable { .. }) => "rule_table",
        Some(MapiObject::Attachment { .. }) => "attachment",
        Some(MapiObject::PendingAttachment { .. }) => "pending_attachment",
        Some(MapiObject::SavedAttachment { .. }) => "saved_attachment",
        Some(MapiObject::AttachmentStream { .. }) => "attachment_stream",
        Some(MapiObject::NotificationSubscription { .. }) => "notification_subscription",
        Some(MapiObject::SynchronizationSource { .. }) => "synchronization_source",
        Some(MapiObject::SynchronizationCollector { .. }) => "synchronization_collector",
        Some(MapiObject::FastTransferDestination { .. }) => "fast_transfer_destination",
    }
}

pub(super) fn mapi_object_debug_folder_id(object: Option<&MapiObject>) -> String {
    object
        .and_then(MapiObject::folder_id)
        .map(|folder_id| format!("0x{folder_id:016x}"))
        .unwrap_or_else(|| "none".to_string())
}

pub(super) fn format_live_handle_debug_summary(session: &MapiSession) -> String {
    session
        .handles
        .iter()
        .map(|(handle, object)| {
            let folder = object
                .folder_id()
                .map(|folder_id| {
                    format!(
                        "folder=0x{folder_id:016x};role={};container={}",
                        debug_role_for_folder_id(folder_id),
                        debug_container_class_for_folder_id(folder_id)
                    )
                })
                .unwrap_or_else(|| "folder=;role=;container=".to_string());
            let table = match object {
                MapiObject::ContentsTable {
                    associated,
                    position,
                    columns,
                    sort_orders,
                    ..
                } => format!(
                    ";associated={associated};position={position};columns={};sort={}",
                    format_debug_property_tags(columns),
                    format_debug_sort_orders(sort_orders)
                ),
                MapiObject::HierarchyTable {
                    position,
                    columns,
                    sort_orders,
                    ..
                } => format!(
                    ";position={position};columns={};sort={}",
                    format_debug_property_tags(columns),
                    format_debug_sort_orders(sort_orders)
                ),
                _ => String::new(),
            };
            format!(
                "handle={handle};kind={};{}{}",
                mapi_object_debug_kind(Some(object)),
                folder,
                table
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}
