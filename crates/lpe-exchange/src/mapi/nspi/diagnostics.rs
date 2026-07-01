use super::*;
use std::collections::BTreeMap;

pub(super) fn log_nspi_dn_to_mid_debug(
    principal: &AccountPrincipal,
    request_type: &str,
    request_id: &str,
    request: &[u8],
    values: &[String],
    matched: &NspiDnToMidMatch,
) {
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "nspi",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = request_type,
        mapi_request_id = request_id,
        request_body_bytes = request.len(),
        requested_value_count = values.len(),
        requested_values = %format_nspi_lookup_values_for_debug(values),
        principal_aliases = %format_nspi_lookup_values_for_debug(&principal_legacy_dn_aliases(principal)),
        matched_mid = %matched.mid.map(|mid| format!("{mid:#010x}")).unwrap_or_default(),
        match_source = matched.source,
        message = "rca debug nspi dn to mid"
    );
}

fn format_nspi_lookup_values_for_debug(values: &[String]) -> String {
    values
        .iter()
        .take(12)
        .map(|value| value.chars().take(180).collect::<String>())
        .collect::<Vec<_>>()
        .join("|")
}

pub(super) fn log_nspi_get_props_debug(
    principal: &AccountPrincipal,
    request: &[u8],
    request_type: &str,
    raw_tag_candidates: &[u32],
    tags: &[u32],
    dropped_tags: &[u32],
    entry: Option<&ExchangeAddressBookEntry>,
) {
    let entry_id = entry
        .map(|entry| nspi_entry_id(principal.account_id, entry))
        .map(|id| format!("{id:#010x}"))
        .unwrap_or_default();
    let entry_kind = entry
        .map(|entry| match entry.entry_kind {
            ExchangeAddressBookEntryKind::Account => "account",
            ExchangeAddressBookEntryKind::Contact => "contact",
            ExchangeAddressBookEntryKind::DistributionList => "distribution_list",
        })
        .unwrap_or("");
    let entry_email = entry.map(|entry| entry.email.as_str()).unwrap_or("");
    let entry_display_name = entry.map(|entry| entry.display_name.as_str()).unwrap_or("");
    let requested_entry_ids = nspi_requested_entry_ids(request);
    let current_rec = nspi_stat_current_rec(request)
        .map(|value| format!("{value:#010x}"))
        .unwrap_or_default();
    let returned_property_tags = if entry.is_some() {
        format_nspi_property_tags_for_debug(tags)
    } else {
        String::new()
    };
    let message = "rca debug mapi nspi get props";
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "nspi",
        mailbox = %principal.email,
        request_type = request_type,
        request_body_bytes = request.len(),
        current_rec = %current_rec,
        requested_entry_ids = %format_nspi_u32_values_for_debug(&requested_entry_ids),
        entry_present = entry.is_some(),
        entry_id = %entry_id,
        entry_kind = entry_kind,
        entry_email = entry_email,
        entry_display_name = entry_display_name,
        requested_property_tag_candidate_count = raw_tag_candidates.len(),
        requested_property_tag_candidates = %format_nspi_property_tags_for_debug(raw_tag_candidates),
        effective_property_tag_count = tags.len(),
        effective_property_tags = %format_nspi_property_tags_for_debug(tags),
        returned_property_tag_count = if entry.is_some() { tags.len() } else { 0 },
        returned_property_tags = %returned_property_tags,
        dropped_property_tag_count = dropped_tags.len(),
        dropped_property_tags = %format_nspi_property_tags_for_debug(dropped_tags),
        dropped_known_unsupported_property_tags = %format_nspi_known_unsupported_property_tags_for_debug(dropped_tags),
        message = message,
    );
}

fn format_nspi_known_unsupported_property_tags_for_debug(tags: &[u32]) -> String {
    tags.iter()
        .filter_map(|tag| nspi_known_unsupported_property_tag_name(*tag).map(|name| (*tag, name)))
        .map(|(tag, name)| format!("{tag:#010x}:{name}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn nspi_raw_property_tag_candidates(request: &[u8]) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= request.len() {
        let tag = u32::from_le_bytes([
            request[offset],
            request[offset + 1],
            request[offset + 2],
            request[offset + 3],
        ]);
        if nspi_word_looks_like_requested_property_tag(tag) && !tags.contains(&tag) {
            tags.push(tag);
        }
        offset += 1;
    }
    tags
}

fn nspi_word_looks_like_requested_property_tag(tag: u32) -> bool {
    let property_id = tag >> 16;
    let property_type = tag & 0xffff;
    property_id != 0
        && matches!(
            property_type,
            0x0002
                | 0x0003
                | 0x0005
                | 0x000A
                | 0x000B
                | 0x0014
                | 0x001E
                | 0x001F
                | 0x0040
                | 0x0048
                | 0x0102
                | 0x1002
                | 0x1003
                | 0x1014
                | 0x101E
                | 0x101F
                | 0x1048
                | 0x1102
        )
}

fn format_nspi_u32_values_for_debug(values: &[u32]) -> String {
    values
        .iter()
        .map(|value| format!("{value:#010x}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_nspi_property_tags_for_debug(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("{tag:#010x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn log_nspi_rowset_debug(
    principal: &AccountPrincipal,
    request: &[u8],
    request_type: &str,
    available_entry_count: usize,
    lookup_values: &[String],
    tags: &[u32],
    entries: &[ExchangeAddressBookEntry],
    row_limit: Option<usize>,
) {
    let requested_entry_ids = nspi_requested_entry_ids(request);
    let current_rec = nspi_stat_current_rec(request)
        .map(|value| format!("{value:#010x}"))
        .unwrap_or_default();
    let row_limit = row_limit.map(|limit| limit.to_string()).unwrap_or_default();
    let query_rows_count = nspi_query_rows_count_details(request_type, request);
    let query_rows_explicit_entry_ids = nspi_query_rows_explicit_entry_ids(request_type, request);
    let query_rows_explicit_table_count = query_rows_count
        .as_ref()
        .map(|details| details.explicit_table_count.to_string())
        .unwrap_or_default();
    let query_rows_count_offset = query_rows_count
        .as_ref()
        .map(|details| details.count_offset.to_string())
        .unwrap_or_default();
    let (duplicate_entry_key_count, duplicate_entry_keys) =
        format_nspi_duplicate_entry_keys_for_debug(entries);
    let message = "rca debug mapi nspi rowset";
    tracing::info!(
        rca_debug = true,
        nspi_rowset_debug_schema = NSPI_ROWSET_DEBUG_SCHEMA,
        adapter = "mapi",
        endpoint = "nspi",
        mailbox = %principal.email,
        request_type = request_type,
        request_type_is_query_rows = nspi_request_type_is_query_rows(request_type),
        request_type_debug = ?request_type,
        request_body_bytes = request.len(),
        request_body_preview_hex = %hex_preview(request, 96),
        current_rec = %current_rec,
        requested_entry_ids = %format_nspi_u32_values_for_debug(&requested_entry_ids),
        lookup_value_count = lookup_values.len(),
        lookup_values = %lookup_values.join(","),
        requested_property_tag_count = tags.len(),
        requested_property_tags = %format_nspi_property_tags_for_debug(tags),
        available_entry_count = available_entry_count,
        returned_entry_count = entries.len(),
        row_limit = %row_limit,
        query_rows_explicit_table_count = %query_rows_explicit_table_count,
        query_rows_explicit_entry_ids = %format_nspi_u32_values_for_debug(&query_rows_explicit_entry_ids),
        query_rows_count_offset = %query_rows_count_offset,
        duplicate_entry_key_count = duplicate_entry_key_count,
        duplicate_entry_keys = %duplicate_entry_keys,
        returned_entries = %format_nspi_entry_summaries_for_debug(principal.account_id, entries),
        message = message,
    );
}

pub(super) fn log_nspi_response_contract(
    principal: &AccountPrincipal,
    request_type: &str,
    request_id: &str,
    method_return_value: u32,
    body: &[u8],
    rowset_present: bool,
    returned_row_count: usize,
    property_tags: &[u32],
    context: &str,
) {
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "nspi",
        mailbox = %principal.email,
        request_type = request_type,
        mapi_request_id = request_id,
        transport_response_code = 0u16,
        method_return_value = %format!("{method_return_value:#010x}"),
        method_return_status = nspi_method_status_name(method_return_value),
        item_not_found_encoded = method_return_value == 0x8004_010f,
        body_contains_item_not_found = nspi_body_contains_status(body, 0x8004_010f),
        rowset_present = rowset_present,
        returned_row_count = returned_row_count,
        property_tag_count = property_tags.len(),
        property_tags = %format_nspi_property_tags_for_debug(property_tags),
        body_bytes = body.len(),
        body_preview_hex = %hex_preview(body, 160),
        context = context,
        message = "rca debug mapi nspi response contract",
    );
}

fn nspi_body_contains_status(body: &[u8], status: u32) -> bool {
    let status = status.to_le_bytes();
    body.windows(status.len()).any(|bytes| bytes == status)
}

fn nspi_method_status_name(value: u32) -> &'static str {
    match value {
        0x0000_0000 => "Success",
        0x0004_03A9 => "ErrorsReturned",
        0x8004_010F => "NotFound",
        0x8004_010B => "InvalidParameter",
        0x8004_0102 => "NotEnoughMemory",
        0x8004_0106 => "InvalidBookmark",
        _ => "Unknown",
    }
}

pub(super) fn format_nspi_entry_summaries_for_debug(
    account_id: Uuid,
    entries: &[ExchangeAddressBookEntry],
) -> String {
    entries
        .iter()
        .map(|entry| {
            let kind = match entry.entry_kind {
                ExchangeAddressBookEntryKind::Account => "account",
                ExchangeAddressBookEntryKind::Contact => "contact",
                ExchangeAddressBookEntryKind::DistributionList => "distribution_list",
            };
            format!(
                "{:#010x}:{}:{}:{}",
                nspi_entry_id(account_id, entry),
                kind,
                entry.email,
                entry.display_name
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(super) fn format_nspi_duplicate_entry_keys_for_debug(
    entries: &[ExchangeAddressBookEntry],
) -> (usize, String) {
    let mut counts = BTreeMap::<String, usize>::new();
    for entry in entries {
        let kind = match entry.entry_kind {
            ExchangeAddressBookEntryKind::Account => "account",
            ExchangeAddressBookEntryKind::Contact => "contact",
            ExchangeAddressBookEntryKind::DistributionList => "distribution_list",
        };
        let key = format!(
            "{}:{}:{}",
            kind,
            entry.email.trim().to_ascii_lowercase(),
            entry.display_name.trim().to_ascii_lowercase()
        );
        *counts.entry(key).or_insert(0) += 1;
    }
    let duplicates = counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(key, count)| format!("{key}x{count}"))
        .collect::<Vec<_>>();
    (duplicates.len(), duplicates.join("|"))
}
