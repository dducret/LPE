use super::*;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct NspiQueryRowsRequest {
    pub(super) state: Option<[u8; 36]>,
    pub(super) explicit_entry_ids: Vec<u32>,
    pub(super) row_count: usize,
    pub(super) row_count_offset: usize,
    pub(super) property_tags: Option<Vec<u32>>,
}

// [MS-OXCMAPIHTTP] 2.2.5.12.1: QueryRows has a byte-sized presence
// discriminator before both the optional STAT and optional column array.
// Those fields leave the property tags unaligned in real Outlook requests.
pub(super) fn parse_nspi_query_rows_request(
    request_type: &str,
    request: &[u8],
) -> Option<NspiQueryRowsRequest> {
    if !nspi_request_type_is_query_rows(request_type) {
        return None;
    }

    let mut cursor = Cursor::new(request);
    let _flags = cursor.read_u32().ok()?;
    let state = if cursor.read_u8().ok()? != 0 {
        Some(cursor.read_bytes(36).ok()?.try_into().ok()?)
    } else {
        None
    };

    let explicit_table_count = cursor.read_u32().ok()? as usize;
    if explicit_table_count > 100_000
        || explicit_table_count > cursor.remaining().saturating_sub(9) / 4
    {
        return None;
    }
    let mut explicit_entry_ids = Vec::with_capacity(explicit_table_count);
    for _ in 0..explicit_table_count {
        explicit_entry_ids.push(cursor.read_u32().ok()?);
    }

    let row_count_offset = cursor.position();
    let row_count = cursor.read_u32().ok()? as usize;
    let property_tags = if cursor.read_u8().ok()? != 0 {
        let count = cursor.read_u32().ok()? as usize;
        if count > 100_000 || count > cursor.remaining().saturating_sub(4) / 4 {
            return None;
        }
        let mut tags = Vec::with_capacity(count);
        for _ in 0..count {
            tags.push(cursor.read_u32().ok()?);
        }
        Some(tags)
    } else {
        None
    };

    let auxiliary_size = cursor.read_u32().ok()? as usize;
    cursor.read_bytes(auxiliary_size).ok()?;
    if cursor.remaining() != 0 {
        return None;
    }

    Some(NspiQueryRowsRequest {
        state,
        explicit_entry_ids,
        row_count,
        row_count_offset,
        property_tags,
    })
}

pub(super) fn nspi_request_type_is_query_rows(request_type: &str) -> bool {
    request_type
        .trim_matches(|ch: char| ch.is_control() || ch.is_whitespace())
        .eq_ignore_ascii_case("QueryRows")
}

// Preserve the bounded body-shape fallback accepted by earlier LPE releases,
// but only after the documented MAPI/HTTP parser has rejected the request.
pub(super) fn parse_legacy_nspi_query_rows_request(
    request_type: &str,
    request: &[u8],
) -> Option<NspiQueryRowsRequest> {
    if !nspi_request_type_is_query_rows(request_type) {
        return None;
    }
    const FLAGS_BYTES: usize = 4;
    const STAT_BYTES: usize = 36;
    legacy_query_rows_layout_at_offset(request, FLAGS_BYTES + STAT_BYTES).or_else(|| {
        (FLAGS_BYTES + 32..=FLAGS_BYTES + 44)
            .filter(|offset| *offset != FLAGS_BYTES + STAT_BYTES)
            .find_map(|offset| legacy_query_rows_layout_at_offset(request, offset))
    })
}

fn legacy_query_rows_layout_at_offset(
    request: &[u8],
    explicit_table_count_offset: usize,
) -> Option<NspiQueryRowsRequest> {
    let explicit_table_count = u32::from_le_bytes(
        request
            .get(explicit_table_count_offset..explicit_table_count_offset + 4)?
            .try_into()
            .ok()?,
    ) as usize;
    if explicit_table_count > 1_024 {
        return None;
    }
    let table_offset = explicit_table_count_offset.checked_add(4)?;
    let row_count_offset = table_offset.checked_add(explicit_table_count.checked_mul(4)?)?;
    let row_count = u32::from_le_bytes(
        request
            .get(row_count_offset..row_count_offset + 4)?
            .try_into()
            .ok()?,
    ) as usize;
    if row_count > 100_000 {
        return None;
    }
    let mut explicit_entry_ids = Vec::with_capacity(explicit_table_count);
    for index in 0..explicit_table_count {
        let offset = table_offset.checked_add(index.checked_mul(4)?)?;
        let value = u32::from_le_bytes(request.get(offset..offset + 4)?.try_into().ok()?);
        if !nspi_word_looks_like_entry_id(value) {
            return None;
        }
        explicit_entry_ids.push(value);
    }
    Some(NspiQueryRowsRequest {
        state: None,
        explicit_entry_ids,
        row_count,
        row_count_offset,
        property_tags: None,
    })
}
