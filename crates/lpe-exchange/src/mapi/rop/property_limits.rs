use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn size_limited_specific_properties(
    request: &RopRequest,
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    columns: &[u32],
    unsupported_tags: &[u32],
    custom_values: &HashMap<u32, Vec<u8>>,
    response_size_limit: usize,
) -> Vec<bool> {
    let property_size_limit = request_property_size_limit(request);
    let value_lengths = columns
        .iter()
        .map(|tag| {
            custom_values.get(tag).map(Vec::len).unwrap_or_else(|| {
                serialize_object_property(
                    object,
                    principal,
                    mailboxes,
                    emails,
                    snapshot,
                    get_properties_specific_value_tag(object, *tag),
                )
                .len()
            })
        })
        .collect::<Vec<_>>();
    let mut limited = value_lengths
        .iter()
        .map(|value_len| property_size_limit != 0 && *value_len > property_size_limit)
        .collect::<Vec<_>>();

    if response_size_limit == usize::MAX {
        return limited;
    }

    // [MS-OXCPRPT] 2.2.2.1 keeps PropertySizeLimit=0 bounded by the ROP
    // response buffer. [MS-OXCDATA] 2.4.2 and 3.2 define the per-value
    // NotEnoughMemory fallback that the client can reopen as a stream.
    let typed_columns = columns
        .iter()
        .map(|tag| get_properties_specific_typed_value_tag(object, *tag).is_some())
        .collect::<Vec<_>>();
    let requires_flagged_row = !unsupported_tags.is_empty()
        || limited.iter().any(|value| *value)
        || typed_columns.iter().any(|value| *value);
    if !requires_flagged_row {
        let standard_response_size = 7usize.saturating_add(value_lengths.iter().sum::<usize>());
        if standard_response_size <= response_size_limit {
            return limited;
        }
    }

    let mut flagged_response_size = 7usize;
    for (index, tag) in columns.iter().enumerate() {
        flagged_response_size = flagged_response_size.saturating_add(flagged_property_cell_size(
            value_lengths[index],
            typed_columns[index],
            unsupported_tags.contains(tag),
            limited[index],
        ));
    }
    if flagged_response_size <= response_size_limit {
        return limited;
    }

    let mut candidates = columns
        .iter()
        .enumerate()
        .filter_map(|(index, tag)| {
            if limited[index]
                || unsupported_tags.contains(tag)
                || !specific_property_supports_stream(object, *tag)
            {
                return None;
            }
            let success_size = flagged_property_cell_size(
                value_lengths[index],
                typed_columns[index],
                false,
                false,
            );
            let error_size =
                flagged_property_cell_size(value_lengths[index], typed_columns[index], false, true);
            success_size
                .checked_sub(error_size)
                .filter(|saved| *saved != 0)
                .map(|saved| (index, saved))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|(left_index, left_saved), (right_index, right_saved)| {
        right_saved
            .cmp(left_saved)
            .then_with(|| left_index.cmp(right_index))
    });
    for (index, saved) in candidates {
        if flagged_response_size <= response_size_limit {
            break;
        }
        limited[index] = true;
        flagged_response_size = flagged_response_size.saturating_sub(saved);
    }
    limited
}

fn flagged_property_cell_size(
    value_len: usize,
    typed: bool,
    unsupported: bool,
    size_limited: bool,
) -> usize {
    if size_limited {
        usize::from(typed).saturating_mul(2).saturating_add(5)
    } else if unsupported {
        5
    } else {
        usize::from(typed)
            .saturating_mul(2)
            .saturating_add(1)
            .saturating_add(value_len)
    }
}

fn specific_property_supports_stream(object: Option<&MapiObject>, tag: u32) -> bool {
    matches!(
        get_properties_specific_value_tag(object, tag) & 0x0000_FFFF,
        0x000D | 0x001E | 0x001F | 0x0102
    )
}

pub(super) fn request_property_size_limit(request: &RopRequest) -> usize {
    request
        .payload
        .get(..2)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .map(usize::from)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_response_budget_limits_streamable_values_by_occurrence() {
        let property_tag = 0x9000_0102;
        let request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific.as_u8(),
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: 0u16.to_le_bytes().to_vec(),
        };
        let encoded_binary = {
            let mut value = 20_000u16.to_le_bytes().to_vec();
            value.extend(std::iter::repeat_n(b'A', 20_000));
            value
        };
        let custom_values = HashMap::from([(property_tag, encoded_binary)]);

        let limited = size_limited_specific_properties(
            &request,
            None,
            &AccountPrincipal {
                tenant_id: Uuid::nil(),
                account_id: Uuid::nil(),
                email: "test@example.test".to_string(),
                display_name: "Test".to_string(),
                quota_mb: None,
                quota_used_octets: None,
            },
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            &[property_tag, property_tag],
            &[],
            &custom_values,
            32_761,
        );

        assert_eq!(limited, vec![true, false]);
    }
}
