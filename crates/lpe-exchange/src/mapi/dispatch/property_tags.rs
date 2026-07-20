use super::*;

pub(super) fn normalized_get_properties_request(
    session: &MapiSession,
    request: &RopRequest,
) -> RopRequest {
    let mut normalized = request.clone();
    for (index, property_tag) in request.property_tags().into_iter().enumerate() {
        let offset = 4 + index * 4;
        if let Some(bytes) = normalized.payload.get_mut(offset..offset + 4) {
            bytes.copy_from_slice(
                &session
                    .normalize_named_property_tag(property_tag)
                    .to_le_bytes(),
            );
        }
    }
    normalized
}

pub(super) fn property_ids_match(left: u32, right: u32) -> bool {
    left & 0xffff_0000 == right & 0xffff_0000
}

pub(super) fn common_views_link_row_expected_default(property_tag: u32) -> bool {
    let property_id = property_tag & 0xffff_0000;
    matches!(
        property_id,
        tag if property_ids_match(tag, PID_TAG_WLINK_CALENDAR_COLOR)
            || property_ids_match(tag, PID_TAG_WLINK_ADDRESS_BOOK_EID)
            || property_ids_match(tag, PID_TAG_WLINK_CLIENT_ID)
            || property_ids_match(tag, PID_TAG_WLINK_RO_GROUP_TYPE)
            || property_ids_match(tag, 0x6893_0102)
    )
}
