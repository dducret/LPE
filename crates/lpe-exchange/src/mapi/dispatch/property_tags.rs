use super::*;

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
