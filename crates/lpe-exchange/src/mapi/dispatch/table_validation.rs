use super::*;

pub(in crate::mapi::dispatch) fn property_tags_are_supported(property_tags: &[u32]) -> bool {
    property_tags.iter().all(|tag| {
        let property_type = (*tag & 0xFFFF) as u16;
        property_type == 0 || MapiPropertyTag::new(*tag).property_type().is_some()
    })
}

pub(in crate::mapi::dispatch) fn property_tags_have_known_wire_types(
    property_tags: &[u32],
) -> bool {
    property_tags.iter().all(|tag| {
        let property_type = (*tag & 0xFFFF) as u16;
        property_type == 0
            || MapiPropertyTag::new(*tag).property_type().is_some()
            || MapiPropertyType::known_unsupported_name(property_type).is_some()
    })
}

pub(in crate::mapi::dispatch) fn restrict_supported_on_object(object: &MapiObject) -> bool {
    matches!(
        object,
        MapiObject::HierarchyTable { .. }
            | MapiObject::ContentsTable { .. }
            | MapiObject::RuleTable { .. }
    )
}

pub(in crate::mapi::dispatch) fn sort_table_request_is_valid(request: &RopRequest) -> bool {
    let Some(flags) = request.payload.first().copied() else {
        return false;
    };
    if flags & !0x01 != 0 {
        return false;
    }
    let Some(count_bytes) = request.payload.get(1..3) else {
        return false;
    };
    let sort_order_count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]);
    let category_count = request.sort_category_count();
    let expanded_count = request.sort_expanded_count();
    if category_count > sort_order_count || expanded_count > category_count {
        return false;
    }
    if request.payload.len() != 7 + usize::from(sort_order_count) * 5 {
        return false;
    }
    let sort_orders = request.sort_orders();
    if !sort_orders.iter().all(sort_order_is_valid) {
        return false;
    }
    if !maximum_category_sort_order_is_valid(&sort_orders, category_count) {
        return false;
    }
    sort_orders
        .iter()
        .take(usize::from(category_count))
        .filter(|sort_order| sort_order.property_tag & 0x0000_1000 != 0)
        .count()
        <= 1
}

fn sort_order_is_valid(sort_order: &MapiSortOrder) -> bool {
    if !matches!(sort_order.order, 0x00 | 0x01 | 0x04) {
        return false;
    }
    let property_type = sort_order.property_tag & 0x0000_FFFF;
    let multivalue = property_type & 0x1000 != 0;
    let multivalue_instance = property_type & 0x2000 != 0;
    multivalue == multivalue_instance
}

fn maximum_category_sort_order_is_valid(
    sort_orders: &[MapiSortOrder],
    category_count: u16,
) -> bool {
    let mut maximum_category_indexes = sort_orders
        .iter()
        .enumerate()
        .filter_map(|(index, sort_order)| (sort_order.order == 0x04).then_some(index));
    let Some(index) = maximum_category_indexes.next() else {
        return true;
    };
    maximum_category_indexes.next().is_none()
        && category_count > 0
        && index == usize::from(category_count)
}

pub(in crate::mapi::dispatch) fn get_attachment_table_flags_are_valid(
    request: &RopRequest,
) -> bool {
    matches!(request.payload.first().copied().unwrap_or(0), 0x00 | 0x40)
}

pub(in crate::mapi::dispatch) fn hierarchy_table_flags_are_valid(request: &RopRequest) -> bool {
    let flags = request.payload.first().copied().unwrap_or(0);
    flags & !0xFC == 0
}

pub(in crate::mapi::dispatch) fn contents_table_flags_error(
    flags: u8,
    folder_id: u64,
    is_public_folder: bool,
) -> Option<u32> {
    if flags & !0xFA != 0 {
        return Some(0x8007_0057);
    }
    if flags & 0x20 != 0 {
        return Some(0x8004_0102);
    }
    if flags & 0x80 != 0 {
        let has_required_bits = flags & 0x48 == 0x48;
        let valid_scope =
            matches!(folder_id, ROOT_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID) || is_public_folder;
        if !has_required_bits || !valid_scope {
            return Some(0x8007_0057);
        }
    }
    None
}

pub(in crate::mapi::dispatch) fn open_attachment_flags_are_valid(request: &RopRequest) -> bool {
    matches!(request.payload.first().copied(), Some(0x00 | 0x01 | 0x03))
}

pub(in crate::mapi::dispatch) fn save_flags_are_supported(request: &RopRequest) -> bool {
    let flags = request.payload.first().copied().unwrap_or(0);
    flags & 0x03 != 0x03
}

pub(in crate::mapi::dispatch) fn table_async_flags_are_valid(request: &RopRequest) -> bool {
    request
        .payload
        .first()
        .is_some_and(|flags| flags & !0x01 == 0)
}

pub(in crate::mapi::dispatch) fn set_columns_request_is_valid(request: &RopRequest) -> bool {
    table_async_flags_are_valid(request)
        && !request.property_tags().is_empty()
        && set_columns_property_tags_are_valid(&request.property_tags())
}

pub(in crate::mapi::dispatch) fn set_columns_request_is_valid_for_rule_table(
    request: &RopRequest,
) -> bool {
    table_async_flags_are_valid(request)
        && !request.property_tags().is_empty()
        && set_columns_property_tags_are_valid_for_rule_table(&request.property_tags())
}

fn set_columns_property_tags_are_valid(property_tags: &[u32]) -> bool {
    property_tags.iter().all(|tag| {
        let property_type = (*tag & 0xFFFF) as u16;
        !matches!(property_type, 0x0000 | 0x000A)
            && !matches!(*tag, PID_TAG_RULE_CONDITION | PID_TAG_RULE_ACTIONS)
            && (MapiPropertyTag::new(*tag).property_type().is_some()
                || MapiPropertyType::known_unsupported_name(property_type).is_some())
    })
}

fn set_columns_property_tags_are_valid_for_rule_table(property_tags: &[u32]) -> bool {
    property_tags.iter().all(|tag| {
        // MS-OXPROPS sections 2.946 and 2.948 define these rule-table columns as
        // PtypRuleAction/PtypRestriction; they are valid column tags only here.
        matches!(*tag, PID_TAG_RULE_ACTIONS | PID_TAG_RULE_CONDITION)
            || set_columns_property_tags_are_valid(&[*tag])
    })
}

pub(in crate::mapi::dispatch) fn format_unknown_wire_type_property_tags(
    property_tags: &[u32],
) -> String {
    property_tags
        .iter()
        .copied()
        .filter(|tag| {
            let property_type = (*tag & 0xFFFF) as u16;
            property_type != 0
                && MapiPropertyTag::new(*tag).property_type().is_none()
                && MapiPropertyType::known_unsupported_name(property_type).is_none()
        })
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod property_tag_validation_tests {
    use super::*;

    #[test]
    fn set_columns_accepts_multi_value_instance_property_types() {
        assert!(property_tags_have_known_wire_types(&[0x8031_3003]));
        assert_eq!(format_unknown_wire_type_property_tags(&[0x8031_3003]), "");
    }

    #[test]
    fn set_columns_rejects_microsoft_invalid_column_property_types() {
        assert!(set_columns_property_tags_are_valid(&[0x8031_3003]));
        assert!(!set_columns_property_tags_are_valid(&[0x0037_0000]));
        assert!(!set_columns_property_tags_are_valid(&[0x0037_000A]));
        assert!(!set_columns_property_tags_are_valid(&[0x0037_801D]));
        assert!(set_columns_property_tags_are_valid(&[0x0037_000D]));
        assert!(!set_columns_property_tags_are_valid(&[
            PID_TAG_RULE_CONDITION,
            PID_TAG_RULE_ACTIONS
        ]));
    }

    #[test]
    fn rule_table_set_columns_accepts_documented_rule_complex_types() {
        assert!(set_columns_property_tags_are_valid_for_rule_table(&[
            PID_TAG_RULE_CONDITION,
            PID_TAG_RULE_ACTIONS,
            PID_TAG_RULE_NAME_W,
        ]));
        assert!(!set_columns_property_tags_are_valid_for_rule_table(&[
            PID_TAG_RULE_CONDITION,
            0x0037_801D
        ]));
    }

    #[test]
    fn set_columns_request_validation_matches_microsoft_flags_and_count() {
        fn request(flags: u8, property_tags: &[u32]) -> RopRequest {
            let mut payload = vec![flags];
            payload.extend_from_slice(&(property_tags.len() as u16).to_le_bytes());
            for tag in property_tags {
                payload.extend_from_slice(&tag.to_le_bytes());
            }
            RopRequest {
                rop_id: RopId::SetColumns.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload,
            }
        }

        assert!(set_columns_request_is_valid(&request(
            0x00,
            &[PID_TAG_SUBJECT_W]
        )));
        assert!(set_columns_request_is_valid(&request(
            0x01,
            &[PID_TAG_SUBJECT_W]
        )));
        assert!(!set_columns_request_is_valid(&request(
            0x02,
            &[PID_TAG_SUBJECT_W]
        )));
        assert!(!set_columns_request_is_valid(&request(0x00, &[])));
        assert!(!set_columns_request_is_valid(&request(
            0x00,
            &[0x0037_000A]
        )));
    }

    #[test]
    fn restrict_flags_validation_matches_microsoft_async_flags() {
        fn request(flags: u8) -> RopRequest {
            RopRequest {
                rop_id: RopId::Restrict.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload: vec![flags, 0, 0],
            }
        }

        assert!(table_async_flags_are_valid(&request(0x00)));
        assert!(table_async_flags_are_valid(&request(0x01)));
        assert!(!table_async_flags_are_valid(&request(0x02)));
        assert!(!table_async_flags_are_valid(&request(0x80)));
    }

    #[test]
    fn restrict_support_matches_microsoft_table_scope() {
        let contents = MapiObject::ContentsTable {
            folder_id: INBOX_FOLDER_ID,
            associated: false,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            category_count: 0,
            expanded_count: 0,
            collapsed_categories: HashSet::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let attachment = MapiObject::AttachmentTable {
            folder_id: INBOX_FOLDER_ID,
            message_id: 1,
            columns: Vec::new(),
            columns_set: false,
            sort_orders: Vec::new(),
            restriction: None,
            bookmarks: HashMap::new(),
            next_bookmark: 1,
            position: 0,
        };
        let permission = MapiObject::PermissionTable {
            folder_id: INBOX_FOLDER_ID,
            columns: Vec::new(),
            columns_set: false,
            position: 0,
        };
        let rule = MapiObject::RuleTable {
            folder_id: INBOX_FOLDER_ID,
            columns: Vec::new(),
            columns_set: false,
            position: 0,
        };

        assert!(restrict_supported_on_object(&contents));
        assert!(restrict_supported_on_object(&rule));
        assert!(!restrict_supported_on_object(&attachment));
        assert!(!restrict_supported_on_object(&permission));
    }

    #[test]
    fn sort_table_request_validation_matches_microsoft_bounds() {
        fn request_with_orders(
            flags: u8,
            category_count: u16,
            expanded_count: u16,
            orders: &[(u32, u8)],
        ) -> RopRequest {
            let mut payload = vec![flags];
            let sort_order_count = orders.len() as u16;
            payload.extend_from_slice(&sort_order_count.to_le_bytes());
            payload.extend_from_slice(&category_count.to_le_bytes());
            payload.extend_from_slice(&expanded_count.to_le_bytes());
            for (property_tag, order) in orders {
                payload.extend_from_slice(&property_tag.to_le_bytes());
                payload.push(*order);
            }
            RopRequest {
                rop_id: RopId::SortTable.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: None,
                payload,
            }
        }
        fn request(
            flags: u8,
            sort_order_count: u16,
            category_count: u16,
            expanded_count: u16,
        ) -> RopRequest {
            request_with_orders(
                flags,
                category_count,
                expanded_count,
                &vec![(PID_TAG_SUBJECT_W, 0); usize::from(sort_order_count)],
            )
        }

        assert!(sort_table_request_is_valid(&request(0x00, 1, 0, 0)));
        assert!(sort_table_request_is_valid(&request(0x01, 1, 0, 0)));
        assert!(!sort_table_request_is_valid(&request(0x02, 1, 0, 0)));
        assert!(!sort_table_request_is_valid(&request(0x00, 1, 2, 0)));
        assert!(!sort_table_request_is_valid(&request(0x00, 1, 1, 2)));
        assert!(!sort_table_request_is_valid(&request_with_orders(
            0x00,
            0,
            0,
            &[
                (PID_TAG_SUBJECT_W, 0x01),
                (PID_TAG_MESSAGE_DELIVERY_TIME, 0x04)
            ],
        )));
        assert!(sort_table_request_is_valid(&request_with_orders(
            0x00,
            1,
            0,
            &[
                (PID_TAG_SUBJECT_W, 0x01),
                (PID_TAG_MESSAGE_DELIVERY_TIME, 0x04),
                (PID_TAG_MESSAGE_SIZE, 0x00)
            ],
        )));
        assert!(!sort_table_request_is_valid(&request_with_orders(
            0x00,
            1,
            0,
            &[
                (PID_TAG_SUBJECT_W, 0x04),
                (PID_TAG_MESSAGE_DELIVERY_TIME, 0x00)
            ],
        )));
        assert!(!sort_table_request_is_valid(&request_with_orders(
            0x00,
            1,
            0,
            &[
                (PID_TAG_SUBJECT_W, 0x00),
                (PID_TAG_MESSAGE_DELIVERY_TIME, 0x04),
                (PID_TAG_MESSAGE_SIZE, 0x04)
            ],
        )));
        assert!(!sort_table_request_is_valid(&request_with_orders(
            0x00,
            0,
            0,
            &[(PID_TAG_SUBJECT_W, 0x02)],
        )));
        assert!(!sort_table_request_is_valid(&request_with_orders(
            0x00,
            0,
            0,
            &[(0x8001_1003, 0x00)],
        )));
        assert!(!sort_table_request_is_valid(&request_with_orders(
            0x00,
            0,
            0,
            &[(0x8001_2003, 0x00)],
        )));
        assert!(sort_table_request_is_valid(&request_with_orders(
            0x00,
            1,
            0,
            &[(0x8001_3003, 0x00)],
        )));
        assert!(sort_table_request_is_valid(&request_with_orders(
            0x00,
            0,
            0,
            &[
                (PID_TAG_MESSAGE_CLASS_W, 0x00),
                (0x683A_0003, 0x00),
                (0x7006_001F, 0x00)
            ],
        )));
        assert!(!sort_table_request_is_valid(&request_with_orders(
            0x00,
            2,
            0,
            &[(0x8001_3003, 0x00), (0x8002_301F, 0x00)],
        )));

        let mut truncated = request(0x00, 1, 0, 0);
        truncated.payload.pop();
        assert!(!sort_table_request_is_valid(&truncated));
    }

    #[test]
    fn get_attachment_table_flags_match_microsoft_message_values() {
        fn request(table_flags: u8) -> RopRequest {
            RopRequest {
                rop_id: RopId::GetAttachmentTable.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: Some(1),
                payload: vec![table_flags],
            }
        }

        assert!(get_attachment_table_flags_are_valid(&request(0x00)));
        assert!(get_attachment_table_flags_are_valid(&request(0x40)));
        assert!(!get_attachment_table_flags_are_valid(&request(0x01)));
        assert!(!get_attachment_table_flags_are_valid(&request(0x02)));
        assert!(!get_attachment_table_flags_are_valid(&request(0x41)));
    }

    #[test]
    fn hierarchy_table_flags_match_microsoft_folder_values() {
        fn request(flags: u8) -> RopRequest {
            RopRequest {
                rop_id: RopId::GetHierarchyTable.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: Some(1),
                payload: vec![flags],
            }
        }

        assert!(hierarchy_table_flags_are_valid(&request(0x00)));
        assert!(hierarchy_table_flags_are_valid(&request(0x04)));
        assert!(hierarchy_table_flags_are_valid(&request(0x08)));
        assert!(hierarchy_table_flags_are_valid(&request(0x10)));
        assert!(hierarchy_table_flags_are_valid(&request(0x20)));
        assert!(hierarchy_table_flags_are_valid(&request(0x40)));
        assert!(hierarchy_table_flags_are_valid(&request(0x80)));
        assert!(!hierarchy_table_flags_are_valid(&request(0x01)));
        assert!(!hierarchy_table_flags_are_valid(&request(0x02)));
        assert!(!hierarchy_table_flags_are_valid(&request(0x03)));
    }

    #[test]
    fn contents_table_flags_match_microsoft_folder_values() {
        assert_eq!(
            contents_table_flags_error(0x00, INBOX_FOLDER_ID, false),
            None
        );
        assert_eq!(
            contents_table_flags_error(0x02, INBOX_FOLDER_ID, false),
            None
        );
        assert_eq!(
            contents_table_flags_error(0x08, INBOX_FOLDER_ID, false),
            None
        );
        assert_eq!(
            contents_table_flags_error(0x10, INBOX_FOLDER_ID, false),
            None
        );
        assert_eq!(
            contents_table_flags_error(0x40, INBOX_FOLDER_ID, false),
            None
        );
        assert_eq!(
            contents_table_flags_error(0x20, INBOX_FOLDER_ID, false),
            Some(0x8004_0102)
        );
        assert_eq!(
            contents_table_flags_error(0x01, INBOX_FOLDER_ID, false),
            Some(0x8007_0057)
        );
        assert_eq!(
            contents_table_flags_error(0x04, INBOX_FOLDER_ID, false),
            Some(0x8007_0057)
        );
        assert_eq!(
            contents_table_flags_error(0x80, ROOT_FOLDER_ID, false),
            Some(0x8007_0057)
        );
        assert_eq!(
            contents_table_flags_error(0xC8, INBOX_FOLDER_ID, false),
            Some(0x8007_0057)
        );
        assert_eq!(
            contents_table_flags_error(0xC8, ROOT_FOLDER_ID, false),
            None
        );
        assert_eq!(
            contents_table_flags_error(0xC8, INBOX_FOLDER_ID, true),
            None
        );
    }

    #[test]
    fn open_attachment_flags_match_microsoft_message_values() {
        fn request(flags: u8) -> RopRequest {
            RopRequest {
                rop_id: RopId::OpenAttachment.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: Some(1),
                payload: vec![flags, 0, 0, 0, 0],
            }
        }

        assert!(open_attachment_flags_are_valid(&request(0x00)));
        assert!(open_attachment_flags_are_valid(&request(0x01)));
        assert!(open_attachment_flags_are_valid(&request(0x03)));
        assert!(!open_attachment_flags_are_valid(&request(0x02)));
        assert!(!open_attachment_flags_are_valid(&request(0x04)));
        assert!(!open_attachment_flags_are_valid(&request(0x40)));
    }

    #[test]
    fn save_flags_match_microsoft_message_and_attachment_combinations() {
        fn request(flags: u8) -> RopRequest {
            RopRequest {
                rop_id: RopId::SaveChangesAttachment.as_u8(),
                input_handle_index: Some(0),
                output_handle_index: Some(1),
                payload: vec![flags],
            }
        }

        assert!(save_flags_are_supported(&request(0x00)));
        assert!(save_flags_are_supported(&request(0x01)));
        assert!(save_flags_are_supported(&request(0x02)));
        assert!(save_flags_are_supported(&request(0x04)));
        assert!(save_flags_are_supported(&request(0x0A)));
        assert!(!save_flags_are_supported(&request(0x03)));
        assert!(!save_flags_are_supported(&request(0x07)));
        assert!(!save_flags_are_supported(&request(0x0B)));
    }
}
