use super::*;

pub(super) struct VisibleInboxProjectionAudit {
    pub(super) summary: String,
    pub(super) valid: bool,
    pub(super) row_available: bool,
    pub(super) missing_count: usize,
    pub(super) defaulted_count: usize,
    pub(super) error_count: usize,
    pub(super) wire_row_bytes: usize,
    pub(super) status_row_bytes: usize,
}

pub(super) fn record_normal_inbox_table_lifecycle(
    session: &mut MapiSession,
    phase: &str,
    request_id: &str,
    request_rop_names: &str,
    input_handle_index: u8,
    handle: Option<u32>,
    details: &str,
) {
    session.record_outlook_view_failure_trace_event(format!(
        "normal_inbox_table_lifecycle:phase={phase};request_id={request_id};request_rops={request_rop_names};input_index={input_handle_index};handle={};{details}",
        format_optional_debug_handle(handle)
    ));
}

pub(super) fn normal_inbox_table_lifecycle_details(
    handle_slots: &[u32],
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Option<(Option<u32>, String)> {
    match object {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            restriction,
            sort_orders,
            ..
        }) if *folder_id == INBOX_FOLDER_ID && !*associated => Some((
            input_handle(handle_slots, request),
            format!(
                "folder=0x{folder_id:016x};role={};associated=false;position={position};columns={};column_support={};sort={};restriction={}",
                debug_role_for_folder_id(*folder_id),
                format_debug_property_tags(columns),
                normal_message_table_column_support_summary(columns),
                format_debug_sort_orders(sort_orders),
                format_debug_restriction_option(restriction.as_ref())
            ),
        )),
        _ => None,
    }
}

pub(super) fn format_visible_inbox_first_row_projection_audit(
    position: usize,
    sort_orders: &[MapiSortOrder],
    restriction: Option<&MapiRestriction>,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> VisibleInboxProjectionAudit {
    let mut rows = emails_for_folder(INBOX_FOLDER_ID, mailboxes, emails);
    rows.retain(|email| restriction_matches_email(restriction, email));
    sort_emails(&mut rows, sort_orders);
    let selected = select_query_window(rows.len(), position, true, 1);
    let Some(index) = selected.first().copied() else {
        return VisibleInboxProjectionAudit {
            summary: format!(
                "row_available=false;valid=false;total={};position={position};columns={};missing_count=0;defaulted_count=0;error_count=0;wire_row_bytes=0;status_row_bytes=0;values=",
                rows.len(),
                format_debug_property_tags(columns)
            ),
            valid: false,
            row_available: false,
            missing_count: 0,
            defaulted_count: 0,
            error_count: 0,
            wire_row_bytes: 0,
            status_row_bytes: 0,
        };
    };
    let email = rows[index];
    let serialized = serialize_message_row(email, columns);
    let standard_row = standard_property_row_bytes(&serialized);
    let mut missing_count = 0usize;
    let mut defaulted_count = 0usize;
    let mut error_count = 0usize;
    let values = columns
        .iter()
        .map(|tag| {
            let storage_tag = canonical_property_storage_tag(*tag);
            let one_column = serialize_message_row(email, &[*tag]);
            let one_column_standard = standard_property_row_bytes(&one_column);
            match normal_message_debug_property_value(email, *tag) {
                Some(MapiValue::Error(error)) => {
                    error_count = error_count.saturating_add(1);
                    format!(
                        "tag=0x{tag:08x};storage=0x{storage_tag:08x};state=error;type=error;value=0x{error:08x};wire_row_bytes={};status_row_bytes={}",
                        one_column.len(),
                        one_column_standard.len()
                    )
                }
                Some(value) => format!(
                    "tag=0x{tag:08x};storage=0x{storage_tag:08x};state=value;type={};value={};wire_row_bytes={};status_row_bytes={}",
                    mapi_value_debug_shape(&value),
                    format_normal_message_debug_value(*tag, &value),
                    one_column.len(),
                    one_column_standard.len()
                ),
                None => {
                    missing_count = missing_count.saturating_add(1);
                    defaulted_count = defaulted_count.saturating_add(1);
                    format!(
                        "tag=0x{tag:08x};storage=0x{storage_tag:08x};state=defaulted;type=missing;value=default;wire_row_bytes={};status_row_bytes={}",
                        one_column.len(),
                        one_column_standard.len()
                    )
                }
            }
        })
        .collect::<Vec<_>>()
        .join("|");
    let valid = !serialized.is_empty() && missing_count == 0 && error_count == 0;
    VisibleInboxProjectionAudit {
        summary: format!(
            "row_available=true;valid={valid};total={};position={position};selected_index={index};mid=0x{:016x};subject={};class={};columns={};missing_count={missing_count};defaulted_count={defaulted_count};error_count={error_count};wire_row_bytes={};status_row_bytes={};values={values}",
            rows.len(),
            mapi_message_id(email),
            email.subject,
            message_class_for_email(email),
            format_debug_property_tags(columns),
            serialized.len(),
            standard_row.len()
        ),
        valid,
        row_available: true,
        missing_count,
        defaulted_count,
        error_count,
        wire_row_bytes: serialized.len(),
        status_row_bytes: standard_row.len(),
    }
}
