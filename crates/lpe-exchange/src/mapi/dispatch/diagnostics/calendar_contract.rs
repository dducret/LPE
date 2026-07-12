use super::super::*;
use lpe_domain::crypto::sha256_hex_prefix;
use std::collections::BTreeMap;

const CALENDAR_CONTRACT_PROTOCOL_REFERENCES: &str =
    "MS-OXCFOLD 2.2.2.2;MS-OXOCFG 2.2.6.1,2.2.6.1.1,2.2.6.2;MS-OXCROPS 2.2.5.1,2.2.5.7;MS-OXCTABL 2.2.2.2,2.2.2.8";
const CALENDAR_NAMED_REGISTRY_SAMPLE_LIMIT: usize = 64;
const NAMED_PROPERTY_COLLISION_SAMPLE_LIMIT: usize = 16;

pub(in crate::mapi::dispatch) fn format_calendar_view_contract_fingerprint(
    session: &MapiSession,
    account_id: Uuid,
    stage: &str,
    object: Option<&MapiObject>,
    query_position_response: Option<(u32, u32)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<String> {
    let (folder_id, associated, columns, position, sort_orders, restriction) = match object {
        Some(MapiObject::Folder { folder_id, .. }) if *folder_id == CALENDAR_FOLDER_ID => {
            (*folder_id, false, &[][..], 0, &[][..], None)
        }
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            sort_orders,
            restriction,
            ..
        }) if *folder_id == CALENDAR_FOLDER_ID && !*associated => (
            *folder_id,
            *associated,
            columns.as_slice(),
            *position,
            sort_orders.as_slice(),
            restriction.as_ref(),
        ),
        _ => return None,
    };

    let view = debug_advertised_default_named_view(snapshot, folder_id);
    let (view_folder_id, view_message_id, view_name, descriptor, descriptor_strings) = view
        .as_ref()
        .map(|message| {
            let definition = outlook_folder_view_definition(message.folder_id, &message.name);
            (
                message.folder_id,
                message.id,
                escape_contract_text(&message.name),
                view_descriptor_binary(&definition),
                view_descriptor_strings_binary(&definition),
            )
        })
        .unwrap_or((0, 0, "missing".to_string(), Vec::new(), Vec::new()));
    let entry_id = view.as_ref().and_then(|message| {
        crate::mapi::identity::message_entry_id_from_object_ids(
            account_id,
            message.folder_id,
            message.id,
        )
    });
    let entry_id_target = entry_id
        .as_deref()
        .and_then(default_view_message_entry_id_target);
    let descriptor_columns = view_descriptor_debug_property_tags(&descriptor);
    let visible_descriptor_columns = view_descriptor_property_tags(&descriptor);
    let fai_inventory = format_calendar_fai_inventory(snapshot, account_id);
    let named_registry = format_named_property_registry(session);
    let named_id_reuse = format_named_property_id_reuse(session);
    let descriptor_named_references =
        format_debug_named_property_context(session, &descriptor_columns);
    let selected_named_references = format_debug_named_property_context(session, columns);
    let table_contract =
        format_outlook_view_handoff_table_contract(folder_id, associated, columns, snapshot);
    let selected_row_projection = format_calendar_event_query_position_summary(
        folder_id,
        associated,
        position,
        1,
        sort_orders,
        restriction,
        columns,
        snapshot,
    );
    let descriptor_row_projection = format_calendar_event_query_position_summary(
        folder_id,
        associated,
        position,
        1,
        sort_orders,
        restriction,
        &visible_descriptor_columns,
        snapshot,
    );
    let implicit_sort = format_debug_sort_orders(sort_orders);
    let restriction_summary = format_debug_restriction_option(restriction);
    let invariant_issues = format_calendar_contract_invariant_issues(
        view.as_ref(),
        &descriptor,
        &descriptor_strings,
        entry_id_target,
        &named_id_reuse,
        &fai_inventory,
    );
    let (response_numerator, response_denominator) = query_position_response
        .map(|(numerator, denominator)| (numerator.to_string(), denominator.to_string()))
        .unwrap_or_else(|| ("not_observed".to_string(), "not_observed".to_string()));
    let entry_id_summary = entry_id
        .as_deref()
        .map(|bytes| {
            let target = entry_id_target
                .map(|(target_folder, target_message)| {
                    format!("folder=0x{target_folder:016x},message=0x{target_message:016x}")
                })
                .unwrap_or_else(|| "decode=invalid".to_string());
            format!(
                "bytes={},sha256_16={},preview={},{}",
                bytes.len(),
                sha256_hex_prefix(bytes, 16),
                hex_preview(bytes, 48),
                target
            )
        })
        .unwrap_or_else(|| "missing".to_string());
    let core = format!(
        "stage={};protocol_refs={};folder=0x{folder_id:016x};view_folder=0x{view_folder_id:016x};view_mid=0x{view_message_id:016x};view_name={};entry_id={};fai_inventory={};descriptor_bytes={};descriptor_sha256_16={};descriptor_preview={};descriptor_strings_bytes={};descriptor_strings_sha256_16={};descriptor_strings_preview={};descriptor_summary={};descriptor_property_types={};descriptor_named_references={};selected_columns={};selected_named_references={};implicit_sort={};restriction={};table_contract={};selected_row_projection={};descriptor_row_projection={};query_position_numerator={};query_position_denominator={};named_registry={};named_id_reuse={};invariant_issues={}",
        escape_contract_text(stage),
        CALENDAR_CONTRACT_PROTOCOL_REFERENCES,
        view_name,
        entry_id_summary,
        fai_inventory,
        descriptor.len(),
        sha256_hex_prefix(&descriptor, 16),
        hex_preview(&descriptor, 64),
        descriptor_strings.len(),
        sha256_hex_prefix(&descriptor_strings, 16),
        hex_preview(&descriptor_strings, 64),
        format_view_descriptor_binary_summary(&descriptor),
        format_property_tags_with_types(&descriptor_columns),
        descriptor_named_references,
        format_debug_property_tags(columns),
        selected_named_references,
        debug_context_or_none(&implicit_sort),
        debug_context_or_none(&restriction_summary),
        table_contract,
        selected_row_projection,
        descriptor_row_projection,
        response_numerator,
        response_denominator,
        named_registry,
        named_id_reuse,
        invariant_issues,
    );
    Some(format!(
        "version=1;sha256_32={};{core}",
        sha256_hex_prefix(core.as_bytes(), 32)
    ))
}

pub(in crate::mapi::dispatch) fn log_calendar_view_contract_fingerprint(
    principal: &AccountPrincipal,
    session: &MapiSession,
    request_id: &str,
    request_rop_id: &str,
    stage: &str,
    object: Option<&MapiObject>,
    query_position_response: Option<(u32, u32)>,
    snapshot: &MapiMailStoreSnapshot,
) {
    let Some(fingerprint) = format_calendar_view_contract_fingerprint(
        session,
        principal.account_id,
        stage,
        object,
        query_position_response,
        snapshot,
    ) else {
        return;
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = %request_id,
        request_rop_id,
        calendar_contract_stage = stage,
        calendar_contract_fingerprint = %fingerprint,
        "rca debug mapi calendar contract fingerprint"
    );
}

fn format_calendar_fai_inventory(snapshot: &MapiMailStoreSnapshot, account_id: Uuid) -> String {
    let mut rows = debug_associated_table_rows(CALENDAR_FOLDER_ID, snapshot, None, account_id);
    rows.sort_by_key(debug_associated_row_id);
    let entries = rows
        .iter()
        .map(|row| match row {
            DebugAssociatedTableRow::Config(message) => {
                let properties = serde_json::to_vec(&message.properties_json).unwrap_or_default();
                let mut keys = message
                    .properties_json
                    .as_object()
                    .map(|object| object.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                keys.sort();
                format!(
                    "id=0x{:016x},folder=0x{:016x},class={},subject={},kind=config,properties_bytes={},properties_sha256_16={},property_keys={}",
                    message.id,
                    message.folder_id,
                    escape_contract_text(&message.message_class),
                    escape_contract_text(&message.subject),
                    properties.len(),
                    sha256_hex_prefix(&properties, 16),
                    keys.join(",")
                )
            }
            DebugAssociatedTableRow::NamedView(message) => format!(
                "id=0x{:016x},folder=0x{:016x},class=IPM.Microsoft.FolderDesign.NamedView,subject={},kind=named_view,view_flags=0x{:08x},view_type=0x{:08x}",
                message.id,
                message.folder_id,
                escape_contract_text(&message.name),
                message.view_flags,
                message.view_type
            ),
        })
        .collect::<Vec<_>>()
        .join("|");
    format!("count={};rows=[{}]", rows.len(), entries)
}

fn format_named_property_registry(session: &MapiSession) -> String {
    let mut entries = session.named_property_ids.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(property_id, _)| **property_id);
    let serialized = entries
        .iter()
        .map(|(property_id, property)| format_named_registry_entry(**property_id, property))
        .collect::<Vec<_>>()
        .join("|");
    let relevant = entries
        .iter()
        .filter(|(_property_id, property)| is_calendar_named_property(property))
        .map(|(property_id, property)| format_named_registry_entry(**property_id, property))
        .collect::<Vec<_>>();
    let sample = relevant
        .iter()
        .take(CALENDAR_NAMED_REGISTRY_SAMPLE_LIMIT)
        .cloned()
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "count={};sha256_16={};calendar_relevant_count={};calendar_relevant_sample_count={};calendar_relevant_omitted={};calendar_relevant_sample=[{}]",
        entries.len(),
        sha256_hex_prefix(serialized.as_bytes(), 16),
        relevant.len(),
        relevant.len().min(CALENDAR_NAMED_REGISTRY_SAMPLE_LIMIT),
        relevant.len().saturating_sub(CALENDAR_NAMED_REGISTRY_SAMPLE_LIMIT),
        sample
    )
}

fn format_named_registry_entry(property_id: u16, property: &MapiNamedProperty) -> String {
    let kind = match &property.kind {
        MapiNamedPropertyKind::Lid(lid) => format!("lid=0x{lid:08x}"),
        MapiNamedPropertyKind::Name(name) => format!("name={}", escape_contract_text(name)),
    };
    format!(
        "id=0x{property_id:04x},guid={},{}",
        hex_preview(&property.guid, 16),
        kind
    )
}

fn format_named_property_id_reuse(session: &MapiSession) -> String {
    let mut by_id = BTreeMap::<u16, Vec<String>>::new();
    for (property, property_id) in &session.named_properties {
        let kind = match &property.kind {
            MapiNamedPropertyKind::Lid(lid) => format!("lid=0x{lid:08x}"),
            MapiNamedPropertyKind::Name(name) => {
                format!("name={}", escape_contract_text(name))
            }
        };
        by_id.entry(*property_id).or_default().push(format!(
            "guid={},{}",
            hex_preview(&property.guid, 16),
            kind
        ));
    }
    let collisions = by_id
        .into_iter()
        .filter_map(|(property_id, mut properties)| {
            properties.sort();
            properties.dedup();
            (properties.len() > 1)
                .then(|| format!("id=0x{property_id:04x}:{}", properties.join(",")))
        })
        .collect::<Vec<_>>();
    if collisions.is_empty() {
        "none".to_string()
    } else {
        let sample = collisions
            .iter()
            .take(NAMED_PROPERTY_COLLISION_SAMPLE_LIMIT)
            .cloned()
            .collect::<Vec<_>>()
            .join("|");
        format!(
            "count={};omitted={};sample=[{}]",
            collisions.len(),
            collisions
                .len()
                .saturating_sub(NAMED_PROPERTY_COLLISION_SAMPLE_LIMIT),
            sample
        )
    }
}

fn format_property_tags_with_types(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| {
            let property = MapiPropertyTag::new(*tag);
            format!(
                "0x{tag:08x}:id=0x{:04x}:type=0x{:04x}",
                property.property_id(),
                property.property_type_code()
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_calendar_contract_invariant_issues(
    view: Option<&crate::mapi_store::MapiCommonViewNamedViewMessage>,
    descriptor: &[u8],
    descriptor_strings: &[u8],
    entry_id_target: Option<(u64, u64)>,
    named_id_reuse: &str,
    fai_inventory: &str,
) -> String {
    let mut issues = Vec::new();
    let Some(view) = view else {
        // Microsoft's PidTagDefaultViewEntryId canonical-property remarks
        // permit absence when the client is to use its Normal view.
        return "none".to_string();
    };
    if view.folder_id != CALENDAR_FOLDER_ID {
        issues.push("default_view_not_folder_local");
    }
    if !fai_inventory.contains(&format!("id=0x{:016x}", view.id)) {
        issues.push("default_view_missing_from_fai_inventory");
    }
    if entry_id_target != Some((view.folder_id, view.id)) {
        issues.push("default_view_entry_id_target_mismatch");
    }
    if descriptor.len() < 60 {
        issues.push("descriptor_binary_too_short");
    }
    if descriptor
        .get(8..12)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        != Some(8)
    {
        issues.push("descriptor_version_not_8");
    }
    if descriptor_strings.is_empty() {
        issues.push("descriptor_strings_missing");
    }
    if view_descriptor_sort_direction_matches_column_flags(descriptor) == Some(false) {
        issues.push("descriptor_sort_direction_conflict");
    }
    if named_id_reuse != "none" {
        issues.push("named_property_id_reused");
    }
    if issues.is_empty() {
        "none".to_string()
    } else {
        issues.join("|")
    }
}

fn escape_contract_text(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace(';', "%3b")
        .replace('|', "%7c")
        .replace('\r', "%0d")
        .replace('\n', "%0a")
}
