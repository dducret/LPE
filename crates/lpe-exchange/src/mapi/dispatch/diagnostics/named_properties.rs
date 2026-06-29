use super::super::*;

pub(in crate::mapi::dispatch) fn format_debug_property_ids(property_ids: &[u16]) -> String {
    property_ids
        .iter()
        .map(|property_id| format!("{property_id:#06x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi::dispatch) fn format_debug_named_properties(
    properties: &[MapiNamedProperty],
) -> String {
    properties
        .iter()
        .map(|property| {
            let kind = match &property.kind {
                MapiNamedPropertyKind::Lid(lid) => format!("lid={lid:#010x}"),
                MapiNamedPropertyKind::Name(name) => format!("name={name}"),
            };
            format!("guid={};{kind}", hex_preview(&property.guid, 16))
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(in crate::mapi::dispatch) fn format_debug_named_property_context(
    session: &MapiSession,
    tags: &[u32],
) -> String {
    tags.iter()
        .copied()
        .filter(|tag| MapiPropertyTag::new(*tag).property_id() >= FIRST_NAMED_PROPERTY_ID)
        .map(|tag| {
            let property_tag = MapiPropertyTag::new(tag);
            let property_id = property_tag.property_id();
            let property_type = property_tag.property_type_code();
            let (property, source) = if let Some(property) =
                session.named_property_ids.get(&property_id).cloned()
            {
                (property, "session")
            } else if let Some(property) = well_known_named_property_for_id(property_id) {
                (property, "well_known")
            } else {
                (session.property_name_for_id(property_id), "unresolved_fallback")
            };
            let kind = match &property.kind {
                MapiNamedPropertyKind::Lid(lid) => format!("lid={lid:#010x}"),
                MapiNamedPropertyKind::Name(name) => format!("name={name}"),
            };
            format!(
                "{tag:#010x}:id={property_id:#06x}:type={property_type:#06x}:source={source}:guid={}:{}",
                hex_preview(&property.guid, 16),
                kind
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(in crate::mapi::dispatch) fn format_contents_table_named_property_context(
    session: &MapiSession,
    object: Option<&MapiObject>,
) -> String {
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        ..
    }) = object
    else {
        return String::new();
    };
    let selected_columns = effective_contents_table_columns(*folder_id, *associated, columns);
    format_debug_named_property_context(session, &selected_columns)
}
