use crate::{protocol::BodyPreferenceType, snapshot::BodyPreference, wbxml::WbxmlNode};
pub(super) fn collection_body_preference(collection_node: &WbxmlNode) -> BodyPreference {
    collection_node
        .child("Options")
        .map(options_body_preference)
        .unwrap_or_default()
}

pub(super) fn fetch_body_preference(fetch_node: &WbxmlNode) -> BodyPreference {
    fetch_node
        .child("Options")
        .map(options_body_preference)
        .unwrap_or_default()
}

fn options_body_preference(options: &WbxmlNode) -> BodyPreference {
    options
        .children_named("BodyPreference")
        .into_iter()
        .filter_map(|preference| {
            let body_type = preference
                .child("Type")
                .and_then(|node| match node.text_value().trim().parse::<u8>() {
                    Ok(value) => Some(value),
                    Err(_) => {
                        tracing::warn!(
                            adapter = "activesync",
                            enum_name = "BodyPreferenceType",
                            raw_value = node.text_value().trim(),
                            "unsupported ActiveSync body preference type"
                        );
                        None
                    }
                })
                .and_then(BodyPreferenceType::from_u8)?;
            let truncation_size = preference
                .child("TruncationSize")
                .and_then(|node| node.text_value().trim().parse::<usize>().ok());
            Some(BodyPreference {
                body_type,
                truncation_size,
            })
        })
        .next()
        .unwrap_or_default()
}

pub(super) fn collection_deletes_as_moves(collection_node: &WbxmlNode) -> bool {
    collection_node
        .child("DeletesAsMoves")
        .map(|node| {
            let value = node.text_value().trim();
            value.is_empty() || value == "1" || value.eq_ignore_ascii_case("true")
        })
        .unwrap_or(true)
}
