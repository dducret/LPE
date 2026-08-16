use super::super::*;

pub(in crate::service) fn requested_item_ids(request: &str) -> Vec<String> {
    requested_item_references(request)
        .into_iter()
        .map(|reference| reference.id)
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::service) struct RequestedItemReference {
    pub id: String,
    pub change_key: Option<String>,
}

pub(in crate::service) fn requested_item_references(request: &str) -> Vec<RequestedItemReference> {
    let mut ids = Vec::new();
    let mut rest = request;
    while let Some(index) = rest.find("<t:ItemId").or_else(|| rest.find("<ItemId")) {
        rest = &rest[index..];
        if let Some(id) = attribute_value_after(rest, "ItemId", "Id") {
            ids.push(RequestedItemReference {
                id: id.to_string(),
                change_key: attribute_value_after(rest, "ItemId", "ChangeKey").map(str::to_string),
            });
        }
        rest = &rest[1..];
    }
    ids
}

pub(in crate::service) fn requested_transfer_item_ids(request: &str) -> Vec<String> {
    let mut ids = requested_item_ids(request);
    ids.extend(
        element_contents(request, "Item")
            .into_iter()
            .filter_map(|item| {
                element_text(item, "ItemId").or_else(|| element_text(item, "SourceItemId"))
            })
            .filter(|value| !value.trim().is_empty()),
    );
    ids
}

pub(in crate::service) fn requested_folder_ids(request: &str) -> Vec<String> {
    attribute_values_for_tag(request, "FolderId", "Id")
        .into_iter()
        .map(str::to_string)
        .collect()
}

pub(in crate::service) fn request_contains_folder_reference(request: &str) -> bool {
    request.contains("FolderId") || request.contains("DistinguishedFolderId")
}

pub(in crate::service) fn requested_collection_id(request: &str) -> Option<&str> {
    requested_collection_id_in(request, "")
}

pub(in crate::service) fn requested_collection_id_in<'a>(
    request: &'a str,
    wrapper: &str,
) -> Option<&'a str> {
    let xml = if wrapper.is_empty() {
        request
    } else {
        element_content(request, wrapper)?
    };
    attribute_values_for_tag(xml, "FolderId", "Id")
        .into_iter()
        .next()
        .or_else(|| {
            attribute_values_for_tag(xml, "DistinguishedFolderId", "Id")
                .into_iter()
                .next()
        })
        .map(|value| match value {
            "contacts" | "calendar" | "tasks" => DEFAULT_COLLECTION_ID,
            other => other,
        })
}

pub(in crate::service) fn requested_folder_path_segments(request: &str) -> Vec<String> {
    element_content(request, "RelativeFolderPath")
        .map(|path| {
            element_contents(path, "DisplayName")
                .into_iter()
                .map(xml_text)
                .filter(|value| !value.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

pub(in crate::service) fn requested_public_folder_ids(request: &str) -> Vec<Uuid> {
    attribute_values_for_tag(request, "FolderId", "Id")
        .into_iter()
        .filter_map(|value| value.strip_prefix("public-folder:"))
        .filter_map(|value| Uuid::parse_str(value).ok())
        .collect()
}

pub(in crate::service) fn requested_public_folder_ids_in(
    request: &str,
    wrapper: &str,
) -> Vec<Uuid> {
    element_content(request, wrapper)
        .map(requested_public_folder_ids)
        .unwrap_or_default()
}

pub(in crate::service) fn requested_mailbox_folder_ids(request: &str) -> Vec<Uuid> {
    requested_folder_ids(request)
        .into_iter()
        .filter_map(|id| {
            id.strip_prefix("mailbox:")
                .or(Some(id.as_str()))
                .and_then(|value| Uuid::parse_str(value).ok())
        })
        .collect()
}

pub(in crate::service) fn requested_mailbox_folder_ids_in(
    request: &str,
    wrapper: &str,
) -> Vec<Uuid> {
    element_content(request, wrapper)
        .map(requested_mailbox_folder_ids)
        .unwrap_or_default()
}

pub(in crate::service) fn requested_mailbox_role(request: &str) -> Option<&'static str> {
    requested_distinguished_folder_id(request).and_then(ews_distinguished_mailbox_role)
}

pub(in crate::service) fn requested_mailbox_role_in(
    request: &str,
    wrapper: &str,
) -> Option<&'static str> {
    element_content(request, wrapper).and_then(requested_mailbox_role)
}

pub(in crate::service) fn requested_distinguished_folder_id(request: &str) -> Option<&str> {
    attribute_values_for_tag(request, "DistinguishedFolderId", "Id")
        .into_iter()
        .next()
        .or_else(|| {
            attribute_values_for_tag(request, "FolderId", "Id")
                .into_iter()
                .next()
        })
}

pub(in crate::service) fn ews_distinguished_mailbox_role(value: &str) -> Option<&'static str> {
    EwsDistinguishedFolderIdName::parse(value).and_then(EwsDistinguishedFolderIdName::mailbox_role)
}

#[cfg(test)]
mod tests {
    use super::requested_item_references;

    #[test]
    fn item_references_keep_supplied_change_keys_with_their_item_ids() {
        let references = requested_item_references(
            r#"<t:ItemId Id="contact:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" ChangeKey="current"/><t:ItemId Id="event:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" ChangeKey="stale"/>"#,
        );

        assert_eq!(references.len(), 2);
        assert_eq!(references[0].change_key.as_deref(), Some("current"));
        assert_eq!(references[1].change_key.as_deref(), Some("stale"));
    }
}
