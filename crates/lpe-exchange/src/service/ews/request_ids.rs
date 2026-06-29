use super::super::*;

pub(in crate::service) fn requested_item_ids(request: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut rest = request;
    while let Some(index) = rest.find("<t:ItemId").or_else(|| rest.find("<ItemId")) {
        rest = &rest[index..];
        if let Some(id) = attribute_value_after(rest, "ItemId", "Id") {
            ids.push(id.to_string());
        }
        rest = &rest[1..];
    }
    ids
}

pub(in crate::service) fn requested_attachment_ids(request: &str) -> Vec<String> {
    attribute_values_for_tag(request, "AttachmentId", "Id")
        .into_iter()
        .map(str::to_string)
        .collect()
}

pub(in crate::service) fn requested_transfer_item_ids(request: &str) -> Vec<String> {
    let mut ids = requested_item_ids(request);
    ids.extend(
        element_contents(request, "Item")
            .into_iter()
            .filter_map(|item| {
                element_text(item, "ItemId")
                    .or_else(|| element_text(item, "SourceItemId"))
                    .or_else(|| element_text(item, "Subject"))
            })
            .filter(|value| !value.trim().is_empty()),
    );
    if ids.is_empty() && request.contains("<t:Item") {
        ids.push(format!("ews-upload:{}", Uuid::new_v4()));
    }
    ids
}

pub(in crate::service) fn requested_folder_ids(request: &str) -> Vec<String> {
    attribute_values_for_tag(request, "FolderId", "Id")
        .into_iter()
        .map(str::to_string)
        .collect()
}
