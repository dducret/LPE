use super::super::*;

pub(in crate::service) fn requested_item_ids(request: &str) -> Vec<String> {
    requested_item_references(request)
        .into_iter()
        .map(|reference| reference.id)
        .collect()
}

/// [MS-OXWSMSG] sections 3.1.4.3 and 3.1.4.6: mutable item operations use
/// their own direct, singular ItemIds collection so unrelated XML cannot add
/// targets to a write request.
pub(in crate::service) fn requested_operation_item_references(
    request: &str,
    operation: &str,
) -> Result<Vec<RequestedItemReference>> {
    let operations = element_contents(request, operation);
    let [operation_content] = operations.as_slice() else {
        bail!("{operation} requires exactly one operation element");
    };
    let item_ids = element_contents(operation_content, "ItemIds");
    let direct_item_ids = direct_child_contents(operation_content, "ItemIds");
    let [item_ids_content] = direct_item_ids.as_slice() else {
        bail!("{operation} requires exactly one direct ItemIds collection");
    };
    if item_ids.len() != 1 {
        bail!("{operation} ItemIds collection is duplicated or misplaced");
    }
    let references = requested_item_references(item_ids_content);
    if references.is_empty()
        || requested_item_references(operation_content).len() != references.len()
    {
        bail!("{operation} ItemIds requires one or more direct ItemId values");
    }
    Ok(references)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::service) struct RequestedItemReference {
    pub id: String,
    pub change_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::service) enum CreateItemSavedFolderTarget {
    Mailbox(Uuid),
    MailboxRole(&'static str),
    PublicFolder(Uuid),
    Collection(String),
    BareUuid(String),
}

/// [MS-OXWSMSG] section 3.1.4.2: a SavedItemFolderId is optional, but when
/// present this bounded adapter must classify its one target before creation.
/// In particular, malformed canonical mailbox and public-folder identifiers
/// must not be treated as an omitted target.
pub(in crate::service) fn requested_create_item_saved_folder_target(
    request: &str,
) -> Result<Option<CreateItemSavedFolderTarget>> {
    let folders = element_contents(request, "SavedItemFolderId");
    if folders.is_empty() {
        return Ok(None);
    }
    if folders.len() != 1 {
        bail!("CreateItem supports at most one SavedItemFolderId");
    }

    let folder_ids = attribute_values_for_tag(folders[0], "FolderId", "Id");
    let distinguished_ids = attribute_values_for_tag(folders[0], "DistinguishedFolderId", "Id");
    if folder_ids.len() + distinguished_ids.len() != 1 {
        bail!("CreateItem SavedItemFolderId requires exactly one folder target");
    }

    if let Some(value) = folder_ids.first() {
        if let Some(value) = value.strip_prefix("mailbox:") {
            return Uuid::parse_str(value)
                .map(CreateItemSavedFolderTarget::Mailbox)
                .map(Some)
                .map_err(|_| anyhow!("CreateItem SavedItemFolderId mailbox id is invalid"));
        }
        if let Some(value) = value.strip_prefix("public-folder:") {
            return Uuid::parse_str(value)
                .map(CreateItemSavedFolderTarget::PublicFolder)
                .map(Some)
                .map_err(|_| anyhow!("CreateItem SavedItemFolderId public-folder id is invalid"));
        }
        if Uuid::parse_str(value).is_ok() {
            return Ok(Some(CreateItemSavedFolderTarget::BareUuid(
                (*value).to_string(),
            )));
        }
        if value.trim().is_empty() || value.contains(':') {
            bail!("CreateItem SavedItemFolderId is not a supported collection id");
        }
        return Ok(Some(CreateItemSavedFolderTarget::Collection(
            (*value).to_string(),
        )));
    }

    let value = distinguished_ids[0];
    if matches!(value, "contacts" | "calendar" | "tasks") {
        return Ok(Some(CreateItemSavedFolderTarget::Collection(
            DEFAULT_COLLECTION_ID.to_string(),
        )));
    }
    ews_distinguished_mailbox_role(value)
        .map(CreateItemSavedFolderTarget::MailboxRole)
        .map(Some)
        .ok_or_else(|| {
            anyhow!("CreateItem SavedItemFolderId distinguished folder is not supported")
        })
}

pub(in crate::service) fn create_item_collection_id(
    target: Option<&CreateItemSavedFolderTarget>,
) -> Result<Option<&str>> {
    match target {
        None => Ok(None),
        Some(
            CreateItemSavedFolderTarget::Collection(id) | CreateItemSavedFolderTarget::BareUuid(id),
        ) => Ok(Some(id)),
        Some(_) => bail!("CreateItem item type requires a canonical collection target"),
    }
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

pub(in crate::service) fn direct_child_contents<'a>(
    xml: &'a str,
    local_name: &str,
) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut cursor = 0;
    let mut depth: usize = 0;
    while let Some(relative_start) = xml[cursor..].find('<') {
        let start = cursor + relative_start;
        let Some(relative_end) = xml[start..].find('>') else {
            break;
        };
        let end = start + relative_end;
        let tag = xml[start + 1..end].trim_start();
        if tag.starts_with('?') || tag.starts_with('!') {
            cursor = end + 1;
            continue;
        }
        let closing = tag.starts_with('/');
        let name = tag
            .trim_start_matches('/')
            .split(|character: char| character.is_whitespace() || character == '/')
            .next()
            .unwrap_or_default()
            .rsplit(':')
            .next()
            .unwrap_or_default();
        let self_closing = tag.trim_end().ends_with('/');
        if closing {
            depth = depth.saturating_sub(1);
        } else {
            if depth == 0 && name == local_name {
                values.push(if self_closing {
                    ""
                } else {
                    matching_element_content(xml, end + 1, name).unwrap_or("")
                });
            }
            if !self_closing {
                depth += 1;
            }
        }
        cursor = end + 1;
    }
    values
}

fn matching_element_content<'a>(
    xml: &'a str,
    content_start: usize,
    local_name: &str,
) -> Option<&'a str> {
    let mut cursor = content_start;
    let mut depth = 1;
    while let Some(relative_start) = xml[cursor..].find('<') {
        let start = cursor + relative_start;
        let relative_end = xml[start..].find('>')?;
        let end = start + relative_end;
        let tag = xml[start + 1..end].trim_start();
        let closing = tag.starts_with('/');
        let name = tag
            .trim_start_matches('/')
            .split(|character: char| character.is_whitespace() || character == '/')
            .next()?
            .rsplit(':')
            .next()?;
        if name == local_name {
            if closing {
                depth -= 1;
                if depth == 0 {
                    return Some(&xml[content_start..start]);
                }
            } else if !tag.trim_end().ends_with('/') {
                depth += 1;
            }
        }
        cursor = end + 1;
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::service) struct RequestedTransferUpload {
    pub parent_folder_id: String,
    pub data: String,
}

pub(in crate::service) fn requested_transfer_uploads(
    request: &str,
) -> Result<Vec<RequestedTransferUpload>> {
    let items = element_contents(request, "Items");
    let [items] = items.as_slice() else {
        bail!("UploadItems requires exactly one Items collection");
    };
    let uploads = element_contents(items, "Item")
        .into_iter()
        .map(|item| {
            let parent = element_contents(item, "ParentFolderId");
            let [parent] = parent.as_slice() else {
                bail!("UploadItems requires one ParentFolderId per item");
            };
            let parent_folder_id = attribute_value_after(parent, "FolderId", "Id")
                .ok_or_else(|| anyhow!("UploadItems supports only canonical ParentFolderId"))?;
            let data = element_contents(item, "Data");
            let [data] = data.as_slice() else {
                bail!("UploadItems requires one Data payload per item");
            };
            if data.trim().is_empty() {
                bail!("UploadItems Data payload must not be empty");
            }
            Ok(RequestedTransferUpload {
                parent_folder_id: parent_folder_id.to_string(),
                data: data.trim().to_string(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if uploads.is_empty() {
        bail!("UploadItems requires one or more items");
    }
    Ok(uploads)
}

pub(in crate::service) fn requested_folder_ids(request: &str) -> Vec<String> {
    attribute_values_for_tag(request, "FolderId", "Id")
        .into_iter()
        .map(str::to_string)
        .collect()
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

pub(in crate::service) fn requested_folder_path_segments(request: &str) -> Result<Vec<String>> {
    let path = element_content(request, "RelativeFolderPath")
        .ok_or_else(|| anyhow!("CreateFolderPath requires one RelativeFolderPath"))?;
    let segments = element_contents(path, "DisplayName")
        .into_iter()
        .map(xml_text)
        .collect::<Vec<_>>();
    if segments.is_empty() || segments.iter().any(|segment| segment.is_empty()) {
        bail!("CreateFolderPath requires nonempty folder DisplayName segments");
    }
    Ok(segments)
}

pub(in crate::service) fn requested_public_folder_ids(request: &str) -> Vec<Uuid> {
    attribute_values_for_tag(request, "FolderId", "Id")
        .into_iter()
        .filter_map(|value| value.strip_prefix("public-folder:"))
        .filter_map(|value| Uuid::parse_str(value).ok())
        .collect()
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
    use super::{requested_create_item_saved_folder_target, requested_item_references};

    #[test]
    fn item_references_keep_supplied_change_keys_with_their_item_ids() {
        let references = requested_item_references(
            r#"<t:ItemId Id="contact:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" ChangeKey="current"/><t:ItemId Id="event:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" ChangeKey="stale"/>"#,
        );

        assert_eq!(references.len(), 2);
        assert_eq!(references[0].change_key.as_deref(), Some("current"));
        assert_eq!(references[1].change_key.as_deref(), Some("stale"));
    }

    #[test]
    fn create_item_saved_folder_rejects_malformed_canonical_ids() {
        for id in ["mailbox:not-a-uuid", "public-folder:not-a-uuid"] {
            let error = requested_create_item_saved_folder_target(&format!(
                "<m:SavedItemFolderId><t:FolderId Id=\"{id}\"/></m:SavedItemFolderId>"
            ))
            .unwrap_err();

            assert!(error.to_string().contains("SavedItemFolderId"));
        }
    }
}
