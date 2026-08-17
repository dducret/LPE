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
