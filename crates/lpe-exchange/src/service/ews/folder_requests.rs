use super::super::*;
use super::folders::FolderKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::service) enum FolderOperationTarget {
    Mailbox(Uuid),
    PublicFolder(Uuid),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::service) enum CreateFolderParent {
    Mailbox(Option<Uuid>),
    PublicFolder(Uuid),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::service) struct CreateFolderRequest {
    pub(in crate::service) parent: CreateFolderParent,
    pub(in crate::service) display_name: String,
    pub(in crate::service) folder_class: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::service) enum FindFolderParent {
    Root,
    Mailbox(Uuid),
    MailboxRole(&'static str),
    PublicFolder(Uuid),
    Collection(FolderKind),
}

/// [MS-OXWSFOLD] sections 2.2.4.2, 2.2.4.16, and 3.1.4.2 require an
/// existing parent and a supported folder shape. LPE deliberately accepts one
/// canonical `IPF.Note` folder per request so validation completes before its
/// single canonical mutation.
pub(in crate::service) fn parse_create_folder_request(
    request: &str,
) -> Result<CreateFolderRequest> {
    let folders = element_content(request, "Folders")
        .ok_or_else(|| anyhow!("CreateFolder requires one Folder"))?;
    let folder_values = element_contents(folders, "Folder");
    if folder_values.len() != 1
        || [
            "ContactsFolder",
            "CalendarFolder",
            "TasksFolder",
            "SearchFolder",
        ]
        .into_iter()
        .any(|name| !element_contents(folders, name).is_empty())
    {
        bail!("CreateFolder supports exactly one IPF.Note Folder");
    }
    let folder = folder_values[0];
    let display_name = element_text(folder, "DisplayName")
        .filter(|name| !name.trim().is_empty())
        .ok_or_else(|| anyhow!("CreateFolder is missing DisplayName"))?;
    let folder_class =
        element_text(folder, "FolderClass").unwrap_or_else(|| "IPF.Note".to_string());
    if !folder_class.eq_ignore_ascii_case("IPF.Note") {
        bail!("CreateFolder supports only the IPF.Note folder class");
    }

    Ok(CreateFolderRequest {
        parent: parse_create_folder_parent(request)?,
        display_name,
        folder_class,
    })
}

/// [MS-OXWSFOLD] section 3.1.4.4 keeps protected/default folder rejection
/// ahead of LPE's single canonical delete mutation.
pub(in crate::service) fn parse_delete_folder_target(
    request: &str,
) -> Result<FolderOperationTarget> {
    if !attribute_value_after(request, "DeleteFolder", "DeleteType")
        .is_some_and(|delete_type| delete_type.eq_ignore_ascii_case("HardDelete"))
    {
        bail!("DeleteFolder supports only HardDelete");
    }
    parse_single_folder_target(
        element_content(request, "FolderIds")
            .ok_or_else(|| anyhow!("DeleteFolder requires one FolderId"))?,
        "DeleteFolder",
    )
}

fn parse_create_folder_parent(request: &str) -> Result<CreateFolderParent> {
    let parent = element_content(request, "ParentFolderId")
        .ok_or_else(|| anyhow!("CreateFolder requires one ParentFolderId"))?;
    let mut folder_ids = attribute_values_for_tag(parent, "FolderId", "Id").into_iter();
    let mut distinguished_ids =
        attribute_values_for_tag(parent, "DistinguishedFolderId", "Id").into_iter();
    let folder_id = folder_ids.next();
    let distinguished_id = distinguished_ids.next();
    if folder_ids.next().is_some()
        || distinguished_ids.next().is_some()
        || folder_id.is_some() == distinguished_id.is_some()
    {
        bail!("CreateFolder requires exactly one supported parent folder id");
    }
    if let Some(id) = folder_id {
        return match id.strip_prefix("mailbox:") {
            Some(id) => Ok(CreateFolderParent::Mailbox(Some(parse_folder_uuid(
                id, "mailbox",
            )?))),
            None => match id.strip_prefix("public-folder:") {
                Some(id) => Ok(CreateFolderParent::PublicFolder(parse_folder_uuid(
                    id,
                    "public folder",
                )?)),
                None => bail!("CreateFolder parent folder id is not supported"),
            },
        };
    }
    match distinguished_id {
        Some("msgfolderroot" | "root") => Ok(CreateFolderParent::Mailbox(None)),
        Some(_) => bail!("CreateFolder parent distinguished folder is not supported"),
        None => unreachable!("validated exactly one parent id"),
    }
}

/// [MS-OXWSSRCH] section 3.1.4.1 scopes the bounded FindFolder projection to
/// the requested parent rather than an adapter-local hierarchy.
pub(in crate::service) fn requested_find_folder_parent(
    request: &str,
) -> Result<Option<FindFolderParent>> {
    let Some(parent) = element_content(request, "ParentFolderIds") else {
        return Ok(None);
    };
    let mut folder_ids = attribute_values_for_tag(parent, "FolderId", "Id").into_iter();
    let mut distinguished_ids =
        attribute_values_for_tag(parent, "DistinguishedFolderId", "Id").into_iter();
    let folder_id = folder_ids.next();
    let distinguished_id = distinguished_ids.next();
    if folder_ids.next().is_some()
        || distinguished_ids.next().is_some()
        || folder_id.is_some() == distinguished_id.is_some()
    {
        bail!("FindFolder requires exactly one supported parent folder id");
    }
    if let Some(id) = folder_id {
        return Ok(Some(match id.strip_prefix("mailbox:") {
            Some(id) => FindFolderParent::Mailbox(parse_folder_uuid(id, "mailbox")?),
            None => match id.strip_prefix("public-folder:") {
                Some(id) => FindFolderParent::PublicFolder(parse_folder_uuid(id, "public folder")?),
                None => bail!("FindFolder parent folder id is not supported"),
            },
        }));
    }
    Ok(Some(
        match distinguished_id.expect("validated exactly one parent id") {
            "msgfolderroot" | "root" => FindFolderParent::Root,
            "contacts" => FindFolderParent::Collection(FolderKind::Contacts),
            "calendar" => FindFolderParent::Collection(FolderKind::Calendar),
            "tasks" => FindFolderParent::Collection(FolderKind::Tasks),
            id => match ews_distinguished_mailbox_role(id) {
                Some(role) => FindFolderParent::MailboxRole(role),
                None => bail!("FindFolder parent distinguished folder is not supported"),
            },
        },
    ))
}

fn parse_single_folder_target(xml: &str, operation: &str) -> Result<FolderOperationTarget> {
    let mut folder_ids = attribute_values_for_tag(xml, "FolderId", "Id").into_iter();
    let mut distinguished_ids =
        attribute_values_for_tag(xml, "DistinguishedFolderId", "Id").into_iter();
    let folder_id = folder_ids.next();
    let distinguished_id = distinguished_ids.next();
    if folder_ids.next().is_some()
        || distinguished_ids.next().is_some()
        || folder_id.is_some() == distinguished_id.is_some()
    {
        bail!("{operation} requires exactly one supported FolderId");
    }
    if let Some(id) = folder_id {
        return match id.strip_prefix("mailbox:") {
            Some(id) => Ok(FolderOperationTarget::Mailbox(parse_folder_uuid(
                id, "mailbox",
            )?)),
            None => match id.strip_prefix("public-folder:") {
                Some(id) => Ok(FolderOperationTarget::PublicFolder(parse_folder_uuid(
                    id,
                    "public folder",
                )?)),
                None => bail!("{operation} folder id is not supported"),
            },
        };
    }
    let distinguished_id = distinguished_id.expect("validated exactly one folder id");
    if let Some(role) = ews_distinguished_mailbox_role(distinguished_id) {
        return Err(anyhow!(
            "{operation} distinguished mailbox `{role}` is protected"
        ));
    }
    bail!("{operation} distinguished folder is not supported")
}

fn parse_folder_uuid(value: &str, family: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| anyhow!("{family} folder id is invalid"))
}

pub(in crate::service) fn requested_folder_kind(request: &str) -> Option<FolderKind> {
    if let Some(kind) =
        requested_sync_state(request).and_then(|state| sync_state_folder_kind(&state))
    {
        return Some(kind);
    }
    if request.contains("DistinguishedFolderId Id=\"msgfolderroot\"")
        || request.contains("DistinguishedFolderId Id='msgfolderroot'")
        || request.contains("DistinguishedFolderId Id=\"root\"")
        || request.contains("DistinguishedFolderId Id='root'")
        || request.contains("FolderId Id=\"msgfolderroot\"")
        || request.contains("FolderId Id='msgfolderroot'")
        || request.contains("FolderId Id=\"root\"")
        || request.contains("FolderId Id='root'")
    {
        return Some(FolderKind::Root);
    }
    if request.contains("DistinguishedFolderId Id=\"calendar\"")
        || request.contains("DistinguishedFolderId Id='calendar'")
        || request.contains("FolderId Id=\"calendar\"")
        || request.contains("FolderId Id='calendar'")
    {
        return Some(FolderKind::Calendar);
    }
    if request.contains("DistinguishedFolderId Id=\"contacts\"")
        || request.contains("DistinguishedFolderId Id='contacts'")
        || request.contains("FolderId Id=\"contacts\"")
        || request.contains("FolderId Id='contacts'")
    {
        return Some(FolderKind::Contacts);
    }
    if request.contains("DistinguishedFolderId Id=\"tasks\"")
        || request.contains("DistinguishedFolderId Id='tasks'")
        || request.contains("FolderId Id=\"tasks\"")
        || request.contains("FolderId Id='tasks'")
    {
        return Some(FolderKind::Tasks);
    }
    if request.contains("public-folder:") {
        return Some(FolderKind::PublicFolders);
    }
    if request.contains("mailbox:") || !requested_mailbox_folder_ids(request).is_empty() {
        return Some(FolderKind::Mailbox);
    }
    if requested_mailbox_role(request).is_some() {
        return Some(FolderKind::Mailbox);
    }
    requested_collection_id(request).and_then(|id| {
        if id.starts_with("shared-calendar-") {
            Some(FolderKind::Calendar)
        } else if id.starts_with("shared-contacts-") {
            Some(FolderKind::Contacts)
        } else if id.starts_with("shared-tasks-") {
            Some(FolderKind::Tasks)
        } else if id.starts_with("public-folder:") {
            Some(FolderKind::PublicFolders)
        } else if id.starts_with("mailbox:") || Uuid::parse_str(id).is_ok() {
            Some(FolderKind::Mailbox)
        } else if id == "msgfolderroot" || id == "root" {
            Some(FolderKind::Root)
        } else {
            None
        }
    })
}

fn sync_state_folder_kind(sync_state: &str) -> Option<FolderKind> {
    if sync_state.starts_with("contacts:") {
        Some(FolderKind::Contacts)
    } else if sync_state.starts_with("calendar:") {
        Some(FolderKind::Calendar)
    } else if sync_state.starts_with("tasks:") {
        Some(FolderKind::Tasks)
    } else if sync_state.starts_with("mailbox:") {
        Some(FolderKind::Mailbox)
    } else if sync_state.starts_with("public-folder:") {
        Some(FolderKind::PublicFolders)
    } else if sync_state.starts_with("root:") {
        Some(FolderKind::Root)
    } else {
        None
    }
}

pub(in crate::service) fn requested_folder_kinds(request: &str) -> Vec<FolderKind> {
    let mut kinds = Vec::new();
    if request.contains("DistinguishedFolderId Id=\"msgfolderroot\"")
        || request.contains("DistinguishedFolderId Id='msgfolderroot'")
        || request.contains("DistinguishedFolderId Id=\"root\"")
        || request.contains("DistinguishedFolderId Id='root'")
        || request.contains("FolderId Id=\"msgfolderroot\"")
        || request.contains("FolderId Id='msgfolderroot'")
        || request.contains("FolderId Id=\"root\"")
        || request.contains("FolderId Id='root'")
    {
        kinds.push(FolderKind::Root);
    }
    if request.contains("DistinguishedFolderId Id=\"contacts\"")
        || request.contains("DistinguishedFolderId Id='contacts'")
        || request.contains("FolderId Id=\"contacts\"")
        || request.contains("FolderId Id='contacts'")
        || request.contains("shared-contacts-")
    {
        kinds.push(FolderKind::Contacts);
    }
    if request.contains("DistinguishedFolderId Id=\"calendar\"")
        || request.contains("DistinguishedFolderId Id='calendar'")
        || request.contains("FolderId Id=\"calendar\"")
        || request.contains("FolderId Id='calendar'")
        || request.contains("shared-calendar-")
    {
        kinds.push(FolderKind::Calendar);
    }
    if request.contains("DistinguishedFolderId Id=\"tasks\"")
        || request.contains("DistinguishedFolderId Id='tasks'")
        || request.contains("FolderId Id=\"tasks\"")
        || request.contains("FolderId Id='tasks'")
        || request.contains("shared-tasks-")
    {
        kinds.push(FolderKind::Tasks);
    }
    if request.contains("public-folder:") {
        kinds.push(FolderKind::PublicFolders);
    }
    if request.contains("mailbox:") || !requested_mailbox_folder_ids(request).is_empty() {
        kinds.push(FolderKind::Mailbox);
    }
    if requested_mailbox_role(request).is_some() {
        kinds.push(FolderKind::Mailbox);
    }
    kinds.dedup();
    kinds
}
