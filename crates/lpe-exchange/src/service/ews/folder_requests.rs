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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::service) struct UpdateFolderRequest {
    pub(in crate::service) target: FolderOperationTarget,
    pub(in crate::service) display_name: String,
    pub(in crate::service) change_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::service) enum FindFolderParent {
    Root,
    Mailbox(Uuid),
    MailboxRole(&'static str),
    PublicFolder(Uuid),
    Collection(FolderKind),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::service) enum GetFolderTarget {
    Root,
    Mailbox(Uuid),
    MailboxRole(&'static str),
    PublicFolder(Uuid),
    Collection(FolderKind, String),
}

/// [MS-OXWSFOLD] sections 2.2.4.4 and 3.1.4.6: GetFolder consumes one
/// FolderIds sequence. Preserve every supported target in wire order so an
/// unsupported target cannot be dropped into an unscoped projection.
pub(in crate::service) fn parse_get_folder_targets(request: &str) -> Result<Vec<GetFolderTarget>> {
    let wrappers = element_contents(request, "FolderIds");
    if wrappers.len() != 1 {
        bail!("GetFolder requires exactly one FolderIds collection");
    }
    let wrapper = wrappers[0];
    if attribute_values_for_tag(request, "FolderId", "Id").len()
        != attribute_values_for_tag(wrapper, "FolderId", "Id").len()
        || attribute_values_for_tag(request, "DistinguishedFolderId", "Id").len()
            != attribute_values_for_tag(wrapper, "DistinguishedFolderId", "Id").len()
    {
        bail!("GetFolder FolderIds must contain every folder target");
    }

    let mut targets = Vec::new();
    let mut rest = wrapper.trim();
    while !rest.is_empty() {
        if !rest.starts_with('<') {
            bail!("GetFolder FolderIds contains unsupported content");
        }
        let tag_end = rest
            .find('>')
            .ok_or_else(|| anyhow!("GetFolder FolderIds is malformed"))?;
        let tag = &rest[1..tag_end];
        if tag.starts_with('/') || !tag.trim_end().ends_with('/') {
            bail!("GetFolder FolderIds contains unsupported content");
        }
        let name = tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()
            .and_then(|name| name.rsplit(':').next())
            .ok_or_else(|| anyhow!("GetFolder FolderIds is malformed"))?;
        let id = attribute_value(tag, "Id")
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| anyhow!("GetFolder folder id is invalid"))?;
        targets.push(match name {
            "FolderId" => parse_get_folder_id(id)?,
            "DistinguishedFolderId" => parse_get_distinguished_folder_id(id)?,
            _ => bail!("GetFolder folder id is not supported"),
        });
        rest = rest[tag_end + 1..].trim();
    }

    if targets.is_empty() {
        bail!("GetFolder requires at least one supported FolderId");
    }
    Ok(targets)
}

fn parse_get_folder_id(id: &str) -> Result<GetFolderTarget> {
    if let Some(id) = id.strip_prefix("mailbox:") {
        return Ok(GetFolderTarget::Mailbox(parse_folder_uuid(id, "mailbox")?));
    }
    if let Some(id) = id.strip_prefix("public-folder:") {
        return Ok(GetFolderTarget::PublicFolder(parse_folder_uuid(
            id,
            "public folder",
        )?));
    }
    if let Ok(id) = Uuid::parse_str(id) {
        return Ok(GetFolderTarget::Mailbox(id));
    }
    parse_get_distinguished_folder_id(id)
}

fn parse_get_distinguished_folder_id(id: &str) -> Result<GetFolderTarget> {
    match id {
        "msgfolderroot" | "root" => Ok(GetFolderTarget::Root),
        "contacts" => Ok(GetFolderTarget::Collection(
            FolderKind::Contacts,
            DEFAULT_COLLECTION_ID.to_string(),
        )),
        "calendar" => Ok(GetFolderTarget::Collection(
            FolderKind::Calendar,
            DEFAULT_COLLECTION_ID.to_string(),
        )),
        "tasks" => Ok(GetFolderTarget::Collection(
            FolderKind::Tasks,
            DEFAULT_COLLECTION_ID.to_string(),
        )),
        id if id.starts_with("shared-contacts-") => {
            Ok(GetFolderTarget::Collection(FolderKind::Contacts, id.to_string()))
        }
        id if id.starts_with("shared-calendar-") => {
            Ok(GetFolderTarget::Collection(FolderKind::Calendar, id.to_string()))
        }
        id if id.starts_with("shared-tasks-") => {
            Ok(GetFolderTarget::Collection(FolderKind::Tasks, id.to_string()))
        }
        id => ews_distinguished_mailbox_role(id)
            .map(GetFolderTarget::MailboxRole)
            .ok_or_else(|| anyhow!("GetFolder distinguished folder is not supported")),
    }
}

/// [MS-OXWSFOLD] sections 2.2.4.2, 2.2.4.16, and 3.1.4.2 require an
/// existing parent and a supported folder shape. LPE deliberately accepts one
/// canonical `IPF.Note` folder per request so validation completes before its
/// single canonical mutation.
pub(in crate::service) fn parse_create_folder_request(
    request: &str,
) -> Result<CreateFolderRequest> {
    let folders = element_contents(request, "Folders");
    if folders.len() != 1 {
        bail!("CreateFolder requires exactly one Folders collection");
    }
    let folders = folders[0];
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
    let folder_ids = element_contents(request, "FolderIds");
    if folder_ids.len() != 1 {
        bail!("DeleteFolder requires exactly one FolderIds collection");
    }
    parse_single_folder_target(folder_ids[0], "DeleteFolder")
}

/// [MS-OXWSFOLD] section 3.1.4.8: LPE accepts one DisplayName update for one
/// canonical folder so the complete request can be rejected before mutation.
pub(in crate::service) fn parse_update_folder_request(
    request: &str,
) -> Result<UpdateFolderRequest> {
    let changes = element_contents(request, "FolderChanges");
    if changes.len() != 1 {
        bail!("UpdateFolder requires exactly one FolderChanges collection");
    }
    let changes = element_contents(changes[0], "FolderChange");
    if changes.len() != 1 {
        bail!("UpdateFolder requires exactly one FolderChange");
    }
    let change = changes[0];
    let target = parse_single_folder_target(change, "UpdateFolder")?;
    let change_key = attribute_values_for_tag(change, "FolderId", "ChangeKey")
        .into_iter()
        .next()
        .map(str::to_string);
    let updates = element_contents(change, "Updates");
    if updates.len() != 1 {
        bail!("UpdateFolder requires exactly one Updates collection");
    }
    let fields = element_contents(updates[0], "SetFolderField");
    if fields.len() != 1
        || !element_contents(updates[0], "DeleteFolderField").is_empty()
        || !element_contents(updates[0], "AppendToFolderField").is_empty()
    {
        bail!("UpdateFolder supports exactly one DisplayName SetFolderField");
    }
    let field = fields[0];
    if attribute_values_for_tag(field, "FieldURI", "FieldURI") != ["folder:DisplayName"] {
        bail!("UpdateFolder supports only folder:DisplayName");
    }
    let folders = element_contents(field, "Folder");
    if folders.len() != 1
        || [
            "CalendarFolder",
            "ContactsFolder",
            "SearchFolder",
            "TasksFolder",
        ]
        .into_iter()
        .any(|name| !element_contents(field, name).is_empty())
    {
        bail!("UpdateFolder requires one Folder DisplayName value");
    }
    let display_name = element_text(folders[0], "DisplayName")
        .filter(|name| !name.trim().is_empty())
        .ok_or_else(|| anyhow!("UpdateFolder requires DisplayName"))?;
    Ok(UpdateFolderRequest {
        target,
        display_name,
        change_key,
    })
}

/// [MS-OXWSFOLD] section 3.1.4.5: one bounded folder target prevents a
/// mixed/public request from partially emptying canonical state.
pub(in crate::service) fn parse_empty_folder_target(
    request: &str,
) -> Result<FolderOperationTarget> {
    parse_folder_ids_target(request, "EmptyFolder")
}

pub(in crate::service) fn parse_mark_all_items_as_read_target(
    request: &str,
) -> Result<FolderOperationTarget> {
    parse_folder_ids_target(request, "MarkAllItemsAsRead")
}

fn parse_folder_ids_target(request: &str, operation: &str) -> Result<FolderOperationTarget> {
    let folder_ids = element_contents(request, "FolderIds");
    if folder_ids.len() != 1 {
        bail!("{operation} requires exactly one FolderIds collection");
    }
    parse_single_folder_target(folder_ids[0], operation)
}

fn parse_create_folder_parent(request: &str) -> Result<CreateFolderParent> {
    let parents = element_contents(request, "ParentFolderId");
    if parents.len() != 1 {
        bail!("CreateFolder requires exactly one ParentFolderId");
    }
    let parent = parents[0];
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
    let parents = element_contents(request, "ParentFolderIds");
    if parents.is_empty() {
        return Ok(None);
    }
    if parents.len() != 1 {
        bail!("FindFolder requires exactly one ParentFolderIds collection");
    }
    let parent = parents[0];
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
