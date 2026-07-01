use super::super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::service) enum FolderKind {
    Root,
    Contacts,
    Calendar,
    Tasks,
    Mailbox,
    PublicFolders,
}

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn find_folder(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let mut folders = String::new();
        for mailbox in self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?
        {
            folders.push_str(&mailbox_folder_xml(&mailbox));
        }
        for collection in self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts"));
        }
        for collection in self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar"));
        }
        for collection in self
            .store
            .fetch_accessible_task_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, TASKS_FOLDER_ID, "Task"));
        }
        for tree in self
            .store
            .fetch_public_folder_trees(principal.account_id)
            .await?
        {
            if let Some(root_folder_id) = tree.root_folder_id {
                let folder = self
                    .store
                    .fetch_public_folder(principal.account_id, root_folder_id)
                    .await?;
                folders.push_str(&public_folder_xml(&folder, None, 0, 0));
            }
        }

        Ok(format!(
            concat!(
                "<m:FindFolderResponse>",
                "<m:ResponseMessages>",
                "<m:FindFolderResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:RootFolder TotalItemsInView=\"{count}\" IncludesLastItemInRange=\"true\">",
                "<t:Folders>{folders}</t:Folders>",
                "</m:RootFolder>",
                "</m:FindFolderResponseMessage>",
                "</m:ResponseMessages>",
                "</m:FindFolderResponse>"
            ),
            folders = folders,
            count = count_folder_elements(&folders),
        ))
    }

    pub(in crate::service) async fn sync_folder_hierarchy(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let mut changes = String::new();
        let mut count = 0;
        for mailbox in self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&mailbox_folder_xml(&mailbox));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for collection in self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for collection in self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for collection in self
            .store
            .fetch_accessible_task_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, TASKS_FOLDER_ID, "Task"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for tree in self
            .store
            .fetch_public_folder_trees(principal.account_id)
            .await?
        {
            if let Some(root_folder_id) = tree.root_folder_id {
                let folder = self
                    .store
                    .fetch_public_folder(principal.account_id, root_folder_id)
                    .await?;
                changes.push_str("<t:Create>");
                changes.push_str(&public_folder_xml(&folder, None, 0, 0));
                changes.push_str("</t:Create>");
                count += 1;
            }
        }
        let sync_state = format!("folder-hierarchy:{count}");

        Ok(format!(
            concat!(
                "<m:SyncFolderHierarchyResponse>",
                "<m:ResponseMessages>",
                "<m:SyncFolderHierarchyResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:SyncState>{sync_state}</m:SyncState>",
                "<m:IncludesLastFolderInRange>true</m:IncludesLastFolderInRange>",
                "<m:Changes>{changes}</m:Changes>",
                "</m:SyncFolderHierarchyResponseMessage>",
                "</m:ResponseMessages>",
                "</m:SyncFolderHierarchyResponse>"
            ),
            sync_state = escape_xml(&sync_state),
            changes = changes,
        ))
    }

    pub(in crate::service) async fn get_folder(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let mailbox_ids = self
            .requested_mailbox_folder_ids(principal, request)
            .await?;
        if !mailbox_ids.is_empty() {
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            let mut folders = String::new();
            for mailbox_id in &mailbox_ids {
                let Some(mailbox) = mailboxes.iter().find(|mailbox| mailbox.id == *mailbox_id)
                else {
                    return Ok(get_folder_error_response(
                        "ErrorFolderNotFound",
                        "requested mailbox folder is not exposed by EWS",
                    ));
                };
                folders.push_str(&mailbox_folder_xml(mailbox));
            }

            return Ok(get_folder_success_response(folders));
        }

        let public_folder_ids = requested_public_folder_ids(request);
        if !public_folder_ids.is_empty() {
            let mut folders = String::new();
            for folder_id in public_folder_ids {
                let folder = self
                    .store
                    .fetch_public_folder(principal.account_id, folder_id)
                    .await?;
                let children = self
                    .store
                    .fetch_public_folder_children(principal.account_id, folder_id)
                    .await?;
                let items = self
                    .store
                    .fetch_public_folder_items(principal.account_id, folder_id)
                    .await?;
                folders.push_str(&public_folder_xml(
                    &folder,
                    folder.parent_folder_id,
                    children.len(),
                    items.len(),
                ));
            }
            return Ok(get_folder_success_response(folders));
        }

        let requested = requested_folder_kinds(request);
        if requested.is_empty() && request_contains_folder_reference(request) {
            return Ok(get_folder_error_response(
                "ErrorFolderNotFound",
                "folder not found",
            ));
        }

        let mut folders = String::new();
        for kind in requested {
            match kind {
                FolderKind::Root => {
                    folders.push_str(&root_folder_xml(
                        self.root_child_folder_count(principal).await?,
                    ));
                }
                FolderKind::Contacts => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_contact_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| {
                                folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts")
                            })
                            .collect::<String>(),
                    );
                }
                FolderKind::Calendar => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_calendar_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| {
                                folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar")
                            })
                            .collect::<String>(),
                    );
                }
                FolderKind::Tasks => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_task_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| folder_xml(&collection, TASKS_FOLDER_ID, "Task"))
                            .collect::<String>(),
                    );
                }
                FolderKind::Mailbox => {
                    let mailbox_ids = self
                        .requested_mailbox_folder_ids(principal, request)
                        .await?;
                    let mailboxes = self
                        .store
                        .fetch_jmap_mailboxes(principal.account_id)
                        .await?;
                    for mailbox in mailboxes.into_iter().filter(|mailbox| {
                        mailbox_ids.is_empty() || mailbox_ids.contains(&mailbox.id)
                    }) {
                        folders.push_str(&mailbox_folder_xml(&mailbox));
                    }
                }
                FolderKind::PublicFolders => {
                    for folder_id in requested_public_folder_ids(request) {
                        let folder = self
                            .store
                            .fetch_public_folder(principal.account_id, folder_id)
                            .await?;
                        let children = self
                            .store
                            .fetch_public_folder_children(principal.account_id, folder_id)
                            .await?;
                        let items = self
                            .store
                            .fetch_public_folder_items(principal.account_id, folder_id)
                            .await?;
                        folders.push_str(&public_folder_xml(
                            &folder,
                            folder.parent_folder_id,
                            children.len(),
                            items.len(),
                        ));
                    }
                }
            }
        }

        if folders.is_empty() {
            return Ok(get_folder_error_response(
                "ErrorFolderNotFound",
                "folder not found",
            ));
        }

        Ok(get_folder_success_response(folders))
    }

    pub(in crate::service) async fn root_child_folder_count(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<usize> {
        Ok(self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
            .len()
            + self
                .store
                .fetch_accessible_calendar_collections(principal.account_id)
                .await?
                .len()
            + self
                .store
                .fetch_accessible_task_collections(principal.account_id)
                .await?
                .len()
            + self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?
                .len()
            + self
                .store
                .fetch_public_folder_trees(principal.account_id)
                .await?
                .into_iter()
                .filter(|tree| tree.root_folder_id.is_some())
                .count())
    }
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

pub(in crate::service) fn mailbox_by_id(
    mailboxes: &[JmapMailbox],
    mailbox_id: Uuid,
) -> Result<&JmapMailbox> {
    mailboxes
        .iter()
        .find(|mailbox| mailbox.id == mailbox_id)
        .ok_or_else(|| anyhow!("mailbox folder not found"))
}

pub(in crate::service) fn ensure_custom_mailbox(mailbox: &JmapMailbox) -> Result<()> {
    if mailbox.role == "custom" {
        Ok(())
    } else {
        bail!("system mailbox folders cannot be moved, copied, updated, or deleted as subfolders")
    }
}

pub(in crate::service) fn create_folder_success_response(mailbox: &JmapMailbox) -> String {
    format!(
        concat!(
            "<m:CreateFolderResponse>",
            "<m:ResponseMessages>",
            "<m:CreateFolderResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Folders>{folder}</m:Folders>",
            "</m:CreateFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateFolderResponse>"
        ),
        folder = mailbox_folder_xml(mailbox),
    )
}

pub(in crate::service) fn create_public_folder_success_response(folder: &PublicFolder) -> String {
    format!(
        concat!(
            "<m:CreateFolderResponse>",
            "<m:ResponseMessages>",
            "<m:CreateFolderResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Folders>{folder}</m:Folders>",
            "</m:CreateFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateFolderResponse>"
        ),
        folder = public_folder_xml(folder, None, 0, 0),
    )
}

pub(in crate::service) fn folders_operation_success_response(
    operation: &str,
    folders: String,
) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Folders>{folders}</m:Folders>",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = operation,
        folders = folders,
    )
}

fn get_folder_success_response(folders: String) -> String {
    format!(
        concat!(
            "<m:GetFolderResponse>",
            "<m:ResponseMessages>",
            "<m:GetFolderResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Folders>{folders}</m:Folders>",
            "</m:GetFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetFolderResponse>"
        ),
        folders = folders,
    )
}

pub(in crate::service) fn delete_folder_success_response() -> String {
    concat!(
        "<m:DeleteFolderResponse>",
        "<m:ResponseMessages>",
        "<m:DeleteFolderResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:DeleteFolderResponseMessage>",
        "</m:ResponseMessages>",
        "</m:DeleteFolderResponse>"
    )
    .to_string()
}

pub(in crate::service) fn root_folder_xml(child_folder_count: usize) -> String {
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>",
            "<t:FolderClass>IPF.Note</t:FolderClass>",
            "<t:DisplayName>Root</t:DisplayName>",
            "<t:TotalCount>0</t:TotalCount>",
            "<t:ChildFolderCount>{child_folder_count}</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>true</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>0</t:UnreadCount>",
            "</t:Folder>"
        ),
        child_folder_count = child_folder_count,
    )
}

pub(in crate::service) fn folder_xml(
    collection: &CollaborationCollection,
    distinguished_id: &str,
    class: &str,
) -> String {
    let element = match distinguished_id {
        CONTACTS_FOLDER_ID => "ContactsFolder",
        CALENDAR_FOLDER_ID => "CalendarFolder",
        TASKS_FOLDER_ID => "TasksFolder",
        _ => "Folder",
    };
    format!(
        concat!(
            "<t:{element}>",
            "<t:FolderId Id=\"{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>",
            "<t:FolderClass>IPF.{class}</t:FolderClass>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:TotalCount>0</t:TotalCount>",
            "<t:ChildFolderCount>0</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>true</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>0</t:UnreadCount>",
            "</t:{element}>"
        ),
        element = element,
        id = escape_xml(&collection.id),
        change_key = escape_xml(&folder_change_key(&collection.id)),
        display = escape_xml(&collection.display_name),
        class = class,
    )
}

pub(in crate::service) fn mailbox_folder_xml(mailbox: &JmapMailbox) -> String {
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"mailbox:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>",
            "<t:FolderClass>IPF.Note</t:FolderClass>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:TotalCount>{total_count}</t:TotalCount>",
            "<t:ChildFolderCount>0</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>true</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>{unread_count}</t:UnreadCount>",
            "</t:Folder>"
        ),
        id = mailbox.id,
        change_key = folder_change_key(&mailbox.id.to_string()),
        display = escape_xml(&mailbox.name),
        total_count = mailbox.total_emails,
        unread_count = mailbox.unread_emails,
    )
}

pub(in crate::service) fn public_folder_xml(
    folder: &PublicFolder,
    parent_folder_id: Option<Uuid>,
    child_folder_count: usize,
    item_count: usize,
) -> String {
    let parent_id = parent_folder_id
        .map(|id| format!("public-folder:{id}"))
        .unwrap_or_else(|| "msgfolderroot".to_string());
    let parent_change_key = parent_folder_id
        .map(|id| folder_change_key(&format!("public-folder:{id}")))
        .unwrap_or_else(|| "root".to_string());
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"public-folder:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{parent_id}\" ChangeKey=\"{parent_change_key}\"/>",
            "<t:FolderClass>{class}</t:FolderClass>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:TotalCount>{item_count}</t:TotalCount>",
            "<t:ChildFolderCount>{child_folder_count}</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>false</t:CreateAssociated>",
            "<t:CreateContents>{may_write}</t:CreateContents>",
            "<t:CreateHierarchy>{may_share}</t:CreateHierarchy>",
            "<t:Delete>{may_delete}</t:Delete>",
            "<t:Modify>{may_write}</t:Modify>",
            "<t:Read>{may_read}</t:Read>",
            "<t:ViewPrivateItems>false</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>0</t:UnreadCount>",
            "</t:Folder>"
        ),
        id = folder.id,
        change_key = folder_change_key(&format!("public-folder:{}", folder.id)),
        parent_id = escape_xml(&parent_id),
        parent_change_key = escape_xml(&parent_change_key),
        class = escape_xml(&folder.folder_class),
        display = escape_xml(&folder.display_name),
        item_count = item_count,
        child_folder_count = child_folder_count,
        may_read = folder.rights.may_read,
        may_write = folder.rights.may_write,
        may_delete = folder.rights.may_delete,
        may_share = folder.rights.may_share,
    )
}

pub(in crate::service) fn folder_change_key(id: &str) -> String {
    format!("ck-{id}")
}
