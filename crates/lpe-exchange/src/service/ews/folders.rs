use serde::{Deserialize, Serialize};

use super::super::*;
use super::folder_requests::{
    parse_get_folder_targets, requested_find_folder_parent, FindFolderParent, GetFolderTarget,
};
use super::sync_state::{ews_sync_cursor_token, parse_ews_sync_cursor_token, requested_sync_state};

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
        request: &str,
    ) -> Result<String> {
        let parent = requested_find_folder_parent(request)?;
        let mut folders = String::new();
        let mailboxes = self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?;
        match parent {
            None => {
                for mailbox in &mailboxes {
                    folders.push_str(&mailbox_folder_xml(mailbox));
                }
                self.append_collection_folders(principal, &mut folders, None)
                    .await?;
                for folder in self.public_folder_tree_folders(principal).await? {
                    folders.push_str(&self.public_folder_projection(principal, &folder).await?);
                }
            }
            Some(FindFolderParent::Root) => {
                for mailbox in mailboxes
                    .iter()
                    .filter(|mailbox| mailbox.parent_id.is_none())
                {
                    folders.push_str(&mailbox_folder_xml(mailbox));
                }
                self.append_collection_folders(principal, &mut folders, None)
                    .await?;
                for folder in self
                    .public_folder_tree_folders(principal)
                    .await?
                    .into_iter()
                    .filter(|folder| folder.parent_folder_id.is_none())
                {
                    folders.push_str(&self.public_folder_projection(principal, &folder).await?);
                }
            }
            Some(FindFolderParent::Mailbox(parent_id)) => {
                if !mailboxes.iter().any(|mailbox| mailbox.id == parent_id) {
                    bail!("requested mailbox folder is not exposed by EWS");
                }
                for mailbox in mailboxes
                    .iter()
                    .filter(|mailbox| mailbox.parent_id == Some(parent_id))
                {
                    folders.push_str(&mailbox_folder_xml(mailbox));
                }
            }
            Some(FindFolderParent::MailboxRole(role)) => {
                let parent_id = mailboxes
                    .iter()
                    .find(|mailbox| mailbox.role == role)
                    .map(|mailbox| mailbox.id)
                    .ok_or_else(|| anyhow!("requested mailbox folder is not exposed by EWS"))?;
                for mailbox in mailboxes
                    .iter()
                    .filter(|mailbox| mailbox.parent_id == Some(parent_id))
                {
                    folders.push_str(&mailbox_folder_xml(mailbox));
                }
            }
            Some(FindFolderParent::PublicFolder(parent_id)) => {
                self.store
                    .fetch_public_folder(principal.account_id, parent_id)
                    .await?;
                for folder in self
                    .store
                    .fetch_public_folder_children(principal.account_id, parent_id)
                    .await?
                {
                    folders.push_str(&self.public_folder_projection(principal, &folder).await?);
                }
            }
            Some(FindFolderParent::Collection(kind)) => {
                self.append_collection_folders(principal, &mut folders, Some(kind))
                    .await?;
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

    async fn append_collection_folders(
        &self,
        principal: &AccountPrincipal,
        folders: &mut String,
        only_kind: Option<FolderKind>,
    ) -> Result<()> {
        if only_kind.is_none() || only_kind == Some(FolderKind::Contacts) {
            for collection in self
                .store
                .fetch_accessible_contact_collections(principal.account_id)
                .await?
            {
                folders.push_str(
                    &self
                        .collection_folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts")
                        .await?,
                );
            }
        }
        if only_kind.is_none() || only_kind == Some(FolderKind::Calendar) {
            for collection in self
                .store
                .fetch_accessible_calendar_collections(principal.account_id)
                .await?
            {
                folders.push_str(
                    &self
                        .collection_folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar")
                        .await?,
                );
            }
        }
        if only_kind.is_none() || only_kind == Some(FolderKind::Tasks) {
            for collection in self
                .store
                .fetch_accessible_task_collections(principal.account_id)
                .await?
            {
                folders.push_str(
                    &self
                        .collection_folder_xml(&collection, TASKS_FOLDER_ID, "Task")
                        .await?,
                );
            }
        }
        Ok(())
    }

    async fn public_folder_tree_folders(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<Vec<PublicFolder>> {
        let mut folders = Vec::new();
        for tree in self
            .store
            .fetch_public_folder_trees(principal.account_id)
            .await?
        {
            if let Some(root_folder_id) = tree.root_folder_id {
                folders.push(
                    self.store
                        .fetch_public_folder(principal.account_id, root_folder_id)
                        .await?,
                );
            }
        }
        let mut index = 0;
        while let Some(folder) = folders.get(index) {
            folders.extend(
                self.store
                    .fetch_public_folder_children(principal.account_id, folder.id)
                    .await?,
            );
            index += 1;
        }
        Ok(folders)
    }

    async fn public_folder_projection(
        &self,
        principal: &AccountPrincipal,
        folder: &PublicFolder,
    ) -> Result<String> {
        let children = self
            .store
            .fetch_public_folder_children(principal.account_id, folder.id)
            .await?;
        let items = self
            .store
            .fetch_public_folder_items(principal.account_id, folder.id)
            .await?;
        Ok(public_folder_xml(
            folder,
            folder.parent_folder_id,
            children.len(),
            items.len(),
        ))
    }

    pub(in crate::service) async fn sync_folder_hierarchy(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let max_changes = match requested_hierarchy_max_changes(request) {
            Ok(value) => value,
            Err(error) => {
                return Ok(operation_error_response(
                    "SyncFolderHierarchy",
                    "ErrorInvalidSyncStateData",
                    &error.to_string(),
                ))
            }
        };
        let state = match match requested_sync_state(request) {
            Some(sync_state) if sync_state.starts_with("lpe-ews-sync.v1") => {
                let (kind, cursor_id) = parse_ews_sync_cursor_token(&sync_state)?;
                if kind != "hierarchy" {
                    bail!("SyncState does not belong to SyncFolderHierarchy");
                }
                let cursor = self
                    .store
                    .fetch_ews_sync_cursor(principal.account_id, cursor_id)
                    .await?
                    .ok_or_else(|| anyhow!("SyncState is no longer available"))?;
                if cursor.scope != "hierarchy" {
                    bail!("SyncState does not belong to SyncFolderHierarchy");
                }
                serde_json::from_value(cursor.snapshot_json)
                    .map(Some)
                    .map_err(|_| anyhow!("SyncState snapshot is invalid"))
            }
            Some(_) => bail!("SyncState is not a supported LPE synchronization cursor"),
            None => Ok(None),
        } {
            Ok(state) => state,
            Err(error) => {
                return Ok(operation_error_response(
                    "SyncFolderHierarchy",
                    "ErrorInvalidSyncStateData",
                    &error.to_string(),
                ))
            }
        };
        let previous = match state {
            Some(HierarchySyncState::Current(entries)) => entries.into_iter().collect(),
            None => HashMap::new(),
            Some(HierarchySyncState::Continuation {
                current,
                changes,
                next_change,
            }) => {
                return Ok(self
                    .hierarchy_sync_page_response(
                        principal,
                        current,
                        changes,
                        next_change,
                        max_changes,
                    )
                    .await
                    .unwrap_or_else(|error| {
                        operation_error_response(
                            "SyncFolderHierarchy",
                            "ErrorInvalidSyncStateData",
                            &error.to_string(),
                        )
                    }))
            }
        };
        let mut folders = Vec::new();
        for mailbox in self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?
        {
            let id = format!("mailbox:{}", mailbox.id);
            folders.push(HierarchySyncFolder::new(
                format!("mailbox|{id}"),
                id,
                mailbox_folder_xml(&mailbox),
            ));
        }
        for collection in self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
        {
            folders.push(HierarchySyncFolder::new(
                format!("contacts|{}", collection.id),
                collection.id.clone(),
                self.collection_folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts")
                    .await?,
            ));
        }
        for collection in self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?
        {
            folders.push(HierarchySyncFolder::new(
                format!("calendar|{}", collection.id),
                collection.id.clone(),
                self.collection_folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar")
                    .await?,
            ));
        }
        for collection in self
            .store
            .fetch_accessible_task_collections(principal.account_id)
            .await?
        {
            folders.push(HierarchySyncFolder::new(
                format!("tasks|{}", collection.id),
                collection.id.clone(),
                self.collection_folder_xml(&collection, TASKS_FOLDER_ID, "Task")
                    .await?,
            ));
        }
        for folder in self.public_folder_tree_folders(principal).await? {
            let id = format!("public-folder:{}", folder.id);
            folders.push(HierarchySyncFolder::new(
                format!("public-folder|{id}"),
                id,
                self.public_folder_projection(principal, &folder).await?,
            ));
        }
        let current = folders
            .iter()
            .map(|folder| (folder.key.as_str(), folder))
            .collect::<HashMap<_, _>>();
        let mut changes = Vec::new();
        for folder in &folders {
            match previous.get(&folder.key) {
                None => changes.push(format!("<t:Create>{}</t:Create>", folder.xml)),
                Some(fingerprint) if fingerprint != &folder.fingerprint => {
                    changes.push(format!("<t:Update>{}</t:Update>", folder.xml));
                }
                _ => {}
            }
        }
        let mut deleted = previous
            .keys()
            .filter(|key| !current.contains_key(key.as_str()))
            .collect::<Vec<_>>();
        deleted.sort_unstable();
        for key in deleted {
            if let Some((_, id)) = key.split_once('|') {
                changes.push(format!(
                    "<t:Delete><t:FolderId Id=\"{}\" ChangeKey=\"{}\"/></t:Delete>",
                    escape_xml(id),
                    escape_xml(&folder_change_key(id)),
                ));
            }
        }
        Ok(self
            .hierarchy_sync_page_response(
                principal,
                hierarchy_sync_entries(&folders),
                changes,
                0,
                max_changes,
            )
            .await
            .unwrap_or_else(|error| {
                operation_error_response(
                    "SyncFolderHierarchy",
                    "ErrorInvalidSyncStateData",
                    &error.to_string(),
                )
            }))
    }

    async fn hierarchy_sync_page_response(
        &self,
        principal: &AccountPrincipal,
        current: Vec<(String, String)>,
        changes: Vec<String>,
        next_change: usize,
        max_changes: usize,
    ) -> Result<String> {
        validate_hierarchy_sync_token_contents(&current, &changes)?;
        let end = next_change.saturating_add(max_changes).min(changes.len());
        let includes_last = end == changes.len();
        let state = if includes_last {
            HierarchySyncState::Current(current)
        } else {
            HierarchySyncState::Continuation {
                current,
                changes: changes.clone(),
                next_change: end,
            }
        };
        let cursor_id = self
            .store
            .store_ews_sync_cursor(
                principal.account_id,
                "hierarchy",
                serde_json::to_value(state)?,
            )
            .await?;
        Ok(hierarchy_sync_response(
            &ews_sync_cursor_token("hierarchy", cursor_id),
            includes_last,
            &changes[next_change..end],
        ))
    }

    pub(in crate::service) async fn create_managed_folder(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let folder_names = element_contents(request, "FolderName")
                .into_iter()
                .map(xml_text)
                .filter(|name| !name.is_empty())
                .collect::<Vec<_>>();
            if folder_names.is_empty() {
                bail!("CreateManagedFolder requires at least one FolderName.");
            }

            let mut folders = String::new();
            for folder_name in folder_names {
                let mailbox = self
                    .store
                    .create_managed_retention_folder(
                        ManagedRetentionFolderCreateInput {
                            account_id: principal.account_id,
                            folder_name: folder_name.clone(),
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-create-managed-folder".to_string(),
                            subject: folder_name,
                        },
                    )
                    .await?;
                folders.push_str(&mailbox_folder_xml(&mailbox));
            }

            Ok(folders_operation_success_response(
                "CreateManagedFolder",
                folders,
            ))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "CreateManagedFolder",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn get_folder(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let targets = match parse_get_folder_targets(request) {
            Ok(targets) => targets,
            Err(error) => {
                return Ok(get_folder_error_response(
                    "ErrorFolderNotFound",
                    &error.to_string(),
                ))
            }
        };
        let mut folders = String::new();
        for target in targets {
            match self.get_folder_target_xml(principal, target).await {
                Ok(folder) => folders.push_str(&folder),
                Err(error) => {
                    return Ok(get_folder_error_response(
                        "ErrorFolderNotFound",
                        &error.to_string(),
                    ))
                }
            }
        }
        Ok(get_folder_success_response(folders))
    }

    async fn get_folder_target_xml(
        &self,
        principal: &AccountPrincipal,
        target: GetFolderTarget,
    ) -> Result<String> {
        match target {
            GetFolderTarget::Root => Ok(root_folder_xml(self.root_child_folder_count(principal).await?)),
            GetFolderTarget::Mailbox(id) => {
                let mailbox = self
                    .store
                    .fetch_jmap_mailboxes(principal.account_id)
                    .await?
                    .into_iter()
                    .find(|mailbox| mailbox.id == id)
                    .ok_or_else(|| anyhow!("requested mailbox folder is not exposed by EWS"))?;
                Ok(mailbox_folder_xml(&mailbox))
            }
            GetFolderTarget::MailboxRole(role) => {
                let mailbox = self
                    .store
                    .fetch_jmap_mailboxes(principal.account_id)
                    .await?
                    .into_iter()
                    .find(|mailbox| mailbox.role == role)
                    .ok_or_else(|| anyhow!("requested mailbox folder is not exposed by EWS"))?;
                Ok(mailbox_folder_xml(&mailbox))
            }
            GetFolderTarget::PublicFolder(id) => {
                let folder = self.store.fetch_public_folder(principal.account_id, id).await?;
                self.public_folder_projection(principal, &folder).await
            }
            GetFolderTarget::Collection(kind, id) => match kind {
                FolderKind::Contacts => self.get_collection_folder_xml(
                    principal,
                    &id,
                    FolderKind::Contacts,
                    CONTACTS_FOLDER_ID,
                    "Contacts",
                ).await,
                FolderKind::Calendar => self.get_collection_folder_xml(
                    principal,
                    &id,
                    FolderKind::Calendar,
                    CALENDAR_FOLDER_ID,
                    "Calendar",
                ).await,
                FolderKind::Tasks => self.get_collection_folder_xml(
                    principal,
                    &id,
                    FolderKind::Tasks,
                    TASKS_FOLDER_ID,
                    "Task",
                ).await,
                _ => bail!("GetFolder collection is not supported"),
            },
        }
    }

    async fn get_collection_folder_xml(
        &self,
        principal: &AccountPrincipal,
        id: &str,
        kind: FolderKind,
        distinguished_id: &str,
        class: &str,
    ) -> Result<String> {
        let collections = match kind {
            FolderKind::Contacts => self.store.fetch_accessible_contact_collections(principal.account_id).await?,
            FolderKind::Calendar => self.store.fetch_accessible_calendar_collections(principal.account_id).await?,
            FolderKind::Tasks => self.store.fetch_accessible_task_collections(principal.account_id).await?,
            _ => unreachable!("GetFolder collection kind was validated"),
        };
        let collection = collections
            .into_iter()
            .find(|collection| collection.id == id)
            .ok_or_else(|| anyhow!("requested {class} collection is not exposed by EWS"))?;
        self.collection_folder_xml(&collection, distinguished_id, class).await
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

    async fn collection_folder_xml(
        &self,
        collection: &CollaborationCollection,
        distinguished_id: &str,
        class: &str,
    ) -> Result<String> {
        let revision = self
            .store
            .fetch_account_category_modseq(collection.owner_account_id, &collection.kind)
            .await?;
        Ok(folder_xml(
            collection,
            distinguished_id,
            class,
            &collection_folder_change_key(collection, revision),
        ))
    }
}

const HIERARCHY_SYNC_PAGE_LIMIT: usize = 200;
const HIERARCHY_SYNC_TOKEN_MAX_ENTRIES: usize = 1_024;
const HIERARCHY_SYNC_TOKEN_MAX_CHANGES: usize = 1_024;
const HIERARCHY_SYNC_TOKEN_MAX_CHANGE_BYTES: usize = 8_192;
const HIERARCHY_SYNC_TOKEN_MAX_ENTRY_BYTES: usize = 512;

struct HierarchySyncFolder {
    key: String,
    fingerprint: String,
    xml: String,
}

impl HierarchySyncFolder {
    fn new(key: String, id: String, xml: String) -> Self {
        Self {
            fingerprint: versioned_change_key("folder-hierarchy", &id, &xml),
            key,
            xml,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum HierarchySyncState {
    Current(Vec<(String, String)>),
    Continuation {
        current: Vec<(String, String)>,
        changes: Vec<String>,
        next_change: usize,
    },
}

fn requested_hierarchy_max_changes(request: &str) -> Result<usize> {
    match element_contents(request, "MaxChangesReturned").as_slice() {
        [] => Ok(HIERARCHY_SYNC_PAGE_LIMIT),
        [value] => xml_text(value)
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .map(|value| value.min(HIERARCHY_SYNC_PAGE_LIMIT))
            .ok_or_else(|| anyhow!("MaxChangesReturned must be a positive integer")),
        _ => bail!("SyncFolderHierarchy accepts at most one MaxChangesReturned"),
    }
}

fn hierarchy_sync_entries(folders: &[HierarchySyncFolder]) -> Vec<(String, String)> {
    let mut entries = folders
        .iter()
        .map(|folder| (folder.key.clone(), folder.fingerprint.clone()))
        .collect::<Vec<_>>();
    entries.sort_unstable();
    entries
}

fn hierarchy_sync_response(sync_state: &str, includes_last: bool, changes: &[String]) -> String {
    format!(
        concat!(
            "<m:SyncFolderHierarchyResponse>",
            "<m:ResponseMessages>",
            "<m:SyncFolderHierarchyResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SyncState>{sync_state}</m:SyncState>",
            "<m:IncludesLastFolderInRange>{includes_last}</m:IncludesLastFolderInRange>",
            "<m:Changes>{changes}</m:Changes>",
            "</m:SyncFolderHierarchyResponseMessage>",
            "</m:ResponseMessages>",
            "</m:SyncFolderHierarchyResponse>"
        ),
        sync_state = escape_xml(sync_state),
        includes_last = includes_last,
        changes = changes.join(""),
    )
}

fn validate_hierarchy_sync_token_contents(
    current: &[(String, String)],
    changes: &[String],
) -> Result<()> {
    if current.len() > HIERARCHY_SYNC_TOKEN_MAX_ENTRIES {
        bail!("SyncState contains too many hierarchy entries");
    }
    if current.iter().any(|(key, fingerprint)| {
        key.is_empty()
            || fingerprint.is_empty()
            || key.len() + fingerprint.len() + 1 > HIERARCHY_SYNC_TOKEN_MAX_ENTRY_BYTES
    }) {
        bail!("SyncState hierarchy entry is invalid");
    }
    if changes.len() > HIERARCHY_SYNC_TOKEN_MAX_CHANGES {
        bail!("SyncState contains too many hierarchy changes");
    }
    for change in changes {
        validate_hierarchy_sync_change(change)?;
    }
    Ok(())
}

fn validate_hierarchy_sync_change(change: &str) -> Result<()> {
    if change.len() > HIERARCHY_SYNC_TOKEN_MAX_CHANGE_BYTES
        || ![
            ("<t:Create>", "</t:Create>"),
            ("<t:Update>", "</t:Update>"),
            ("<t:Delete>", "</t:Delete>"),
        ]
        .iter()
        .any(|(start, end)| change.starts_with(start) && change.ends_with(end))
    {
        bail!("SyncState change page is invalid");
    }
    Ok(())
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

pub(in crate::service) fn validate_supplied_folder_change_key(
    supplied_change_key: Option<&str>,
    current_change_key: &str,
    id: &str,
) -> Result<()> {
    if matches!(supplied_change_key, Some(change_key) if change_key != current_change_key) {
        bail!("stale EWS ChangeKey for {id}");
    }
    Ok(())
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
        folder = public_folder_xml(folder, folder.parent_folder_id, 0, 0),
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
    change_key: &str,
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
            "<t:CreateAssociated>false</t:CreateAssociated>",
            "<t:CreateContents>{may_write}</t:CreateContents>",
            "<t:CreateHierarchy>{may_share}</t:CreateHierarchy>",
            "<t:Delete>{may_delete}</t:Delete>",
            "<t:Modify>{may_write}</t:Modify>",
            "<t:Read>{may_read}</t:Read>",
            "<t:ViewPrivateItems>{may_view_private_items}</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>0</t:UnreadCount>",
            "</t:{element}>"
        ),
        element = element,
        id = escape_xml(&collection.id),
        change_key = escape_xml(change_key),
        display = escape_xml(&collection.display_name),
        class = class,
        may_read = collection.rights.may_read,
        may_write = collection.rights.may_write,
        may_delete = collection.rights.may_delete,
        may_share = collection.rights.may_share,
        may_view_private_items = collection.is_owned || collection.rights.may_write,
    )
}

pub(in crate::service) fn mailbox_folder_xml(mailbox: &JmapMailbox) -> String {
    let parent_id = mailbox
        .parent_id
        .map(|id| format!("mailbox:{id}"))
        .unwrap_or_else(|| "msgfolderroot".to_string());
    let parent_change_key = mailbox
        .parent_id
        .map(|id| folder_change_key(&id.to_string()))
        .unwrap_or_else(|| "root".to_string());
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"mailbox:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{parent_id}\" ChangeKey=\"{parent_change_key}\"/>",
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
        change_key = mailbox_folder_change_key(mailbox),
        parent_id = escape_xml(&parent_id),
        parent_change_key = escape_xml(&parent_change_key),
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
        change_key = public_folder_change_key(folder),
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

pub(in crate::service) fn mailbox_folder_change_key(mailbox: &JmapMailbox) -> String {
    versioned_change_key(
        "mailbox-folder",
        &mailbox.id.to_string(),
        &mailbox.modseq.to_string(),
    )
}

pub(in crate::service) fn public_folder_change_key(folder: &PublicFolder) -> String {
    versioned_change_key(
        "public-folder",
        &folder.id.to_string(),
        &folder.change_counter.to_string(),
    )
}

fn collection_folder_change_key(collection: &CollaborationCollection, revision: u64) -> String {
    versioned_change_key("collection-folder", &collection.id, &revision.to_string())
}

#[cfg(test)]
mod tests {
    use super::requested_hierarchy_max_changes;
    use crate::service::ews::sync_state::{ews_sync_cursor_token, parse_ews_sync_cursor_token};
    use uuid::Uuid;

    fn account(id: &str) -> Uuid {
        Uuid::parse_str(id).unwrap()
    }

    #[test]
    fn hierarchy_uses_opaque_server_cursor_tokens() {
        let cursor_id = account("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        let token = ews_sync_cursor_token("hierarchy", cursor_id);
        assert_eq!(
            parse_ews_sync_cursor_token(&token).unwrap(),
            ("hierarchy".to_string(), cursor_id)
        );
    }

    #[test]
    fn hierarchy_max_changes_is_positive_and_capped() {
        assert_eq!(
            requested_hierarchy_max_changes(
                "<m:SyncFolderHierarchy><m:MaxChangesReturned>999</m:MaxChangesReturned></m:SyncFolderHierarchy>"
            )
            .unwrap(),
            200
        );
        assert!(requested_hierarchy_max_changes(
            "<m:SyncFolderHierarchy><m:MaxChangesReturned>0</m:MaxChangesReturned></m:SyncFolderHierarchy>"
        )
        .is_err());
    }

}
