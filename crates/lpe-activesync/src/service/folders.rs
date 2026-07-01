use anyhow::{bail, Result};
use axum::response::Response;
use lpe_domain::MailboxNamePolicy;
use lpe_storage::{AuditEntryInput, JmapMailboxCreateInput, JmapMailboxUpdateInput};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    constants::{FOLDER_SYNC_COLLECTION_ID, ROOT_FOLDER_ID},
    protocol::{ActiveSyncFolderType, ActiveSyncStatus},
    response::wbxml_response,
    snapshot::{diff_snapshots, mail_collection, snapshot_to_value},
    store::ActiveSyncStore,
    types::{AuthenticatedPrincipal, CollectionDefinition, SnapshotEntry},
    wbxml::{encode_wbxml, WbxmlNode},
};

use super::{command_status_response, ActiveSyncService};

impl<S: ActiveSyncStore> ActiveSyncService<S> {
    pub(super) async fn handle_folder_sync(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "FolderSync" {
            return command_status_response(protocol_version, 7, "FolderSync", "10");
        }

        let requested_key = request
            .child("SyncKey")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let collections = self.folder_collections(principal.account_id).await?;
        let snapshot = folder_hierarchy_snapshot(&collections);

        let old_snapshot = if requested_key == "0" || requested_key.is_empty() {
            None
        } else {
            let Some(state) = self
                .load_requested_sync_state(
                    principal.account_id,
                    device_id,
                    FOLDER_SYNC_COLLECTION_ID,
                    &requested_key,
                )
                .await?
            else {
                let mut response = WbxmlNode::new(7, "FolderSync");
                response.push(WbxmlNode::with_text(7, "Status", "9"));
                return wbxml_response(protocol_version, encode_wbxml(&response));
            };
            Some(serde_json::from_str(&state.snapshot_json)?)
        };

        if !requested_key.is_empty() && requested_key != "0" && old_snapshot.is_none() {
            let mut response = WbxmlNode::new(7, "FolderSync");
            response.push(WbxmlNode::with_text(7, "Status", "9"));
            return wbxml_response(protocol_version, encode_wbxml(&response));
        }

        let new_key = Uuid::new_v4().to_string();
        self.store
            .store_activesync_sync_state(
                principal.account_id,
                device_id,
                FOLDER_SYNC_COLLECTION_ID,
                &new_key,
                snapshot.to_string(),
            )
            .await?;

        let mut response = WbxmlNode::new(7, "FolderSync");
        response.push(WbxmlNode::with_text(
            7,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        response.push(WbxmlNode::with_text(7, "SyncKey", new_key));

        let changes = diff_snapshots(old_snapshot.as_ref(), &snapshot);
        let mut changes_node = WbxmlNode::new(7, "Changes");
        changes_node.push(WbxmlNode::with_text(7, "Count", changes.len().to_string()));
        for change in changes {
            match change.kind.as_str() {
                "Add" => {
                    if let Some(collection) = collections
                        .iter()
                        .find(|collection| collection.id == change.server_id)
                    {
                        let mut node = WbxmlNode::new(7, "Add");
                        push_folder_metadata(&mut node, collection);
                        changes_node.push(node);
                    }
                }
                "Update" => {
                    if let Some(collection) = collections
                        .iter()
                        .find(|collection| collection.id == change.server_id)
                    {
                        let mut node = WbxmlNode::new(7, "Update");
                        push_folder_metadata(&mut node, collection);
                        changes_node.push(node);
                    }
                }
                "Delete" => {
                    let mut node = WbxmlNode::new(7, "Delete");
                    node.push(WbxmlNode::with_text(7, "ServerId", &change.server_id));
                    changes_node.push(node);
                }
                _ => {}
            }
        }
        response.push(changes_node);
        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    pub(super) async fn handle_folder_create(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "FolderCreate" {
            bail!("invalid FolderCreate payload");
        }

        let Some(sync_key) = request
            .child("SyncKey")
            .map(|node| node.text_value().trim())
            .filter(|value| !value.is_empty())
        else {
            return folder_mutation_response(protocol_version, "FolderCreate", "10", None, None);
        };
        let Some(parent_id) = request
            .child("ParentId")
            .map(|node| node.text_value().trim())
            .filter(|value| !value.is_empty())
        else {
            return folder_mutation_response(protocol_version, "FolderCreate", "10", None, None);
        };
        let Some(display_name) = request
            .child("DisplayName")
            .map(|node| node.text_value().trim())
            .filter(|value| !value.is_empty())
        else {
            return folder_mutation_response(protocol_version, "FolderCreate", "10", None, None);
        };
        let folder_type = request
            .child("Type")
            .map(|node| node.text_value().trim())
            .unwrap_or_default();
        if folder_type != ActiveSyncFolderType::UserCreatedMail.as_str()
            || MailboxNamePolicy::system_role_for_display_name(display_name).is_some()
        {
            return folder_mutation_response(protocol_version, "FolderCreate", "10", None, None);
        }

        if self
            .load_requested_sync_state(
                principal.account_id,
                device_id,
                FOLDER_SYNC_COLLECTION_ID,
                sync_key,
            )
            .await?
            .is_none()
        {
            return folder_mutation_response(protocol_version, "FolderCreate", "9", None, None);
        }

        let parent_mailbox_id = if parent_id == ROOT_FOLDER_ID {
            None
        } else {
            let Some(parent) = self
                .resolve_collection(principal.account_id, parent_id)
                .await?
                .filter(|collection| {
                    collection.account_id == principal.account_id
                        && mail_collection(collection)
                        && collection.mailbox_id.is_some()
                })
            else {
                return folder_mutation_response(protocol_version, "FolderCreate", "5", None, None);
            };
            parent.mailbox_id
        };

        let created = match self
            .store
            .create_jmap_mailbox(
                JmapMailboxCreateInput {
                    account_id: principal.account_id,
                    name: display_name.to_string(),
                    parent_id: parent_mailbox_id,
                    sort_order: None,
                    is_subscribed: true,
                },
                active_sync_audit(principal, "activesync.folder_create", display_name),
            )
            .await
        {
            Ok(created) => created,
            Err(error) => {
                let status = folder_create_error_status(&error.to_string());
                return folder_mutation_response(
                    protocol_version,
                    "FolderCreate",
                    status,
                    None,
                    None,
                );
            }
        };

        let new_key = self
            .store_current_folder_hierarchy(principal.account_id, device_id)
            .await?;
        folder_mutation_response(
            protocol_version,
            "FolderCreate",
            "1",
            Some(&new_key),
            Some(&created.id.to_string()),
        )
    }

    pub(super) async fn handle_folder_delete(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "FolderDelete" {
            bail!("invalid FolderDelete payload");
        }

        let Some(sync_key) = request
            .child("SyncKey")
            .map(|node| node.text_value().trim())
            .filter(|value| !value.is_empty())
        else {
            return folder_mutation_response(protocol_version, "FolderDelete", "10", None, None);
        };
        let Some(server_id) = request
            .child("ServerId")
            .map(|node| node.text_value().trim())
            .filter(|value| !value.is_empty())
        else {
            return folder_mutation_response(protocol_version, "FolderDelete", "10", None, None);
        };

        if self
            .load_requested_sync_state(
                principal.account_id,
                device_id,
                FOLDER_SYNC_COLLECTION_ID,
                sync_key,
            )
            .await?
            .is_none()
        {
            return folder_mutation_response(protocol_version, "FolderDelete", "9", None, None);
        }

        let Some(folder) = self
            .owned_mail_folder(principal.account_id, server_id)
            .await?
        else {
            return folder_mutation_response(protocol_version, "FolderDelete", "4", None, None);
        };
        if folder.folder_type != ActiveSyncFolderType::UserCreatedMail {
            return folder_mutation_response(protocol_version, "FolderDelete", "3", None, None);
        }
        let Some(mailbox_id) = folder.mailbox_id else {
            return folder_mutation_response(protocol_version, "FolderDelete", "4", None, None);
        };

        if let Err(error) = self
            .store
            .destroy_jmap_mailbox(
                principal.account_id,
                mailbox_id,
                active_sync_audit(principal, "activesync.folder_delete", server_id),
            )
            .await
        {
            let status = folder_delete_error_status(&error.to_string());
            return folder_mutation_response(protocol_version, "FolderDelete", status, None, None);
        }

        let new_key = self
            .store_current_folder_hierarchy(principal.account_id, device_id)
            .await?;
        folder_mutation_response(protocol_version, "FolderDelete", "1", Some(&new_key), None)
    }

    pub(super) async fn handle_folder_update(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "FolderUpdate" {
            bail!("invalid FolderUpdate payload");
        }

        let Some(sync_key) = request
            .child("SyncKey")
            .map(|node| node.text_value().trim())
            .filter(|value| !value.is_empty())
        else {
            return folder_mutation_response(protocol_version, "FolderUpdate", "10", None, None);
        };
        let Some(server_id) = request
            .child("ServerId")
            .map(|node| node.text_value().trim())
            .filter(|value| !value.is_empty())
        else {
            return folder_mutation_response(protocol_version, "FolderUpdate", "10", None, None);
        };
        let Some(parent_id) = request
            .child("ParentId")
            .map(|node| node.text_value().trim())
            .filter(|value| !value.is_empty())
        else {
            return folder_mutation_response(protocol_version, "FolderUpdate", "10", None, None);
        };
        let Some(display_name) = request
            .child("DisplayName")
            .map(|node| node.text_value().trim())
            .filter(|value| !value.is_empty())
        else {
            return folder_mutation_response(protocol_version, "FolderUpdate", "10", None, None);
        };
        if MailboxNamePolicy::system_role_for_display_name(display_name).is_some() {
            return folder_mutation_response(protocol_version, "FolderUpdate", "10", None, None);
        }

        if self
            .load_requested_sync_state(
                principal.account_id,
                device_id,
                FOLDER_SYNC_COLLECTION_ID,
                sync_key,
            )
            .await?
            .is_none()
        {
            return folder_mutation_response(protocol_version, "FolderUpdate", "9", None, None);
        }

        let Some(folder) = self
            .owned_mail_folder(principal.account_id, server_id)
            .await?
        else {
            return folder_mutation_response(protocol_version, "FolderUpdate", "4", None, None);
        };
        if folder.folder_type != ActiveSyncFolderType::UserCreatedMail {
            return folder_mutation_response(protocol_version, "FolderUpdate", "2", None, None);
        }
        let Some(mailbox_id) = folder.mailbox_id else {
            return folder_mutation_response(protocol_version, "FolderUpdate", "4", None, None);
        };
        let parent_mailbox_id = if parent_id == ROOT_FOLDER_ID {
            None
        } else {
            let Some(parent) = self
                .resolve_collection(principal.account_id, parent_id)
                .await?
                .filter(|collection| {
                    collection.account_id == principal.account_id
                        && mail_collection(collection)
                        && collection.mailbox_id.is_some()
                })
            else {
                return folder_mutation_response(protocol_version, "FolderUpdate", "5", None, None);
            };
            parent.mailbox_id
        };

        if let Err(error) = self
            .store
            .update_jmap_mailbox(
                JmapMailboxUpdateInput {
                    account_id: principal.account_id,
                    mailbox_id,
                    name: Some(display_name.to_string()),
                    parent_id: Some(parent_mailbox_id),
                    sort_order: None,
                    is_subscribed: None,
                },
                active_sync_audit(principal, "activesync.folder_update", server_id),
            )
            .await
        {
            let status = folder_update_error_status(&error.to_string());
            return folder_mutation_response(protocol_version, "FolderUpdate", status, None, None);
        }

        let new_key = self
            .store_current_folder_hierarchy(principal.account_id, device_id)
            .await?;
        folder_mutation_response(protocol_version, "FolderUpdate", "1", Some(&new_key), None)
    }

    async fn store_current_folder_hierarchy(
        &self,
        account_id: Uuid,
        device_id: &str,
    ) -> Result<String> {
        let collections = self.folder_collections(account_id).await?;
        let snapshot = folder_hierarchy_snapshot(&collections);
        let new_key = Uuid::new_v4().to_string();
        self.store
            .store_activesync_sync_state(
                account_id,
                device_id,
                FOLDER_SYNC_COLLECTION_ID,
                &new_key,
                snapshot.to_string(),
            )
            .await?;
        Ok(new_key)
    }
}

fn folder_hierarchy_snapshot(collections: &[CollectionDefinition]) -> Value {
    snapshot_to_value(
        &collections
            .iter()
            .map(|collection| SnapshotEntry {
                server_id: collection.id.clone(),
                fingerprint: format!(
                    "{}:{}:{}:{}",
                    collection.class_name,
                    collection.parent_id.as_deref().unwrap_or(ROOT_FOLDER_ID),
                    collection.display_name,
                    collection.folder_type.as_str()
                ),
                data: json!({
                    "page": 7,
                    "name": "Folder",
                    "children": [],
                }),
            })
            .collect::<Vec<_>>(),
    )
}

fn push_folder_metadata(node: &mut WbxmlNode, collection: &CollectionDefinition) {
    node.push(WbxmlNode::with_text(7, "ServerId", &collection.id));
    node.push(WbxmlNode::with_text(
        7,
        "ParentId",
        collection.parent_id.as_deref().unwrap_or(ROOT_FOLDER_ID),
    ));
    node.push(WbxmlNode::with_text(
        7,
        "DisplayName",
        &collection.display_name,
    ));
    node.push(WbxmlNode::with_text(
        7,
        "Type",
        collection.folder_type.as_str(),
    ));
}

fn folder_mutation_response(
    protocol_version: &str,
    command: &str,
    status: &str,
    sync_key: Option<&str>,
    server_id: Option<&str>,
) -> Result<Response> {
    let mut response = WbxmlNode::new(7, command);
    response.push(WbxmlNode::with_text(7, "Status", status));
    if status == "1" {
        if let Some(sync_key) = sync_key {
            response.push(WbxmlNode::with_text(7, "SyncKey", sync_key));
        }
        if let Some(server_id) = server_id {
            response.push(WbxmlNode::with_text(7, "ServerId", server_id));
        }
    }
    wbxml_response(protocol_version, encode_wbxml(&response))
}

fn active_sync_audit(
    principal: &AuthenticatedPrincipal,
    action: &str,
    subject: &str,
) -> AuditEntryInput {
    AuditEntryInput {
        actor: principal.email.clone(),
        action: action.to_string(),
        subject: subject.to_string(),
    }
}

fn folder_create_error_status(error: &str) -> &'static str {
    if error.contains("already exists") {
        "2"
    } else if error.contains("parent") || error.contains("not found") {
        "5"
    } else if error.contains("ReservedName") || error.contains("system mailbox") {
        "10"
    } else {
        "6"
    }
}

fn folder_delete_error_status(error: &str) -> &'static str {
    if error.contains("system mailbox") {
        "3"
    } else if error.contains("not found") {
        "4"
    } else {
        "6"
    }
}

fn folder_update_error_status(error: &str) -> &'static str {
    if error.contains("system mailbox") || error.contains("already exists") {
        "2"
    } else if error.contains("not found") {
        "4"
    } else if error.contains("parent") {
        "5"
    } else if error.contains("ReservedName") {
        "10"
    } else {
        "6"
    }
}
