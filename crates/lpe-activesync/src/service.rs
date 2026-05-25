use anyhow::{anyhow, bail, Result};
use axum::{http::HeaderMap, response::Response};
use lpe_domain::MailboxNamePolicy;
use lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{
    calendar_attendee_labels, serialize_calendar_participants_metadata, ActiveSyncItemState,
    AuditEntryInput, CalendarParticipantMetadata, CalendarParticipantsMetadata,
    CanonicalChangeCategory, CanonicalChangeListener, JmapEmailFollowupUpdate,
    JmapMailboxCreateInput, JmapMailboxUpdateInput, SubmitMessageInput, SubmittedRecipientInput,
    UpsertClientContactInput, UpsertClientEventInput,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};
use uuid::Uuid;

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

const PING_SETTINGS_SYNC_KEY: &str = "current";
const PING_MIN_HEARTBEAT_SECONDS: u32 = 60;
const PING_MAX_HEARTBEAT_SECONDS: u32 = 3540;
const PING_MAX_FOLDERS: usize = 200;
const PING_FALLBACK_POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PingSettings {
    heartbeat_interval: u32,
    folders: Vec<PingFolder>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PingFolder {
    id: String,
    class_name: String,
}

enum PingResolution {
    Ready(Vec<(CollectionDefinition, StoredSyncState)>),
    MissingParameters,
    FolderSyncRequired,
}

use crate::{
    constants::{
        CALENDAR_CLASS, CONTACTS_CLASS, FOLDER_SYNC_COLLECTION_ID, MAIL_CLASS,
        PING_SETTINGS_COLLECTION_ID, ROOT_FOLDER_ID,
    },
    message::{
        default_sender, draft_input_from_application_data, field_text, merged_draft_input,
        parse_mime_message,
    },
    protocol::{ActiveSyncCommand, ActiveSyncFolderType, ActiveSyncStatus, BodyPreferenceType},
    response::{empty_response, is_message_rfc822, policy_key, sync_status_node, wbxml_response},
    snapshot::{
        calendar_application_data, collection_window_size, contact_application_data,
        diff_collection_states, diff_snapshots, drafts_collection, email_application_data,
        mail_collection, parse_collection_mailbox_id, require_collection_id,
        require_sync_collections, snapshot_to_value, BodyPreference,
    },
    store::ActiveSyncStore,
    types::{
        AuthenticatedPrincipal, CollectionDefinition, CollectionStateEntry, ParsedActiveSyncQuery,
        SnapshotChange, SnapshotEntry, StoredSyncState,
    },
    wbxml::{decode_wbxml, encode_wbxml, WbxmlNode},
};

#[cfg(test)]
use crate::types::ActiveSyncQuery;

#[derive(Clone)]
pub struct ActiveSyncService<S> {
    store: S,
    policy_mode: PolicyMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyMode {
    Permissive,
    Enforced,
}

impl<S> ActiveSyncService<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            policy_mode: PolicyMode::Permissive,
        }
    }

    pub fn with_policy_enforcement(store: S) -> Self {
        Self {
            store,
            policy_mode: PolicyMode::Enforced,
        }
    }

    pub fn from_env(store: S) -> Self {
        match std::env::var("LPE_ACTIVESYNC_PROVISIONING_MODE") {
            Ok(value) if value.trim().eq_ignore_ascii_case("enforced") => {
                Self::with_policy_enforcement(store)
            }
            _ => Self::new(store),
        }
    }
}

impl<S: ActiveSyncStore> ActiveSyncService<S> {
    async fn mailbox_accesses(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<Vec<lpe_storage::MailboxAccountAccess>> {
        self.store
            .fetch_accessible_mailbox_accounts(principal.account_id)
            .await
    }

    async fn mailbox_access_for_account(
        &self,
        principal: &AuthenticatedPrincipal,
        target_account_id: Uuid,
    ) -> Result<lpe_storage::MailboxAccountAccess> {
        self.mailbox_accesses(principal)
            .await?
            .into_iter()
            .find(|access| access.account_id == target_account_id)
            .ok_or_else(|| anyhow!("mailbox account is not accessible"))
    }

    async fn mailbox_access_for_from_address(
        &self,
        principal: &AuthenticatedPrincipal,
        from_address: Option<&str>,
    ) -> Result<lpe_storage::MailboxAccountAccess> {
        let normalized = from_address
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty());
        let accesses = self.mailbox_accesses(principal).await?;
        if let Some(from_address) = normalized {
            accesses
                .into_iter()
                .find(|access| access.email == from_address)
                .ok_or_else(|| anyhow!("from email is not accessible"))
        } else {
            accesses
                .into_iter()
                .find(|access| access.account_id == principal.account_id)
                .ok_or_else(|| anyhow!("primary mailbox account is not accessible"))
        }
    }

    #[cfg(test)]
    pub(crate) async fn handle_request(
        &self,
        query: ActiveSyncQuery,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        self.handle_parsed_request(
            ParsedActiveSyncQuery {
                query,
                ..Default::default()
            },
            headers,
            body,
        )
        .await
    }

    pub(crate) async fn handle_parsed_request(
        &self,
        parsed: ParsedActiveSyncQuery,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let ParsedActiveSyncQuery {
            query,
            protocol_version,
            _policy_key,
            ..
        } = parsed;
        let command = query
            .cmd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ActiveSyncCommand::from_name)
            .transpose()?
            .ok_or_else(|| anyhow!("missing ActiveSync command"))?;
        let device_id = query
            .device_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("missing DeviceId"))?;
        let protocol_version =
            protocol_version.unwrap_or_else(|| crate::auth::protocol_version(headers));
        crate::auth::ensure_supported_protocol_version(&protocol_version)?;
        let principal = self.authenticate(query.user.as_deref(), headers).await?;
        self.store
            .cleanup_expired_activesync_sync_cursors(principal.account_id, device_id)
            .await?;
        let device_type = query._device_type.as_deref().unwrap_or("unknown");
        let request_policy_key = _policy_key
            .map(|value| value.to_string())
            .or_else(|| header_policy_key(headers));

        if command != ActiveSyncCommand::Provision && command != ActiveSyncCommand::Ping {
            if self.policy_mode == PolicyMode::Enforced
                && !self
                    .policy_key_is_current(
                        principal.account_id,
                        device_id,
                        request_policy_key.as_deref(),
                    )
                    .await?
            {
                return policy_required_response(command, &protocol_version);
            }
            self.store
                .touch_activesync_device(principal.account_id, device_id)
                .await?;
        }

        match command {
            ActiveSyncCommand::Provision => {
                let request = decode_wbxml(body)?;
                self.handle_provision(
                    &principal,
                    device_id,
                    device_type,
                    &protocol_version,
                    &request,
                )
                .await
            }
            ActiveSyncCommand::FolderSync => {
                let request = decode_wbxml(body)?;
                self.handle_folder_sync(&principal, device_id, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::FolderCreate => {
                let request = decode_wbxml(body)?;
                self.handle_folder_create(&principal, device_id, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::FolderDelete => {
                let request = decode_wbxml(body)?;
                self.handle_folder_delete(&principal, device_id, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::FolderUpdate => {
                let request = decode_wbxml(body)?;
                self.handle_folder_update(&principal, device_id, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::GetItemEstimate => {
                let request = decode_wbxml(body)?;
                self.handle_get_item_estimate(&principal, device_id, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::Sync => {
                let request = decode_wbxml(body)?;
                self.handle_sync(&principal, device_id, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::ItemOperations => {
                let request = decode_wbxml(body)?;
                self.handle_item_operations(&principal, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::MoveItems => {
                let request = decode_wbxml(body)?;
                self.handle_move_items(&principal, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::Ping => {
                let request = if body.is_empty() {
                    WbxmlNode::new(13, "Ping")
                } else {
                    decode_wbxml(body)?
                };
                self.handle_ping(&principal, device_id, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::Search => {
                let request = decode_wbxml(body)?;
                self.handle_search(&principal, &protocol_version, &request)
                    .await
            }
            ActiveSyncCommand::SendMail => {
                self.handle_send_mail(&principal, &protocol_version, headers, body)
                    .await
            }
            ActiveSyncCommand::SmartReply => {
                let request = decode_wbxml(body)?;
                self.handle_smart_compose(
                    &principal,
                    &protocol_version,
                    &request,
                    ActiveSyncCommand::SmartReply,
                )
                .await
            }
            ActiveSyncCommand::SmartForward => {
                let request = decode_wbxml(body)?;
                self.handle_smart_compose(
                    &principal,
                    &protocol_version,
                    &request,
                    ActiveSyncCommand::SmartForward,
                )
                .await
            }
            other => {
                let known_unsupported_name =
                    ActiveSyncCommand::known_unsupported_name_for_str(other.as_str());
                tracing::warn!(
                    adapter = "activesync",
                    enum_name = "ActiveSyncCommand",
                    raw_value = other.as_str(),
                    known_unsupported = known_unsupported_name.is_some(),
                    known_unsupported_name = known_unsupported_name.unwrap_or(""),
                    "unsupported ActiveSync command reached service dispatcher"
                );
                bail!("unsupported ActiveSync command: {other}")
            }
        }
    }

    async fn authenticate(
        &self,
        hinted_user: Option<&str>,
        headers: &HeaderMap,
    ) -> Result<AccountPrincipal> {
        authenticate_account(&self.store, hinted_user, headers, "activesync").await
    }

    async fn handle_provision(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        device_type: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Provision" {
            return command_status_response(protocol_version, 14, "Provision", "2");
        }

        let requested_key = request
            .child("Policies")
            .and_then(|policies| policies.child("Policy"))
            .and_then(|policy| policy.child("PolicyKey"))
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let client_status = request
            .child("Policies")
            .and_then(|policies| policies.child("Policy"))
            .and_then(|policy| policy.child("Status"))
            .map(|node| node.text_value().trim().to_string());

        let current_policy_key = policy_key(principal.account_id, device_id);
        if client_status.as_deref() == Some("1") && requested_key == current_policy_key {
            self.store
                .acknowledge_activesync_device_policy(
                    principal.account_id,
                    device_id,
                    device_type,
                    &current_policy_key,
                )
                .await?;
        } else {
            self.store
                .store_activesync_device_pending_policy(
                    principal.account_id,
                    device_id,
                    device_type,
                    &current_policy_key,
                )
                .await?;
        }
        let mut response = WbxmlNode::new(14, "Provision");

        if request
            .child("DeviceInformation")
            .or_else(|| {
                request
                    .children
                    .iter()
                    .find(|child| child.page == 18 && child.name == "DeviceInformation")
            })
            .is_some()
        {
            let mut device_information = WbxmlNode::new(18, "DeviceInformation");
            device_information.push(WbxmlNode::with_text(
                18,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
            response.push(device_information);
        }

        response.push(WbxmlNode::with_text(
            14,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        let mut policies = WbxmlNode::new(14, "Policies");
        let mut policy = WbxmlNode::new(14, "Policy");
        policy.push(WbxmlNode::with_text(
            14,
            "PolicyType",
            "MS-EAS-Provisioning-WBXML",
        ));
        policy.push(WbxmlNode::with_text(
            14,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        policy.push(WbxmlNode::with_text(14, "PolicyKey", &current_policy_key));

        if client_status.as_deref() != Some("1") || requested_key != current_policy_key {
            let mut data = WbxmlNode::new(14, "Data");
            let mut document = WbxmlNode::new(14, "EASProvisionDoc");
            for (name, value) in [
                ("DevicePasswordEnabled", "0"),
                ("AlphanumericDevicePasswordRequired", "0"),
                ("AttachmentsEnabled", "1"),
                ("MinDevicePasswordLength", "0"),
                ("AllowSimpleDevicePassword", "1"),
                ("AllowStorageCard", "1"),
                ("AllowCamera", "1"),
                ("RequireDeviceEncryption", "0"),
                ("AllowWiFi", "1"),
                ("AllowTextMessaging", "1"),
                ("AllowPOPIMAPEmail", "1"),
                ("AllowBrowser", "1"),
                ("AllowConsumerEmail", "1"),
            ] {
                document.push(WbxmlNode::with_text(14, name, value));
            }
            data.push(document);
            policy.push(data);
        }

        policies.push(policy);
        response.push(policies);
        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    async fn policy_key_is_current(
        &self,
        account_id: Uuid,
        device_id: &str,
        request_policy_key: Option<&str>,
    ) -> Result<bool> {
        let Some(request_policy_key) = request_policy_key
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(false);
        };
        let Some(device) = self
            .store
            .fetch_activesync_device(account_id, device_id)
            .await?
        else {
            return Ok(false);
        };
        Ok(device.provision_status == "active"
            && device
                .policy_key
                .as_deref()
                .map(|policy_key| policy_key == request_policy_key)
                .unwrap_or(false))
    }

    async fn handle_folder_sync(
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

    async fn handle_folder_create(
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

    async fn handle_folder_delete(
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

    async fn handle_folder_update(
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

    async fn handle_get_item_estimate(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "GetItemEstimate" {
            bail!("invalid GetItemEstimate payload");
        }

        let mut response = WbxmlNode::new(6, "GetItemEstimate");

        let Some(collections) = request.child("Collections") else {
            response.push(WbxmlNode::with_text(6, "Status", "2"));
            return wbxml_response(protocol_version, encode_wbxml(&response));
        };

        response.push(WbxmlNode::with_text(
            6,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        for collection_request in collections.children_named("Collection") {
            response.push(
                self.get_item_estimate_response(principal, device_id, collection_request)
                    .await?,
            );
        }

        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    async fn get_item_estimate_response(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        collection_request: &WbxmlNode,
    ) -> Result<WbxmlNode> {
        let collection_id = collection_request
            .child("CollectionId")
            .map(|node| node.text_value().trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_default();
        let sync_key = collection_request
            .child("SyncKey")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();

        let mut response = WbxmlNode::new(6, "Response");
        let Some(collection) = self
            .resolve_collection(principal.account_id, &collection_id)
            .await?
        else {
            response.push(WbxmlNode::with_text(6, "Status", "2"));
            return Ok(response);
        };

        if sync_key.is_empty() || sync_key == "0" {
            response.push(WbxmlNode::with_text(6, "Status", "3"));
            return Ok(response);
        }

        let Some(sync_state) = self
            .load_requested_sync_state(principal.account_id, device_id, &collection.id, &sync_key)
            .await?
        else {
            response.push(WbxmlNode::with_text(6, "Status", "4"));
            return Ok(response);
        };

        let previous_state = decode_sync_state(&sync_state.snapshot_json)?;
        let current_state = self
            .collection_state(principal.account_id, &collection)
            .await?;
        let estimate = if previous_state.next_offset < previous_state.pending_changes.len() {
            previous_state.pending_changes.len() - previous_state.next_offset
        } else {
            diff_collection_states(&previous_state.collection_state, &current_state).len()
        };

        response.push(WbxmlNode::with_text(
            6,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        let mut response_collection = WbxmlNode::new(6, "Collection");
        response_collection.push(WbxmlNode::with_text(6, "CollectionId", collection.id));
        response_collection.push(WbxmlNode::with_text(6, "Estimate", estimate.to_string()));
        response.push(response_collection);
        Ok(response)
    }

    async fn handle_sync(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        let collection_nodes = match require_sync_collections(request) {
            Ok(collection_nodes) => collection_nodes,
            Err(_) => {
                let mut sync = WbxmlNode::new(0, "Sync");
                sync.push(WbxmlNode::with_text(0, "Status", "13"));
                return wbxml_response(protocol_version, encode_wbxml(&sync));
            }
        };
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections_node = WbxmlNode::new(0, "Collections");

        for collection_node in &collection_nodes {
            let response_collection = self
                .sync_collection(principal, device_id, request, collection_node)
                .await?;
            collections_node.push(response_collection);
        }

        sync.push(collections_node);
        wbxml_response(protocol_version, encode_wbxml(&sync))
    }

    async fn sync_collection(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        request: &WbxmlNode,
        collection_node: &WbxmlNode,
    ) -> Result<WbxmlNode> {
        let collection_id = match require_collection_id(collection_node) {
            Ok(collection_id) => collection_id,
            Err(_) => return Ok(sync_collection_status_node(None, "4")),
        };
        let sync_key = collection_node
            .child("SyncKey")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let window_size = collection_window_size(request, collection_node);
        let current_hierarchy_generation = self
            .current_hierarchy_generation(principal.account_id)
            .await?;

        let Some(collection) = self
            .resolve_collection(principal.account_id, &collection_id)
            .await?
        else {
            return Ok(sync_status_node(&collection_id, "8"));
        };

        let previous_state = if sync_key == "0" || sync_key.is_empty() {
            None
        } else {
            self.load_requested_sync_state(
                principal.account_id,
                device_id,
                &collection.id,
                &sync_key,
            )
            .await?
            .map(|state| decode_sync_state(&state.snapshot_json))
            .transpose()?
        };

        if !sync_key.is_empty() && sync_key != "0" && previous_state.is_none() {
            return Ok(sync_status_node(&collection.id, "3"));
        }

        if let Some(previous_state) = previous_state.as_ref() {
            if previous_state
                .hierarchy_generation
                .as_deref()
                .is_some_and(|generation| generation != current_hierarchy_generation)
                && !self
                    .device_hierarchy_is_current(
                        principal.account_id,
                        device_id,
                        &current_hierarchy_generation,
                    )
                    .await?
            {
                return Ok(sync_status_node(&collection.id, "12"));
            }
        }

        let body_preference = collection_body_preference(collection_node);
        if sync_collection_has_unsupported_command(collection_node, &collection) {
            return Ok(sync_status_node(&collection.id, "4"));
        }
        let client_responses = if drafts_collection(&collection) {
            self.apply_draft_sync_commands(principal, &collection, collection_node)
                .await?
        } else if mail_collection(&collection) {
            self.apply_mail_sync_commands(principal, &collection, collection_node)
                .await?
        } else if collection.class_name == CONTACTS_CLASS {
            self.apply_contact_sync_commands(principal, collection_node)
                .await?
        } else if collection.class_name == CALENDAR_CLASS {
            self.apply_calendar_sync_commands(principal, collection_node)
                .await?
        } else {
            Vec::new()
        };

        let final_state = self
            .collection_state(principal.account_id, &collection)
            .await?;
        let next_key = Uuid::new_v4().to_string();
        let mut response_collection = WbxmlNode::new(0, "Collection");
        response_collection.push(WbxmlNode::with_text(0, "Class", &collection.class_name));
        response_collection.push(WbxmlNode::with_text(0, "SyncKey", &next_key));
        response_collection.push(WbxmlNode::with_text(0, "CollectionId", &collection.id));
        response_collection.push(WbxmlNode::with_text(
            0,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));

        if !client_responses.is_empty() {
            let mut responses = WbxmlNode::new(0, "Responses");
            for client_response in client_responses {
                responses.push(client_response);
            }
            response_collection.push(responses);
        }

        if sync_key == "0" && !has_client_commands(collection_node) {
            let pending_changes = diff_collection_states(&[], &final_state);
            let stored_state = if pending_changes.is_empty() {
                completed_sync_state(final_state.clone(), Some(current_hierarchy_generation))
            } else {
                StoredSyncState {
                    hierarchy_generation: Some(current_hierarchy_generation),
                    collection_state: final_state,
                    pending_changes,
                    next_offset: 0,
                }
            };
            self.store_sync_state(
                principal.account_id,
                device_id,
                &collection.id,
                &next_key,
                &stored_state,
            )
            .await?;
            return Ok(response_collection);
        }

        let previous_state = previous_state.unwrap_or_default();

        let (commands, more_available, stored_state) =
            if previous_state.next_offset < previous_state.pending_changes.len() {
                let pending_page = pending_page(
                    &previous_state.pending_changes,
                    previous_state.next_offset,
                    window_size,
                );
                if !self
                    .pending_page_is_stable(
                        principal.account_id,
                        &collection,
                        &previous_state.collection_state,
                        &pending_page.0,
                    )
                    .await?
                {
                    return Ok(sync_status_node(&collection.id, "3"));
                }
                let commands = self
                    .build_commands(
                        principal.account_id,
                        &collection,
                        &pending_page.0,
                        &body_preference,
                    )
                    .await?;
                let more_available = pending_page.1 < previous_state.pending_changes.len();
                let stored_state = if more_available {
                    StoredSyncState {
                        hierarchy_generation: Some(current_hierarchy_generation.clone()),
                        next_offset: pending_page.1,
                        ..previous_state.clone()
                    }
                } else {
                    completed_sync_state(
                        previous_state.collection_state.clone(),
                        Some(current_hierarchy_generation),
                    )
                };
                (commands, more_available, stored_state)
            } else {
                let changed_items =
                    diff_collection_states(&previous_state.collection_state, &final_state);
                let pending_page = pending_page(&changed_items, 0, window_size);
                let commands = self
                    .build_commands(
                        principal.account_id,
                        &collection,
                        &pending_page.0,
                        &body_preference,
                    )
                    .await?;
                let more_available = pending_page.1 < changed_items.len();
                let stored_state = if more_available {
                    StoredSyncState {
                        hierarchy_generation: Some(current_hierarchy_generation),
                        collection_state: final_state,
                        pending_changes: changed_items,
                        next_offset: pending_page.1,
                    }
                } else {
                    completed_sync_state(final_state, Some(current_hierarchy_generation))
                };
                (commands, more_available, stored_state)
            };

        self.store_sync_state(
            principal.account_id,
            device_id,
            &collection.id,
            &next_key,
            &stored_state,
        )
        .await?;

        if !commands.children.is_empty() {
            response_collection.push(commands);
        }
        if more_available {
            response_collection.push(WbxmlNode::new(0, "MoreAvailable"));
        }

        Ok(response_collection)
    }

    async fn store_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        sync_key: &str,
        state: &StoredSyncState,
    ) -> Result<()> {
        self.store
            .store_activesync_sync_state(
                account_id,
                device_id,
                collection_id,
                sync_key,
                serde_json::to_string(state)?,
            )
            .await
    }

    async fn load_requested_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        requested_key: &str,
    ) -> Result<Option<lpe_storage::ActiveSyncSyncState>> {
        let requested_state = self
            .store
            .fetch_activesync_sync_state(account_id, device_id, collection_id, requested_key)
            .await?;
        let Some(requested_state) = requested_state else {
            return Ok(None);
        };

        let latest_state = self
            .store
            .fetch_latest_activesync_sync_state(account_id, device_id, collection_id)
            .await?;
        let Some(latest_state) = latest_state else {
            return Ok(None);
        };

        if latest_state.sync_key != requested_key {
            return Ok(None);
        }

        Ok(Some(requested_state))
    }

    async fn current_hierarchy_generation(&self, account_id: Uuid) -> Result<String> {
        Ok(hierarchy_generation(
            &self.folder_collections(account_id).await?,
        ))
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

    async fn device_hierarchy_is_current(
        &self,
        account_id: Uuid,
        device_id: &str,
        current_hierarchy_generation: &str,
    ) -> Result<bool> {
        let Some(state) = self
            .store
            .fetch_latest_activesync_sync_state(account_id, device_id, FOLDER_SYNC_COLLECTION_ID)
            .await?
        else {
            return Ok(false);
        };
        let snapshot: Value = serde_json::from_str(&state.snapshot_json)?;
        Ok(hierarchy_generation_from_snapshot(&snapshot) == current_hierarchy_generation)
    }

    async fn collection_state(
        &self,
        account_id: Uuid,
        collection: &CollectionDefinition,
    ) -> Result<Vec<CollectionStateEntry>> {
        let mut state = if mail_collection(collection) {
            let mailbox_id = parse_collection_mailbox_id(collection)?;
            self.fetch_all_mail_states(collection.account_id, mailbox_id)
                .await?
        } else if collection.class_name == CONTACTS_CLASS {
            self.store
                .fetch_activesync_contact_states(account_id)
                .await?
        } else if collection.class_name == CALENDAR_CLASS {
            self.store.fetch_activesync_event_states(account_id).await?
        } else {
            Vec::new()
        }
        .into_iter()
        .map(|entry| CollectionStateEntry {
            server_id: entry.id.to_string(),
            fingerprint: entry.fingerprint,
        })
        .collect::<Vec<_>>();

        state.sort_by(|left, right| left.server_id.cmp(&right.server_id));
        Ok(state)
    }

    async fn fetch_all_mail_states(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let mut state = Vec::new();
        let mut position = 0;
        let page_size = 512_u64;

        loop {
            let page = self
                .store
                .fetch_activesync_email_states(account_id, mailbox_id, position, page_size)
                .await?;
            let batch_len = page.len() as u64;
            if batch_len == 0 {
                break;
            }
            state.extend(page);
            if batch_len < page_size {
                break;
            }
            position += batch_len;
        }

        Ok(state)
    }

    async fn build_commands(
        &self,
        account_id: Uuid,
        collection: &CollectionDefinition,
        page_changes: &[SnapshotChange],
        body_preference: &BodyPreference,
    ) -> Result<WbxmlNode> {
        let mut commands = WbxmlNode::new(0, "Commands");
        let item_nodes = self
            .fetch_collection_nodes(account_id, collection, page_changes, body_preference)
            .await?;

        for change in page_changes {
            match change.kind.as_str() {
                "Add" | "Update" => {
                    if let Some(item) = item_nodes.get(&change.server_id) {
                        let mut node = WbxmlNode::new(
                            0,
                            if change.kind == "Add" {
                                "Add"
                            } else {
                                "Change"
                            },
                        );
                        node.push(WbxmlNode::with_text(0, "ServerId", &change.server_id));
                        node.push(item.clone());
                        commands.push(node);
                    }
                }
                "Delete" => {
                    let mut node = WbxmlNode::new(0, "Delete");
                    node.push(WbxmlNode::with_text(0, "ServerId", &change.server_id));
                    commands.push(node);
                }
                _ => {}
            }
        }

        Ok(commands)
    }

    async fn fetch_collection_nodes(
        &self,
        account_id: Uuid,
        collection: &CollectionDefinition,
        page_changes: &[SnapshotChange],
        body_preference: &BodyPreference,
    ) -> Result<HashMap<String, WbxmlNode>> {
        let ids = page_changes
            .iter()
            .filter(|change| change.kind == "Add" || change.kind == "Update")
            .map(|change| Uuid::parse_str(&change.server_id))
            .collect::<Result<Vec<_>, _>>()?;

        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut nodes = HashMap::new();
        if mail_collection(collection) {
            for email in self
                .store
                .fetch_jmap_emails(collection.account_id, &ids)
                .await?
            {
                let attachments = self
                    .store
                    .fetch_activesync_message_attachments(collection.account_id, email.id)
                    .await?;
                let mime_blob = if body_preference.body_type == BodyPreferenceType::Mime {
                    self.store
                        .fetch_jmap_message_blob(collection.account_id, email.id)
                        .await?
                } else {
                    None
                };
                nodes.insert(
                    email.id.to_string(),
                    email_application_data(
                        &email,
                        &attachments,
                        body_preference,
                        mime_blob.as_ref(),
                    )
                    .pipe(value_to_wbxml),
                );
            }
        } else if collection.class_name == CONTACTS_CLASS {
            for contact in self
                .store
                .fetch_client_contacts_by_ids(account_id, &ids)
                .await?
            {
                nodes.insert(
                    contact.id.to_string(),
                    contact_application_data(&contact).pipe(value_to_wbxml),
                );
            }
        } else if collection.class_name == CALENDAR_CLASS {
            for event in self
                .store
                .fetch_client_events_by_ids(account_id, &ids)
                .await?
            {
                nodes.insert(
                    event.id.to_string(),
                    calendar_application_data(&event).pipe(value_to_wbxml),
                );
            }
        }

        Ok(nodes)
    }

    async fn pending_page_is_stable(
        &self,
        account_id: Uuid,
        collection: &CollectionDefinition,
        target_state: &[CollectionStateEntry],
        page_changes: &[SnapshotChange],
    ) -> Result<bool> {
        let target_map = target_state
            .iter()
            .map(|entry| (entry.server_id.clone(), entry.fingerprint.clone()))
            .collect::<HashMap<_, _>>();
        let ids = page_changes
            .iter()
            .map(|change| Uuid::parse_str(&change.server_id))
            .collect::<Result<Vec<_>, _>>()?;
        let current_state = self
            .fetch_collection_states_by_ids(account_id, collection, &ids)
            .await?;
        let current_map = current_state
            .into_iter()
            .map(|entry| (entry.id.to_string(), entry.fingerprint))
            .collect::<HashMap<_, _>>();

        for change in page_changes {
            match change.kind.as_str() {
                "Add" | "Update" => {
                    let Some(expected) = target_map.get(&change.server_id) else {
                        return Ok(false);
                    };
                    if current_map.get(&change.server_id) != Some(expected) {
                        return Ok(false);
                    }
                }
                "Delete" => {
                    if current_map.contains_key(&change.server_id) {
                        return Ok(false);
                    }
                }
                _ => {}
            }
        }

        Ok(true)
    }

    async fn fetch_collection_states_by_ids(
        &self,
        account_id: Uuid,
        collection: &CollectionDefinition,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        if mail_collection(collection) {
            let mailbox_id = parse_collection_mailbox_id(collection)?;
            self.store
                .fetch_activesync_email_states_by_ids(collection.account_id, mailbox_id, ids)
                .await
        } else if collection.class_name == CONTACTS_CLASS {
            self.store
                .fetch_activesync_contact_states_by_ids(account_id, ids)
                .await
        } else if collection.class_name == CALENDAR_CLASS {
            self.store
                .fetch_activesync_event_states_by_ids(account_id, ids)
                .await
        } else {
            Ok(Vec::new())
        }
    }

    async fn apply_mail_sync_commands(
        &self,
        principal: &AuthenticatedPrincipal,
        collection: &CollectionDefinition,
        collection_node: &WbxmlNode,
    ) -> Result<Vec<WbxmlNode>> {
        let mut responses = Vec::new();
        let Some(commands) = collection_node.child("Commands") else {
            return Ok(responses);
        };
        let mailbox_id = parse_collection_mailbox_id(collection)?;
        let deletes_as_moves = collection_deletes_as_moves(collection_node);

        for command in &commands.children {
            match command.name.as_str() {
                "Change" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("mail change command is missing ServerId"))?;
                    let message_id = Uuid::parse_str(&server_id)?;
                    let application_data = command.child("ApplicationData");
                    let unread = application_data
                        .and_then(|application_data| application_data.child("Read"))
                        .map(|read| read.text_value().trim() != "1");
                    let followup_update = match application_data
                        .and_then(|application_data| application_data.child("Flag"))
                    {
                        Some(flag) => match mail_flag_update(flag) {
                            Ok(update) => Some(update),
                            Err(_) => {
                                let mut change = WbxmlNode::new(0, "Change");
                                change.push(WbxmlNode::with_text(0, "ServerId", server_id));
                                change.push(WbxmlNode::with_text(0, "Status", "6"));
                                responses.push(change);
                                continue;
                            }
                        },
                        None => None,
                    };
                    if unread.is_some() || followup_update.is_some() {
                        let mut update = followup_update.unwrap_or_default();
                        update.unread = unread;
                        self.store
                            .update_jmap_email_followup_flags(
                                collection.account_id,
                                message_id,
                                update,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "activesync-update-mail-flags".to_string(),
                                    subject: server_id.clone(),
                                },
                            )
                            .await?;
                    }
                    let mut change = WbxmlNode::new(0, "Change");
                    change.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    change.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(change);
                }
                "Delete" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("mail delete command is missing ServerId"))?;
                    let message_id = Uuid::parse_str(&server_id)?;
                    if deletes_as_moves {
                        if let Some(trash) = self.trash_collection(collection.account_id).await? {
                            if trash.mailbox_id != Some(mailbox_id) {
                                self.store
                                    .move_jmap_email_from_mailbox(
                                        collection.account_id,
                                        mailbox_id,
                                        message_id,
                                        parse_collection_mailbox_id(&trash)?,
                                        AuditEntryInput {
                                            actor: principal.email.clone(),
                                            action: "activesync-delete-move-to-trash".to_string(),
                                            subject: format!("message:{server_id}->{}", trash.id),
                                        },
                                    )
                                    .await?;
                            } else {
                                self.hard_delete_mail_command(
                                    principal, collection, mailbox_id, message_id, &server_id,
                                )
                                .await?;
                            }
                        } else {
                            self.hard_delete_mail_command(
                                principal, collection, mailbox_id, message_id, &server_id,
                            )
                            .await?;
                        }
                    } else {
                        self.hard_delete_mail_command(
                            principal, collection, mailbox_id, message_id, &server_id,
                        )
                        .await?;
                    }
                    let mut delete = WbxmlNode::new(0, "Delete");
                    delete.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    delete.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(delete);
                }
                _ => {}
            }
        }

        Ok(responses)
    }

    async fn hard_delete_mail_command(
        &self,
        principal: &AuthenticatedPrincipal,
        collection: &CollectionDefinition,
        mailbox_id: Uuid,
        message_id: Uuid,
        server_id: &str,
    ) -> Result<()> {
        self.store
            .delete_jmap_email_from_mailbox(
                collection.account_id,
                mailbox_id,
                message_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "activesync-delete-message".to_string(),
                    subject: server_id.to_string(),
                },
            )
            .await
    }

    async fn trash_collection(&self, account_id: Uuid) -> Result<Option<CollectionDefinition>> {
        Ok(self
            .folder_collections(account_id)
            .await?
            .into_iter()
            .find(|collection| {
                collection.class_name == MAIL_CLASS
                    && collection.folder_type == ActiveSyncFolderType::DeletedItems
            }))
    }

    async fn apply_draft_sync_commands(
        &self,
        principal: &AuthenticatedPrincipal,
        collection: &CollectionDefinition,
        collection_node: &WbxmlNode,
    ) -> Result<Vec<WbxmlNode>> {
        let mut responses = Vec::new();
        let Some(commands) = collection_node.child("Commands") else {
            return Ok(responses);
        };
        let mailbox_access = self
            .mailbox_access_for_account(principal, collection.account_id)
            .await?;

        for command in &commands.children {
            match command.name.as_str() {
                "Add" => {
                    let client_id = command
                        .child("ClientId")
                        .map(|node| node.text_value().trim().to_string())
                        .unwrap_or_else(|| Uuid::new_v4().to_string());
                    let application_data = command
                        .child("ApplicationData")
                        .ok_or_else(|| anyhow!("draft add command is missing ApplicationData"))?;
                    let input = draft_input_from_application_data(
                        principal,
                        &mailbox_access,
                        None,
                        application_data,
                        "activesync-sync-add",
                    );
                    let saved = self
                        .store
                        .save_draft_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "activesync-save-draft".to_string(),
                                subject: client_id.clone(),
                            },
                        )
                        .await?;
                    let mut add = WbxmlNode::new(0, "Add");
                    add.push(WbxmlNode::with_text(0, "ClientId", client_id));
                    add.push(WbxmlNode::with_text(
                        0,
                        "ServerId",
                        saved.message_id.to_string(),
                    ));
                    add.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(add);
                }
                "Change" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("draft change command is missing ServerId"))?;
                    let draft_id = Uuid::parse_str(&server_id)?;
                    let existing = self
                        .store
                        .fetch_jmap_draft(collection.account_id, draft_id)
                        .await?
                        .ok_or_else(|| anyhow!("draft not found"))?;
                    let application_data = command.child("ApplicationData").ok_or_else(|| {
                        anyhow!("draft change command is missing ApplicationData")
                    })?;
                    let input = merged_draft_input(
                        principal,
                        &mailbox_access,
                        draft_id,
                        &existing,
                        application_data,
                    );
                    self.store
                        .save_draft_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "activesync-update-draft".to_string(),
                                subject: server_id.clone(),
                            },
                        )
                        .await?;
                    let mut change = WbxmlNode::new(0, "Change");
                    change.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    change.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(change);
                }
                "Delete" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("draft delete command is missing ServerId"))?;
                    let draft_id = Uuid::parse_str(&server_id)?;
                    self.store
                        .delete_draft_message(
                            collection.account_id,
                            draft_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "activesync-delete-draft".to_string(),
                                subject: server_id.clone(),
                            },
                        )
                        .await?;
                    let mut delete = WbxmlNode::new(0, "Delete");
                    delete.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    delete.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(delete);
                }
                _ => {}
            }
        }

        Ok(responses)
    }

    async fn apply_contact_sync_commands(
        &self,
        principal: &AuthenticatedPrincipal,
        collection_node: &WbxmlNode,
    ) -> Result<Vec<WbxmlNode>> {
        let mut responses = Vec::new();
        let Some(commands) = collection_node.child("Commands") else {
            return Ok(responses);
        };

        for command in &commands.children {
            match command.name.as_str() {
                "Add" => {
                    let client_id = command
                        .child("ClientId")
                        .map(|node| node.text_value().trim().to_string())
                        .unwrap_or_else(|| Uuid::new_v4().to_string());
                    let application_data = command
                        .child("ApplicationData")
                        .ok_or_else(|| anyhow!("contact add command is missing ApplicationData"))?;
                    let created = self
                        .store
                        .upsert_client_contact(parse_contact_input(
                            principal.account_id,
                            None,
                            None,
                            application_data,
                        )?)
                        .await?;
                    let mut add = WbxmlNode::new(0, "Add");
                    add.push(WbxmlNode::with_text(0, "ClientId", client_id));
                    add.push(WbxmlNode::with_text(0, "ServerId", created.id.to_string()));
                    add.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(add);
                }
                "Change" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("contact change command is missing ServerId"))?;
                    let contact_id = Uuid::parse_str(&server_id)?;
                    let existing = self
                        .store
                        .fetch_client_contacts_by_ids(principal.account_id, &[contact_id])
                        .await?
                        .into_iter()
                        .next();
                    let application_data = command.child("ApplicationData").ok_or_else(|| {
                        anyhow!("contact change command is missing ApplicationData")
                    })?;
                    self.store
                        .upsert_client_contact(parse_contact_input(
                            principal.account_id,
                            Some(contact_id),
                            existing.as_ref(),
                            application_data,
                        )?)
                        .await?;
                    let mut change = WbxmlNode::new(0, "Change");
                    change.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    change.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(change);
                }
                "Delete" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("contact delete command is missing ServerId"))?;
                    self.store
                        .delete_client_contact(principal.account_id, Uuid::parse_str(&server_id)?)
                        .await?;
                    let mut delete = WbxmlNode::new(0, "Delete");
                    delete.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    delete.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(delete);
                }
                _ => {}
            }
        }

        Ok(responses)
    }

    async fn apply_calendar_sync_commands(
        &self,
        principal: &AuthenticatedPrincipal,
        collection_node: &WbxmlNode,
    ) -> Result<Vec<WbxmlNode>> {
        let mut responses = Vec::new();
        let Some(commands) = collection_node.child("Commands") else {
            return Ok(responses);
        };

        for command in &commands.children {
            match command.name.as_str() {
                "Add" => {
                    let client_id = command
                        .child("ClientId")
                        .map(|node| node.text_value().trim().to_string())
                        .unwrap_or_else(|| Uuid::new_v4().to_string());
                    let application_data = command.child("ApplicationData").ok_or_else(|| {
                        anyhow!("calendar add command is missing ApplicationData")
                    })?;
                    let created = self
                        .store
                        .upsert_client_event(parse_event_input(
                            principal.account_id,
                            None,
                            None,
                            application_data,
                        )?)
                        .await?;
                    let mut add = WbxmlNode::new(0, "Add");
                    add.push(WbxmlNode::with_text(0, "ClientId", client_id));
                    add.push(WbxmlNode::with_text(0, "ServerId", created.id.to_string()));
                    add.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(add);
                }
                "Change" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("calendar change command is missing ServerId"))?;
                    let event_id = Uuid::parse_str(&server_id)?;
                    let existing = self
                        .store
                        .fetch_client_events_by_ids(principal.account_id, &[event_id])
                        .await?
                        .into_iter()
                        .next();
                    let application_data = command.child("ApplicationData").ok_or_else(|| {
                        anyhow!("calendar change command is missing ApplicationData")
                    })?;
                    self.store
                        .upsert_client_event(parse_event_input(
                            principal.account_id,
                            Some(event_id),
                            existing.as_ref(),
                            application_data,
                        )?)
                        .await?;
                    let mut change = WbxmlNode::new(0, "Change");
                    change.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    change.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(change);
                }
                "Delete" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("calendar delete command is missing ServerId"))?;
                    self.store
                        .delete_client_event(principal.account_id, Uuid::parse_str(&server_id)?)
                        .await?;
                    let mut delete = WbxmlNode::new(0, "Delete");
                    delete.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    delete.push(WbxmlNode::with_text(
                        0,
                        "Status",
                        ActiveSyncStatus::Success.as_str(),
                    ));
                    responses.push(delete);
                }
                _ => {}
            }
        }

        Ok(responses)
    }

    async fn handle_send_mail(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let mime_payload = if is_message_rfc822(headers) {
            body.to_vec()
        } else {
            let request = decode_wbxml(body)?;
            if request.name != "SendMail" {
                return command_status_response(protocol_version, 21, "SendMail", "103");
            }
            match request.child("Mime") {
                Some(node) => node.text_value().as_bytes().to_vec(),
                None => return command_status_response(protocol_version, 21, "SendMail", "103"),
            }
        };

        let parsed = match parse_mime_message(&mime_payload) {
            Ok(parsed) => parsed,
            Err(_) => return command_status_response(protocol_version, 21, "SendMail", "107"),
        };
        if validate_mime_attachments(&mime_payload).is_err() {
            return command_status_response(protocol_version, 21, "SendMail", "107");
        }
        let mailbox_access = self
            .mailbox_access_for_from_address(
                principal,
                parsed.from.as_ref().map(|mailbox| mailbox.address.as_str()),
            )
            .await;
        let Ok(mailbox_access) = mailbox_access else {
            return command_status_response(protocol_version, 21, "SendMail", "166");
        };
        let from_display = parsed
            .from
            .as_ref()
            .and_then(|mailbox| mailbox.display_name.clone())
            .or_else(|| Some(mailbox_access.display_name.clone()));
        let from_address = parsed
            .from
            .map(|mailbox| mailbox.address)
            .unwrap_or_else(|| mailbox_access.email.clone());
        let (sender_display, sender_address) = match parsed.sender {
            Some(sender) => (sender.display_name, Some(sender.address)),
            None => default_sender(&mailbox_access, principal, None, None),
        };
        let submitted = self
            .store
            .submit_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: mailbox_access.account_id,
                    submitted_by_account_id: principal.account_id,
                    source: "activesync-sendmail".to_string(),
                    from_display,
                    from_address,
                    sender_display,
                    sender_address,
                    to: parsed.to,
                    cc: parsed.cc,
                    bcc: parsed.bcc,
                    subject: parsed.subject,
                    body_text: parsed.body_text,
                    body_html_sanitized: None,
                    internet_message_id: parsed.internet_message_id,
                    mime_blob_ref: Some(format!("activesync-mime:{}", Uuid::new_v4())),
                    size_octets: mime_payload.len() as i64,
                    unread: Some(false),
                    flagged: Some(false),
                    attachments: parsed.attachments,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "activesync-sendmail".to_string(),
                    subject: "native client message submission".to_string(),
                },
            )
            .await;
        if submitted.is_err() {
            return command_status_response(protocol_version, 21, "SendMail", "120");
        }

        if is_message_rfc822(headers) {
            Ok(empty_response())
        } else {
            wbxml_response(protocol_version, Vec::new())
        }
    }

    async fn handle_item_operations(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "ItemOperations" {
            return command_status_response(protocol_version, 20, "ItemOperations", "2");
        }

        let mut root = WbxmlNode::new(20, "ItemOperations");
        let mut response = WbxmlNode::new(20, "Response");
        let mut unsupported_child = false;

        for child in &request.children {
            if child.name == "Fetch" {
                response.push(self.handle_item_operations_fetch(principal, child).await?);
            } else {
                unsupported_child = true;
            }
        }

        if unsupported_child || response.children.is_empty() {
            root.push(WbxmlNode::with_text(20, "Status", "2"));
        } else {
            root.push(WbxmlNode::with_text(
                20,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
        }

        if !response.children.is_empty() {
            root.push(response);
        }

        wbxml_response(protocol_version, encode_wbxml(&root))
    }

    async fn handle_item_operations_fetch(
        &self,
        principal: &AuthenticatedPrincipal,
        fetch: &WbxmlNode,
    ) -> Result<WbxmlNode> {
        let mut node = WbxmlNode::new(20, "Fetch");
        if let Some(file_reference) = fetch
            .child("FileReference")
            .map(|value| value.text_value().trim())
            .filter(|value| !value.is_empty())
        {
            let mut attachment = None;
            for access in self.mailbox_accesses(principal).await? {
                attachment = self
                    .store
                    .fetch_activesync_attachment_content(access.account_id, file_reference)
                    .await?;
                if attachment.is_some() {
                    break;
                }
            }
            if let Some(attachment) = attachment {
                node.push(WbxmlNode::with_text(
                    20,
                    "Status",
                    ActiveSyncStatus::Success.as_str(),
                ));
                node.push(WbxmlNode::with_text(
                    17,
                    "FileReference",
                    &attachment.file_reference,
                ));
                let mut properties = WbxmlNode::new(20, "Properties");
                properties.push(WbxmlNode::with_text(
                    17,
                    "ContentType",
                    &attachment.media_type,
                ));
                properties.push(WbxmlNode::with_opaque(20, "Data", attachment.blob_bytes));
                node.push(properties);
            } else {
                node.push(WbxmlNode::with_text(20, "Status", "15"));
            }
            return Ok(node);
        }

        let Some(server_id) = fetch
            .child("ServerId")
            .map(|value| value.text_value().trim())
        else {
            node.push(WbxmlNode::with_text(20, "Status", "15"));
            return Ok(node);
        };
        let message_id = match Uuid::parse_str(server_id) {
            Ok(message_id) => message_id,
            Err(_) => {
                node.push(WbxmlNode::with_text(20, "Status", "6"));
                return Ok(node);
            }
        };
        let mut resolved = None;
        if let Some(collection_id) = fetch
            .child("CollectionId")
            .map(|node| node.text_value().trim().to_string())
            .filter(|value| !value.is_empty())
        {
            let account_id = self
                .resolve_collection(principal.account_id, &collection_id)
                .await?
                .map(|collection| collection.account_id)
                .ok_or_else(|| anyhow!("collection not found"));
            let Ok(account_id) = account_id else {
                node.push(WbxmlNode::with_text(20, "Status", "6"));
                return Ok(node);
            };
            if let Some(email) = self
                .store
                .fetch_jmap_emails(account_id, &[message_id])
                .await?
                .into_iter()
                .next()
            {
                resolved = Some((account_id, email));
            }
        } else {
            for access in self.mailbox_accesses(principal).await? {
                if let Some(email) = self
                    .store
                    .fetch_jmap_emails(access.account_id, &[message_id])
                    .await?
                    .into_iter()
                    .next()
                {
                    resolved = Some((access.account_id, email));
                    break;
                }
            }
        }
        if let Some((account_id, email)) = resolved {
            let attachments = self
                .store
                .fetch_activesync_message_attachments(account_id, email.id)
                .await?;
            let body_preference = fetch_body_preference(fetch);
            let mime_blob = if body_preference.body_type == BodyPreferenceType::Mime {
                self.store
                    .fetch_jmap_message_blob(account_id, email.id)
                    .await?
            } else {
                None
            };
            node.push(WbxmlNode::with_text(
                20,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
            if let Some(collection_id) = fetch.child("CollectionId") {
                node.push(WbxmlNode::with_text(
                    0,
                    "CollectionId",
                    collection_id.text_value(),
                ));
            }
            node.push(WbxmlNode::with_text(0, "ServerId", email.id.to_string()));
            let mut properties = WbxmlNode::new(20, "Properties");
            properties.push(
                email_application_data(&email, &attachments, &body_preference, mime_blob.as_ref())
                    .pipe(value_to_wbxml),
            );
            node.push(properties);
        } else {
            node.push(WbxmlNode::with_text(20, "Status", "6"));
        }

        Ok(node)
    }

    async fn handle_move_items(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "MoveItems" {
            return command_status_response(protocol_version, 5, "MoveItems", "5");
        }

        let mut root = WbxmlNode::new(5, "MoveItems");
        let mut unsupported_child = false;
        for child in &request.children {
            if child.name == "Move" {
                root.push(self.handle_move_item(principal, child).await?);
            } else {
                unsupported_child = true;
            }
        }
        if unsupported_child || root.children.is_empty() {
            root.push(WbxmlNode::with_text(5, "Status", "5"));
        }
        wbxml_response(protocol_version, encode_wbxml(&root))
    }

    async fn handle_move_item(
        &self,
        principal: &AuthenticatedPrincipal,
        move_node: &WbxmlNode,
    ) -> Result<WbxmlNode> {
        let src_msg_id = move_node
            .child("SrcMsgId")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let src_fld_id = move_node
            .child("SrcFldId")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let dst_fld_id = move_node
            .child("DstFldId")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let mut response = WbxmlNode::new(5, "Response");
        if !src_msg_id.is_empty() {
            response.push(WbxmlNode::with_text(5, "SrcMsgId", &src_msg_id));
        }

        let source = self
            .resolve_collection(principal.account_id, &src_fld_id)
            .await?;
        let target = self
            .resolve_collection(principal.account_id, &dst_fld_id)
            .await?;
        let Some(source) = source else {
            response.push(WbxmlNode::with_text(
                5,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
            return Ok(response);
        };
        let Some(target) = target else {
            response.push(WbxmlNode::with_text(5, "Status", "2"));
            return Ok(response);
        };
        if source.id == target.id {
            response.push(WbxmlNode::with_text(5, "Status", "4"));
            return Ok(response);
        }
        if !mail_collection(&source)
            || !mail_collection(&target)
            || source.account_id != target.account_id
        {
            response.push(WbxmlNode::with_text(
                5,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
            return Ok(response);
        }

        let message_id = match Uuid::parse_str(&src_msg_id) {
            Ok(message_id) => message_id,
            Err(_) => {
                response.push(WbxmlNode::with_text(
                    5,
                    "Status",
                    ActiveSyncStatus::Success.as_str(),
                ));
                return Ok(response);
            }
        };
        let moved = self
            .store
            .move_jmap_email_from_mailbox(
                source.account_id,
                parse_collection_mailbox_id(&source)?,
                message_id,
                parse_collection_mailbox_id(&target)?,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "activesync-move-message".to_string(),
                    subject: format!("message:{src_msg_id}->{dst_fld_id}"),
                },
            )
            .await;

        match moved {
            Ok(email) => {
                response.push(WbxmlNode::with_text(5, "Status", "3"));
                response.push(WbxmlNode::with_text(5, "DstMsgId", email.id.to_string()));
            }
            Err(_) => response.push(WbxmlNode::with_text(
                5,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            )),
        }
        Ok(response)
    }

    async fn handle_ping(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Ping" {
            bail!("invalid Ping payload");
        }

        let cached_settings = self
            .load_ping_settings(principal.account_id, device_id)
            .await?;
        let Some(settings) = self.ping_settings_from_request(request, cached_settings.as_ref())
        else {
            return self.ping_status_response(protocol_version, "3", None, None, &[]);
        };

        if settings.heartbeat_interval < PING_MIN_HEARTBEAT_SECONDS {
            return self.ping_status_response(
                protocol_version,
                "5",
                Some(PING_MIN_HEARTBEAT_SECONDS),
                None,
                &[],
            );
        }
        if settings.heartbeat_interval > PING_MAX_HEARTBEAT_SECONDS {
            return self.ping_status_response(
                protocol_version,
                "5",
                Some(PING_MAX_HEARTBEAT_SECONDS),
                None,
                &[],
            );
        }
        if settings.folders.len() > PING_MAX_FOLDERS {
            return self.ping_status_response(
                protocol_version,
                "6",
                None,
                Some(PING_MAX_FOLDERS),
                &[],
            );
        }

        let mut change_listener = self
            .store
            .create_canonical_change_listener(principal.account_id)
            .await?;
        let collections = self.folder_collections(principal.account_id).await?;
        let monitored = match self
            .resolve_ping_collections(principal, device_id, &collections, &settings.folders)
            .await?
        {
            PingResolution::Ready(monitored) => monitored,
            PingResolution::MissingParameters => {
                return self.ping_status_response(protocol_version, "3", None, None, &[]);
            }
            PingResolution::FolderSyncRequired => {
                return self.ping_status_response(protocol_version, "7", None, None, &[]);
            }
        };
        self.store_ping_settings(principal.account_id, device_id, &settings)
            .await?;

        let categories = ping_change_categories(&monitored);
        let deadline = ping_deadline(settings.heartbeat_interval);
        let changed = loop {
            if self
                .ping_requires_folder_sync(principal.account_id, device_id, &monitored)
                .await?
            {
                return self.ping_status_response(protocol_version, "7", None, None, &[]);
            };

            let changed = self
                .changed_ping_collections(principal.account_id, &monitored)
                .await?;
            if !changed.is_empty() {
                break changed;
            }

            let now = Instant::now();
            if now >= deadline {
                break Vec::new();
            }
            self.wait_for_ping_change(&mut change_listener, &categories, deadline - now)
                .await?;
        };

        self.ping_status_response(
            protocol_version,
            if changed.is_empty() { "1" } else { "2" },
            None,
            None,
            &changed,
        )
    }

    async fn ping_requires_folder_sync(
        &self,
        account_id: Uuid,
        device_id: &str,
        monitored: &[(CollectionDefinition, StoredSyncState)],
    ) -> Result<bool> {
        let current_hierarchy_generation = self.current_hierarchy_generation(account_id).await?;
        if monitored.iter().any(|(_, previous_state)| {
            previous_state
                .hierarchy_generation
                .as_deref()
                .is_some_and(|generation| generation != current_hierarchy_generation)
        }) {
            return Ok(!self
                .device_hierarchy_is_current(account_id, device_id, &current_hierarchy_generation)
                .await?);
        }

        Ok(false)
    }

    async fn changed_ping_collections(
        &self,
        account_id: Uuid,
        monitored: &[(CollectionDefinition, StoredSyncState)],
    ) -> Result<Vec<String>> {
        let mut changed = Vec::new();
        for (collection, previous_state) in monitored {
            let current_state = self.collection_state(account_id, collection).await?;
            if diff_collection_states(&previous_state.collection_state, &current_state)
                .iter()
                .any(|change| change.kind == "Add")
            {
                changed.push(collection.id.clone());
            }
        }
        Ok(changed)
    }

    async fn wait_for_ping_change(
        &self,
        change_listener: &mut Option<CanonicalChangeListener>,
        categories: &[CanonicalChangeCategory],
        remaining: Duration,
    ) -> Result<()> {
        if remaining.is_zero() {
            return Ok(());
        }

        if let Some(listener) = change_listener {
            if let Ok(result) = timeout(remaining, listener.wait_for_change(categories)).await {
                result?;
            }
            return Ok(());
        }

        sleep(remaining.min(PING_FALLBACK_POLL_INTERVAL)).await;
        Ok(())
    }

    fn ping_status_response(
        &self,
        protocol_version: &str,
        status: &str,
        heartbeat_interval: Option<u32>,
        max_folders: Option<usize>,
        changed: &[String],
    ) -> Result<Response> {
        let mut response = WbxmlNode::new(13, "Ping");
        response.push(WbxmlNode::with_text(13, "Status", status));
        if let Some(heartbeat_interval) = heartbeat_interval {
            response.push(WbxmlNode::with_text(
                13,
                "HeartbeatInterval",
                heartbeat_interval.to_string(),
            ));
        }
        if let Some(max_folders) = max_folders {
            response.push(WbxmlNode::with_text(
                13,
                "MaxFolders",
                max_folders.to_string(),
            ));
        }
        if !changed.is_empty() {
            let mut folders_node = WbxmlNode::new(13, "Folders");
            for collection_id in changed {
                folders_node.push(WbxmlNode::with_text(13, "Folder", collection_id));
            }
            response.push(folders_node);
        }

        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    fn ping_settings_from_request(
        &self,
        request: &WbxmlNode,
        cached: Option<&PingSettings>,
    ) -> Option<PingSettings> {
        let heartbeat_interval = match request.child("HeartbeatInterval") {
            Some(node) => node.text_value().trim().parse::<u32>().ok()?,
            None => cached?.heartbeat_interval,
        };

        let folders = match request.child("Folders") {
            Some(folders) => {
                let mut parsed = Vec::new();
                for folder in folders.children_named("Folder") {
                    let id = folder
                        .child("Id")
                        .map(|node| node.text_value().trim().to_string())
                        .filter(|value| !value.is_empty())?;
                    let class_name = folder
                        .child("Class")
                        .map(|node| node.text_value().trim().to_string())
                        .filter(|value| !value.is_empty())?;
                    parsed.push(PingFolder { id, class_name });
                }
                if parsed.is_empty() {
                    return None;
                }
                parsed
            }
            None => cached?.folders.clone(),
        };

        Some(PingSettings {
            heartbeat_interval,
            folders,
        })
    }

    async fn load_ping_settings(
        &self,
        account_id: Uuid,
        device_id: &str,
    ) -> Result<Option<PingSettings>> {
        self.store
            .fetch_latest_activesync_sync_state(account_id, device_id, PING_SETTINGS_COLLECTION_ID)
            .await?
            .map(|state| serde_json::from_str(&state.snapshot_json).map_err(Into::into))
            .transpose()
    }

    async fn store_ping_settings(
        &self,
        account_id: Uuid,
        device_id: &str,
        settings: &PingSettings,
    ) -> Result<()> {
        self.store
            .store_activesync_sync_state(
                account_id,
                device_id,
                PING_SETTINGS_COLLECTION_ID,
                PING_SETTINGS_SYNC_KEY,
                serde_json::to_string(settings)?,
            )
            .await
    }

    async fn resolve_ping_collections(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        collections: &[CollectionDefinition],
        folders: &[PingFolder],
    ) -> Result<PingResolution> {
        let mut monitored = Vec::new();
        for folder in folders {
            let Some(collection) = collections.iter().find(|entry| entry.id == folder.id) else {
                return Ok(PingResolution::FolderSyncRequired);
            };
            if collection.class_name != folder.class_name {
                return Ok(PingResolution::FolderSyncRequired);
            }
            let Some(state) = self
                .store
                .fetch_latest_activesync_sync_state(principal.account_id, device_id, &collection.id)
                .await?
            else {
                return Ok(PingResolution::MissingParameters);
            };
            monitored.push((collection.clone(), decode_sync_state(&state.snapshot_json)?));
        }

        Ok(PingResolution::Ready(monitored))
    }

    async fn handle_search(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Search" {
            return search_status_response(protocol_version, "3", None);
        }

        let Some(store) = request.child("Store") else {
            return search_status_response(protocol_version, "3", None);
        };
        let query_text = search_query_text(store);
        let range = parse_range(
            store
                .child("Options")
                .and_then(|options| options.child("Range"))
                .or_else(|| store.child("Range"))
                .map(|node| node.text_value()),
        );
        let (start, end) = match range {
            Ok(range) => range,
            Err(_) => return search_status_response(protocol_version, "1", Some("2")),
        };
        let limit = end.saturating_sub(start) + 1;
        let query = self
            .store
            .query_jmap_email_ids(
                principal.account_id,
                None,
                query_text.as_deref(),
                start,
                limit,
            )
            .await?;
        let emails = self
            .store
            .fetch_jmap_emails(principal.account_id, &query.ids)
            .await?;

        let mut response = WbxmlNode::new(15, "Search");
        response.push(WbxmlNode::with_text(
            15,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        let mut response_store = WbxmlNode::new(15, "Store");
        response_store.push(WbxmlNode::with_text(
            15,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        response_store.push(WbxmlNode::with_text(15, "Name", "Mailbox"));
        response_store.push(WbxmlNode::with_text(15, "Total", query.total.to_string()));
        if !emails.is_empty() {
            response_store.push(WbxmlNode::with_text(
                15,
                "Range",
                format!("{}-{}", start, start + emails.len() as u64 - 1),
            ));
        }
        for email in emails {
            let attachments = self
                .store
                .fetch_activesync_message_attachments(principal.account_id, email.id)
                .await?;
            let mut result = WbxmlNode::new(15, "Result");
            result.push(WbxmlNode::with_text(15, "LongId", email.id.to_string()));
            let mut properties = WbxmlNode::new(15, "Properties");
            properties.push(WbxmlNode::with_text(
                0,
                "CollectionId",
                email.mailbox_id.to_string(),
            ));
            properties.push(WbxmlNode::with_text(0, "ServerId", email.id.to_string()));
            properties.push(
                email_application_data(&email, &attachments, &BodyPreference::default(), None)
                    .pipe(value_to_wbxml),
            );
            properties.push(WbxmlNode::with_text(
                17,
                "Preview",
                trim_preview(&email.body_text),
            ));
            result.push(properties);
            response_store.push(result);
        }
        let mut response_node = WbxmlNode::new(15, "Response");
        response_node.push(response_store);
        response.push(response_node);

        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    async fn handle_smart_compose(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        request: &WbxmlNode,
        command: ActiveSyncCommand,
    ) -> Result<Response> {
        let command_name = command.as_str();
        if request.name != command_name {
            return command_status_response(protocol_version, 21, command_name, "103");
        }

        let source = self.resolve_source_message(principal, request).await;
        let Ok((source_mailbox_access, source_message)) = source else {
            return command_status_response(protocol_version, 21, command_name, "150");
        };
        let mime_payload = request
            .child("Mime")
            .map(|node| node.text_value().as_bytes().to_vec())
            .ok_or_else(|| anyhow!("{command_name} is missing MIME content"));
        let Ok(mime_payload) = mime_payload else {
            return command_status_response(protocol_version, 21, command_name, "103");
        };
        if validate_mime_attachments(&mime_payload).is_err() {
            return command_status_response(protocol_version, 21, command_name, "107");
        }
        let parsed = match parse_mime_message(&mime_payload) {
            Ok(parsed) => parsed,
            Err(_) => return command_status_response(protocol_version, 21, command_name, "107"),
        };
        let mailbox_access = match self
            .mailbox_access_for_from_address(
                principal,
                parsed.from.as_ref().map(|mailbox| mailbox.address.as_str()),
            )
            .await
        {
            Ok(access) => access,
            Err(_) => source_mailbox_access.clone(),
        };

        let (to, cc) =
            if parsed.to.is_empty() && parsed.cc.is_empty() && command_name == "SmartReply" {
                (
                    reply_recipients(principal.email.as_str(), &source_message),
                    Vec::new(),
                )
            } else {
                (parsed.to, parsed.cc)
            };
        let mut attachments = parsed.attachments;
        if command_name == "SmartForward" {
            attachments.extend(
                self.load_message_attachment_uploads(
                    source_mailbox_access.account_id,
                    source_message.id,
                )
                .await?,
            );
        }

        let subject = if parsed.subject.trim().is_empty() {
            default_reply_subject(command_name, &source_message.subject)
        } else {
            parsed.subject
        };
        let body_text =
            merge_smart_body(command_name, &parsed.body_text, &source_message.body_text);
        let (sender_display, sender_address) = match parsed.sender {
            Some(sender) => (sender.display_name, Some(sender.address)),
            None => default_sender(&mailbox_access, principal, None, None),
        };
        let submitted = self
            .store
            .submit_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: mailbox_access.account_id,
                    submitted_by_account_id: principal.account_id,
                    source: format!("activesync-{}", command_name.to_ascii_lowercase()),
                    from_display: parsed
                        .from
                        .as_ref()
                        .and_then(|mailbox| mailbox.display_name.clone())
                        .or_else(|| Some(mailbox_access.display_name.clone())),
                    from_address: parsed
                        .from
                        .map(|mailbox| mailbox.address)
                        .unwrap_or_else(|| mailbox_access.email.clone()),
                    sender_display,
                    sender_address,
                    to,
                    cc,
                    bcc: parsed.bcc,
                    subject,
                    body_text,
                    body_html_sanitized: None,
                    internet_message_id: parsed.internet_message_id,
                    mime_blob_ref: Some(format!("activesync-mime:{}", Uuid::new_v4())),
                    size_octets: mime_payload.len() as i64,
                    unread: Some(false),
                    flagged: Some(false),
                    attachments,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: format!("activesync-{}", command_name.to_ascii_lowercase()),
                    subject: source_message.id.to_string(),
                },
            )
            .await;
        if submitted.is_err() {
            return command_status_response(protocol_version, 21, command_name, "120");
        }

        let mut response = WbxmlNode::new(21, command_name);
        response.push(WbxmlNode::with_text(
            21,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    async fn resolve_source_message(
        &self,
        principal: &AuthenticatedPrincipal,
        request: &WbxmlNode,
    ) -> Result<(lpe_storage::MailboxAccountAccess, lpe_storage::JmapEmail)> {
        let source = request
            .child("Source")
            .ok_or_else(|| anyhow!("compose command is missing Source"))?;
        let message_id = source
            .child("ItemId")
            .or_else(|| source.child("LongId"))
            .map(|node| node.text_value().trim().to_string())
            .ok_or_else(|| anyhow!("compose command is missing ItemId"))?;
        let message_id = Uuid::parse_str(&message_id)?;
        for access in self.mailbox_accesses(principal).await? {
            if let Some(email) = self
                .store
                .fetch_jmap_emails(access.account_id, &[message_id])
                .await?
                .into_iter()
                .next()
            {
                return Ok((access, email));
            }
        }
        bail!("source message not found")
    }

    async fn load_message_attachment_uploads(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Vec<lpe_storage::AttachmentUploadInput>> {
        let attachments = self
            .store
            .fetch_activesync_message_attachments(account_id, message_id)
            .await?;
        let mut uploads = Vec::with_capacity(attachments.len());
        for attachment in attachments {
            let Some(content) = self
                .store
                .fetch_activesync_attachment_content(account_id, &attachment.file_reference)
                .await?
            else {
                continue;
            };
            uploads.push(lpe_storage::AttachmentUploadInput {
                file_name: content.file_name,
                media_type: content.media_type,
                disposition: Some("attachment".to_string()),
                content_id: None,
                blob_bytes: content.blob_bytes,
            });
        }
        Ok(uploads)
    }

    async fn folder_collections(&self, account_id: Uuid) -> Result<Vec<CollectionDefinition>> {
        let mut collections = Vec::new();
        for access in self
            .store
            .fetch_accessible_mailbox_accounts(account_id)
            .await?
        {
            let mailboxes = self.store.fetch_jmap_mailboxes(access.account_id).await?;
            let mailbox_ids = mailboxes
                .iter()
                .map(|mailbox| mailbox.id)
                .collect::<std::collections::HashSet<_>>();
            for mailbox in mailboxes {
                let parent_id = mailbox
                    .parent_id
                    .filter(|parent_id| mailbox_ids.contains(parent_id))
                    .map(|parent_id| parent_id.to_string());
                let display_name = if access.is_owned || parent_id.is_some() {
                    mailbox.name.clone()
                } else {
                    format!("{} / {}", access.email, mailbox.name)
                };
                collections.push(CollectionDefinition {
                    id: mailbox.id.to_string(),
                    parent_id,
                    account_id: access.account_id,
                    class_name: MAIL_CLASS.to_string(),
                    display_name,
                    folder_type: ActiveSyncFolderType::from_mailbox_role(&mailbox.role),
                    mailbox_id: Some(mailbox.id),
                });
            }
        }

        collections.push(CollectionDefinition {
            id: "contacts".to_string(),
            parent_id: None,
            account_id,
            class_name: CONTACTS_CLASS.to_string(),
            display_name: "Contacts".to_string(),
            folder_type: ActiveSyncFolderType::Contacts,
            mailbox_id: None,
        });
        collections.push(CollectionDefinition {
            id: "calendar".to_string(),
            parent_id: None,
            account_id,
            class_name: CALENDAR_CLASS.to_string(),
            display_name: "Calendar".to_string(),
            folder_type: ActiveSyncFolderType::Calendar,
            mailbox_id: None,
        });
        Ok(collections)
    }

    async fn resolve_collection(
        &self,
        account_id: Uuid,
        collection_id: &str,
    ) -> Result<Option<CollectionDefinition>> {
        Ok(self
            .folder_collections(account_id)
            .await?
            .into_iter()
            .find(|collection| collection.id == collection_id))
    }

    async fn owned_mail_folder(
        &self,
        account_id: Uuid,
        collection_id: &str,
    ) -> Result<Option<CollectionDefinition>> {
        Ok(self
            .resolve_collection(account_id, collection_id)
            .await?
            .filter(|collection| {
                collection.account_id == account_id && mail_collection(collection)
            }))
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

fn command_status_response(
    protocol_version: &str,
    page: u8,
    command: &str,
    status: &str,
) -> Result<Response> {
    let mut response = WbxmlNode::new(page, command);
    response.push(WbxmlNode::with_text(page, "Status", status));
    wbxml_response(protocol_version, encode_wbxml(&response))
}

fn search_status_response(
    protocol_version: &str,
    search_status: &str,
    store_status: Option<&str>,
) -> Result<Response> {
    let mut response = WbxmlNode::new(15, "Search");
    response.push(WbxmlNode::with_text(15, "Status", search_status));
    if let Some(store_status) = store_status {
        let mut response_node = WbxmlNode::new(15, "Response");
        let mut store = WbxmlNode::new(15, "Store");
        store.push(WbxmlNode::with_text(15, "Status", store_status));
        response_node.push(store);
        response.push(response_node);
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

fn collection_body_preference(collection_node: &WbxmlNode) -> BodyPreference {
    collection_node
        .child("Options")
        .map(options_body_preference)
        .unwrap_or_default()
}

fn fetch_body_preference(fetch_node: &WbxmlNode) -> BodyPreference {
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

fn collection_deletes_as_moves(collection_node: &WbxmlNode) -> bool {
    collection_node
        .child("DeletesAsMoves")
        .map(|node| {
            let value = node.text_value().trim();
            value.is_empty() || value == "1" || value.eq_ignore_ascii_case("true")
        })
        .unwrap_or(true)
}

fn hierarchy_generation(collections: &[CollectionDefinition]) -> String {
    let mut entries = collections
        .iter()
        .map(|collection| {
            format!(
                "{}|{}:{}:{}:{}",
                collection.id,
                collection.class_name,
                collection.parent_id.as_deref().unwrap_or(ROOT_FOLDER_ID),
                collection.display_name,
                collection.folder_type.as_str()
            )
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries.join("\n")
}

fn hierarchy_generation_from_snapshot(snapshot: &Value) -> String {
    let mut entries = match snapshot {
        Value::Array(entries) => entries
            .iter()
            .filter_map(|entry| {
                let object = entry.as_object()?;
                Some(format!(
                    "{}|{}",
                    object.get("id")?.as_str()?,
                    object.get("fingerprint")?.as_str()?
                ))
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    entries.sort();
    entries.join("\n")
}

fn validate_mime_attachments(bytes: &[u8]) -> Result<()> {
    validate_mime_attachments_with_validator(&Validator::from_env(), bytes)
}

fn validate_mime_attachments_with_validator<D: Detector>(
    validator: &Validator<D>,
    bytes: &[u8],
) -> Result<()> {
    for attachment in collect_mime_attachment_parts(bytes)? {
        let outcome = validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::ActiveSyncMimeSubmission,
                declared_mime: attachment.declared_mime.clone(),
                filename: attachment.filename.clone(),
                expected_kind: ExpectedKind::Any,
            },
            &attachment.bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "ActiveSync SendMail blocked by Magika validation for {:?}: {}",
                attachment.filename,
                outcome.reason
            );
        }
    }
    Ok(())
}

fn decode_sync_state(snapshot_json: &str) -> Result<StoredSyncState> {
    if let Ok(state) = serde_json::from_str::<StoredSyncState>(snapshot_json) {
        return Ok(state);
    }

    let legacy_snapshot: Value = serde_json::from_str(snapshot_json)?;
    let collection_state = match legacy_snapshot {
        Value::Array(entries) => entries
            .into_iter()
            .filter_map(|entry| {
                let object = entry.as_object()?;
                Some(CollectionStateEntry {
                    server_id: object.get("id")?.as_str()?.to_string(),
                    fingerprint: object.get("fingerprint")?.as_str()?.to_string(),
                })
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    Ok(completed_sync_state(collection_state, None))
}

fn completed_sync_state(
    collection_state: Vec<CollectionStateEntry>,
    hierarchy_generation: Option<String>,
) -> StoredSyncState {
    StoredSyncState {
        hierarchy_generation,
        collection_state,
        pending_changes: Vec::new(),
        next_offset: 0,
    }
}

fn ping_change_categories(
    monitored: &[(CollectionDefinition, StoredSyncState)],
) -> Vec<CanonicalChangeCategory> {
    let mut categories = Vec::new();
    for (collection, _) in monitored {
        let category = if mail_collection(collection) {
            Some(CanonicalChangeCategory::Mail)
        } else if collection.class_name == CONTACTS_CLASS {
            Some(CanonicalChangeCategory::Contacts)
        } else if collection.class_name == CALENDAR_CLASS {
            Some(CanonicalChangeCategory::Calendar)
        } else {
            None
        };
        if let Some(category) = category {
            if !categories.contains(&category) {
                categories.push(category);
            }
        }
    }
    categories
}

fn ping_deadline(heartbeat_interval: u32) -> Instant {
    Instant::now() + ping_heartbeat_duration(heartbeat_interval)
}

#[cfg(not(test))]
fn ping_heartbeat_duration(heartbeat_interval: u32) -> Duration {
    Duration::from_secs(u64::from(heartbeat_interval))
}

#[cfg(test)]
fn ping_heartbeat_duration(heartbeat_interval: u32) -> Duration {
    Duration::from_millis(u64::from(heartbeat_interval))
}

fn has_client_commands(collection_node: &WbxmlNode) -> bool {
    collection_node
        .child("Commands")
        .map(|commands| !commands.children.is_empty())
        .unwrap_or(false)
}

fn sync_collection_status_node(collection_id: Option<&str>, status: &str) -> WbxmlNode {
    let mut collection = WbxmlNode::new(0, "Collection");
    if let Some(collection_id) = collection_id {
        collection.push(WbxmlNode::with_text(0, "CollectionId", collection_id));
    }
    collection.push(WbxmlNode::with_text(0, "Status", status));
    collection
}

fn sync_collection_has_unsupported_command(
    collection_node: &WbxmlNode,
    collection: &CollectionDefinition,
) -> bool {
    let Some(commands) = collection_node.child("Commands") else {
        return false;
    };
    commands
        .children
        .iter()
        .any(|command| !sync_command_supported_for_collection(command.name.as_str(), collection))
}

fn sync_command_supported_for_collection(command: &str, collection: &CollectionDefinition) -> bool {
    if drafts_collection(collection) {
        return matches!(command, "Add" | "Change" | "Delete");
    }
    if mail_collection(collection) {
        return matches!(command, "Change" | "Delete");
    }
    if collection.class_name == CONTACTS_CLASS || collection.class_name == CALENDAR_CLASS {
        return matches!(command, "Add" | "Change" | "Delete");
    }
    false
}

fn pending_page(
    changes: &[SnapshotChange],
    offset: usize,
    window_size: u64,
) -> (Vec<SnapshotChange>, usize) {
    let end = (offset + window_size as usize).min(changes.len());
    (changes[offset..end].to_vec(), end)
}

fn value_to_wbxml(value: Value) -> WbxmlNode {
    match value {
        Value::Object(map) => crate::snapshot::value_to_node(&map),
        _ => WbxmlNode::new(0, "ApplicationData"),
    }
}

fn mail_flag_update(flag: &WbxmlNode) -> Result<JmapEmailFollowupUpdate> {
    if flag.children.is_empty() {
        return Ok(JmapEmailFollowupUpdate {
            flagged: Some(false),
            followup_flag_status: Some("none".to_string()),
            ..Default::default()
        });
    }

    let status = field_text(flag, "Status").unwrap_or_else(|| "0".to_string());
    let mut update = match status.as_str() {
        "0" => JmapEmailFollowupUpdate {
            flagged: Some(false),
            followup_flag_status: Some("none".to_string()),
            ..Default::default()
        },
        "1" => JmapEmailFollowupUpdate {
            flagged: Some(true),
            followup_flag_status: Some("complete".to_string()),
            followup_icon: Some(6),
            todo_item_flags: Some(8),
            ..Default::default()
        },
        "2" => JmapEmailFollowupUpdate {
            flagged: Some(true),
            followup_flag_status: Some("flagged".to_string()),
            followup_icon: Some(6),
            todo_item_flags: Some(8),
            ..Default::default()
        },
        _ => bail!("unsupported ActiveSync mail flag status"),
    };

    if let Some(flag_type) = field_text(flag, "FlagType").filter(|value| !value.is_empty()) {
        update.followup_request = Some(flag_type);
    }
    let start = field_text(flag, "UtcStartDate")
        .or_else(|| field_text(flag, "StartDate"))
        .map(|value| active_sync_datetime_to_rfc3339(&value))
        .transpose()?;
    let due = field_text(flag, "UtcDueDate")
        .or_else(|| field_text(flag, "DueDate"))
        .map(|value| active_sync_datetime_to_rfc3339(&value))
        .transpose()?;
    if start.is_some() != due.is_some() {
        bail!("ActiveSync mail flag start and due dates must be paired");
    }
    update.followup_start_at = start;
    update.followup_due_at = due;
    update.followup_completed_at = field_text(flag, "CompleteTime")
        .or_else(|| field_text(flag, "DateCompleted"))
        .map(|value| active_sync_datetime_to_rfc3339(&value))
        .transpose()?;

    Ok(update)
}

fn active_sync_datetime_to_rfc3339(value: &str) -> Result<String> {
    let value = value.trim();
    if value.contains('-') {
        return Ok(value.to_string());
    }
    let bytes = value.as_bytes();
    if bytes.len() == 16 && bytes[8] == b'T' && bytes[15] == b'Z' {
        return Ok(format!(
            "{}-{}-{}T{}:{}:{}Z",
            &value[0..4],
            &value[4..6],
            &value[6..8],
            &value[9..11],
            &value[11..13],
            &value[13..15]
        ));
    }
    bail!("invalid ActiveSync dateTime value")
}

fn parse_contact_input(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: Option<&lpe_storage::ClientContact>,
    application_data: &WbxmlNode,
) -> Result<UpsertClientContactInput> {
    let file_as = field_text(application_data, "FileAs");
    let first_name = field_text(application_data, "FirstName").unwrap_or_default();
    let last_name = field_text(application_data, "LastName").unwrap_or_default();
    let derived_name = format!("{first_name} {last_name}").trim().to_string();
    let name = file_as
        .or_else(|| (!derived_name.is_empty()).then_some(derived_name))
        .or_else(|| existing.map(|contact| contact.name.clone()))
        .unwrap_or_default();
    let email = field_text(application_data, "Email1Address")
        .or_else(|| existing.map(|contact| contact.email.clone()))
        .unwrap_or_default();
    let phone = field_text(application_data, "MobilePhoneNumber")
        .or_else(|| field_text(application_data, "BusinessPhoneNumber"))
        .or_else(|| field_text(application_data, "HomePhoneNumber"))
        .or_else(|| existing.map(|contact| contact.phone.clone()))
        .unwrap_or_default();
    let notes = body_text(application_data)
        .or_else(|| existing.map(|contact| contact.notes.clone()))
        .unwrap_or_default();

    Ok(UpsertClientContactInput {
        id,
        account_id,
        name,
        role: field_text(application_data, "JobTitle")
            .or_else(|| field_text(application_data, "Title"))
            .or_else(|| existing.map(|contact| contact.role.clone()))
            .unwrap_or_default(),
        email,
        phone,
        team: field_text(application_data, "CompanyName")
            .or_else(|| existing.map(|contact| contact.team.clone()))
            .unwrap_or_default(),
        notes,
    })
}

fn parse_event_input(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: Option<&lpe_storage::ClientEvent>,
    application_data: &WbxmlNode,
) -> Result<UpsertClientEventInput> {
    let start = field_text(application_data, "StartTime")
        .or_else(|| {
            existing.map(|event| {
                format!(
                    "{}T{}:00Z",
                    event.date.replace('-', ""),
                    event.time.replace(':', "")
                )
            })
        })
        .unwrap_or_default();
    let (date, time) = parse_compact_datetime(&start)?;
    let end = field_text(application_data, "EndTime");
    let duration_minutes = end
        .as_deref()
        .map(|value| duration_from_datetimes(&start, value))
        .transpose()?
        .or_else(|| existing.map(|event| event.duration_minutes))
        .unwrap_or_default();
    let attendees_metadata = attendees_from_nodes(application_data);
    let attendees = attendees_metadata
        .as_ref()
        .map(calendar_attendee_labels)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| existing.map(|event| event.attendees.clone()))
        .unwrap_or_default();
    let notes = body_text(application_data)
        .or_else(|| existing.map(|event| event.notes.clone()))
        .unwrap_or_default();

    Ok(UpsertClientEventInput {
        id,
        account_id,
        uid: field_text(application_data, "UID")
            .or_else(|| existing.map(|event| event.uid.clone()))
            .unwrap_or_default(),
        date,
        time,
        time_zone: field_text(application_data, "TimeZone")
            .or_else(|| existing.map(|event| event.time_zone.clone()))
            .unwrap_or_default(),
        duration_minutes,
        all_day: field_text(application_data, "AllDayEvent")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .or_else(|| existing.map(|event| event.all_day))
            .unwrap_or(false),
        status: existing
            .map(|event| event.status.clone())
            .unwrap_or_else(|| "confirmed".to_string()),
        sequence: existing.map(|event| event.sequence).unwrap_or(0),
        recurrence_rule: if let Some(recurrence) = application_data.child("Recurrence") {
            if recurrence.children.is_empty() {
                String::new()
            } else {
                recurrence_to_rrule(recurrence)?
            }
        } else {
            existing
                .map(|event| event.recurrence_rule.clone())
                .unwrap_or_default()
        },
        recurrence_json: existing
            .map(|event| event.recurrence_json.clone())
            .unwrap_or_else(|| "{}".to_string()),
        recurrence_exceptions_json: existing
            .map(|event| event.recurrence_exceptions_json.clone())
            .unwrap_or_else(|| "[]".to_string()),
        title: field_text(application_data, "Subject")
            .or_else(|| existing.map(|event| event.title.clone()))
            .unwrap_or_default(),
        location: field_text(application_data, "Location")
            .or_else(|| existing.map(|event| event.location.clone()))
            .unwrap_or_default(),
        organizer_json: existing
            .map(|event| event.organizer_json.clone())
            .unwrap_or_else(|| "{}".to_string()),
        attendees,
        attendees_json: attendees_metadata
            .as_ref()
            .map(serialize_calendar_participants_metadata)
            .or_else(|| existing.map(|event| event.attendees_json.clone()))
            .unwrap_or_default(),
        notes,
        body_html: existing
            .map(|event| event.body_html.clone())
            .unwrap_or_default(),
    })
}

fn body_text(application_data: &WbxmlNode) -> Option<String> {
    application_data.child("Body").and_then(|body| {
        body.child("Data")
            .map(|node| node.text_value().trim().to_string())
            .or_else(|| {
                let value = body.text_value().trim();
                (!value.is_empty()).then(|| value.to_string())
            })
    })
}

fn parse_compact_datetime(value: &str) -> Result<(String, String)> {
    let compact = value.trim().trim_end_matches('Z');
    let (date_part, time_part) = compact
        .split_once('T')
        .ok_or_else(|| anyhow!("invalid ActiveSync datetime"))?;
    if date_part.len() != 8 || time_part.len() < 4 {
        bail!("invalid ActiveSync datetime");
    }
    Ok((
        format!(
            "{}-{}-{}",
            &date_part[0..4],
            &date_part[4..6],
            &date_part[6..8]
        ),
        format!("{}:{}", &time_part[0..2], &time_part[2..4]),
    ))
}

fn duration_from_datetimes(start: &str, end: &str) -> Result<i32> {
    let (start_date, start_time) = parse_compact_datetime(start)?;
    let (end_date, end_time) = parse_compact_datetime(end)?;
    let start_minutes = date_time_to_minutes(&start_date, &start_time)?;
    let end_minutes = date_time_to_minutes(&end_date, &end_time)?;
    Ok((end_minutes - start_minutes).max(0) as i32)
}

fn date_time_to_minutes(date: &str, time: &str) -> Result<i64> {
    let mut date_parts = date.split('-');
    let year = date_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync date"))?
        .parse::<i64>()?;
    let month = date_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync date"))?
        .parse::<i64>()?;
    let day = date_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync date"))?
        .parse::<i64>()?;
    let mut time_parts = time.split(':');
    let hour = time_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync time"))?
        .parse::<i64>()?;
    let minute = time_parts
        .next()
        .ok_or_else(|| anyhow!("invalid ActiveSync time"))?
        .parse::<i64>()?;
    Ok(days_from_civil(year, month, day) * 1440 + hour * 60 + minute)
}

fn attendees_from_nodes(application_data: &WbxmlNode) -> Option<CalendarParticipantsMetadata> {
    let attendees_node = application_data.child("Attendees")?;
    let attendees = attendees_node
        .children_named("Attendee")
        .into_iter()
        .filter_map(|attendee| {
            let email = attendee
                .child("Email")
                .map(|value| value.text_value().trim())
                .unwrap_or("");
            let name = attendee
                .child("Name")
                .map(|value| value.text_value().trim())
                .unwrap_or("");
            if name.is_empty() && email.is_empty() {
                return None;
            }
            Some(CalendarParticipantMetadata {
                email: email.to_ascii_lowercase(),
                common_name: name.to_string(),
                role: match attendee
                    .child("AttendeeType")
                    .map(|node| node.text_value().trim())
                {
                    Some("2") => "OPT-PARTICIPANT".to_string(),
                    _ => "REQ-PARTICIPANT".to_string(),
                },
                partstat: match attendee
                    .child("AttendeeStatus")
                    .map(|node| node.text_value().trim())
                {
                    Some("2") => "tentative".to_string(),
                    Some("3") => "accepted".to_string(),
                    Some("4") => "declined".to_string(),
                    _ => "needs-action".to_string(),
                },
                rsvp: false,
            })
        })
        .collect::<Vec<_>>();
    if attendees.is_empty() {
        return None;
    }
    Some(CalendarParticipantsMetadata {
        organizer: None,
        attendees,
    })
}

fn recurrence_to_rrule(recurrence: &WbxmlNode) -> Result<String> {
    let recurrence_type = field_text(recurrence, "Type").unwrap_or_else(|| "0".to_string());
    let mut parts = Vec::new();
    match recurrence_type.as_str() {
        "0" => {
            if let Some(days) =
                field_text(recurrence, "DayOfWeek").and_then(|value| day_of_week_to_rrule(&value))
            {
                parts.push("FREQ=WEEKLY".to_string());
                parts.push(format!("BYDAY={days}"));
            } else {
                parts.push("FREQ=DAILY".to_string());
            }
        }
        "1" => {
            parts.push("FREQ=WEEKLY".to_string());
            if let Some(days) =
                field_text(recurrence, "DayOfWeek").and_then(|value| day_of_week_to_rrule(&value))
            {
                parts.push(format!("BYDAY={days}"));
            }
        }
        "2" => {
            parts.push("FREQ=MONTHLY".to_string());
            let day = field_text(recurrence, "DayOfMonth")
                .ok_or_else(|| anyhow!("monthly recurrence is missing DayOfMonth"))?;
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
        }
        "5" => {
            parts.push("FREQ=YEARLY".to_string());
            let day = field_text(recurrence, "DayOfMonth")
                .ok_or_else(|| anyhow!("yearly recurrence is missing DayOfMonth"))?;
            let month = field_text(recurrence, "MonthOfYear")
                .ok_or_else(|| anyhow!("yearly recurrence is missing MonthOfYear"))?;
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
            parts.push(format!(
                "BYMONTH={}",
                parse_positive_number(&month, "MonthOfYear")?
            ));
        }
        other => bail!("unsupported ActiveSync recurrence type {other}"),
    }
    if let Some(interval) = field_text(recurrence, "Interval")
        .map(|value| parse_positive_number(&value, "Interval"))
        .transpose()?
        .filter(|value| *value > 1)
    {
        parts.push(format!("INTERVAL={interval}"));
    }
    if let Some(count) = field_text(recurrence, "Occurrences")
        .map(|value| parse_positive_number(&value, "Occurrences"))
        .transpose()?
    {
        parts.push(format!("COUNT={count}"));
    }
    if let Some(until) = field_text(recurrence, "Until") {
        parts.push(format!("UNTIL={}", compact_datetime_date(&until)?));
    }
    Ok(parts.join(";"))
}

fn day_of_week_to_rrule(value: &str) -> Option<String> {
    let mask = value.trim().parse::<u32>().ok()?;
    let mut days = Vec::new();
    for (bit, day) in [
        (1, "SU"),
        (2, "MO"),
        (4, "TU"),
        (8, "WE"),
        (16, "TH"),
        (32, "FR"),
        (64, "SA"),
    ] {
        if mask & bit != 0 {
            days.push(day);
        }
    }
    (!days.is_empty()).then(|| days.join(","))
}

fn parse_positive_number(value: &str, field: &str) -> Result<u32> {
    let number = value
        .trim()
        .parse::<u32>()
        .map_err(|_| anyhow!("{field} must be a positive integer"))?;
    if number == 0 {
        bail!("{field} must be a positive integer");
    }
    Ok(number)
}

fn compact_datetime_date(value: &str) -> Result<String> {
    let compact = value.trim().trim_end_matches('Z');
    let date = compact.split('T').next().unwrap_or_default();
    if date.len() != 8 {
        bail!("invalid ActiveSync recurrence Until");
    }
    Ok(date.to_string())
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn search_query_text(store: &WbxmlNode) -> Option<String> {
    store
        .child("Query")
        .and_then(|query| {
            field_text(query, "FreeText").or_else(|| {
                let parts = query
                    .children_named("Value")
                    .into_iter()
                    .map(|node| node.text_value().trim())
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>();
                (!parts.is_empty()).then(|| parts.join(" "))
            })
        })
        .or_else(|| field_text(store, "Query"))
}

fn parse_range(value: Option<&str>) -> Result<(u64, u64)> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok((0, 49));
    };
    let Some((start, end)) = value.split_once('-') else {
        bail!("invalid Search range");
    };
    let start = start.trim().parse::<u64>()?;
    let end = end.trim().parse::<u64>()?;
    if end < start {
        bail!("invalid Search range");
    }
    Ok((start, end))
}

fn trim_preview(value: &str) -> String {
    value
        .split_whitespace()
        .take(24)
        .collect::<Vec<_>>()
        .join(" ")
}

fn reply_recipients(
    principal_email: &str,
    source_message: &lpe_storage::JmapEmail,
) -> Vec<SubmittedRecipientInput> {
    if source_message
        .from_address
        .eq_ignore_ascii_case(principal_email)
    {
        return source_message
            .to
            .iter()
            .filter(|recipient| !recipient.address.eq_ignore_ascii_case(principal_email))
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
    }

    vec![SubmittedRecipientInput {
        address: source_message.from_address.clone(),
        display_name: source_message.from_display.clone(),
    }]
}

fn default_reply_subject(command_name: &str, original_subject: &str) -> String {
    let normalized = original_subject.trim();
    let prefix = if command_name == "SmartForward" {
        "Fwd:"
    } else {
        "Re:"
    };
    if normalized
        .to_ascii_lowercase()
        .starts_with(&prefix[..2].to_ascii_lowercase())
    {
        normalized.to_string()
    } else {
        format!("{prefix} {normalized}").trim().to_string()
    }
}

fn merge_smart_body(command_name: &str, composed: &str, original: &str) -> String {
    let label = if command_name == "SmartForward" {
        "Forwarded message"
    } else {
        "Original message"
    };
    let composed = composed.trim();
    let original = original.trim();
    if composed.is_empty() {
        original.to_string()
    } else if original.is_empty() {
        composed.to_string()
    } else {
        format!("{composed}\n\n----- {label} -----\n{original}")
    }
}

fn header_policy_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-ms-policykey")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn policy_required_response(
    command: ActiveSyncCommand,
    protocol_version: &str,
) -> Result<Response> {
    let (page, root) = match command {
        ActiveSyncCommand::FolderSync => (7, "FolderSync"),
        ActiveSyncCommand::GetItemEstimate => (6, "GetItemEstimate"),
        ActiveSyncCommand::ItemOperations => (20, "ItemOperations"),
        ActiveSyncCommand::MoveItems => (5, "MoveItems"),
        ActiveSyncCommand::Search => (15, "Search"),
        ActiveSyncCommand::SendMail => (21, "SendMail"),
        ActiveSyncCommand::SmartForward => (21, "SmartForward"),
        ActiveSyncCommand::SmartReply => (21, "SmartReply"),
        ActiveSyncCommand::Sync => (0, "Sync"),
        _ => (0, "Sync"),
    };
    let mut response = WbxmlNode::new(page, root);
    response.push(WbxmlNode::with_text(
        page,
        "Status",
        ActiveSyncStatus::PolicyRequired.as_str(),
    ));
    wbxml_response(protocol_version, encode_wbxml(&response))
}

#[cfg(test)]
mod tests {
    use super::validate_mime_attachments_with_validator;
    use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};

    #[derive(Debug, Clone)]
    struct FakeDetector {
        detection: MagikaDetection,
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
            Ok(self.detection.clone())
        }
    }

    #[test]
    fn activesync_sendmail_blocks_mismatched_attachment_payloads() {
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "exe".to_string(),
                    mime_type: "application/x-msdownload".to_string(),
                    description: "exe".to_string(),
                    group: "binary".to_string(),
                    extensions: vec!["exe".to_string()],
                    score: Some(0.99),
                },
            },
            0.80,
        );
        let mime = concat!(
            "Content-Type: multipart/mixed; boundary=\"abc\"\r\n",
            "\r\n",
            "--abc\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "Body\r\n",
            "--abc\r\n",
            "Content-Type: application/pdf; name=\"invoice.pdf\"\r\n",
            "Content-Disposition: attachment; filename=\"invoice.pdf\"\r\n",
            "\r\n",
            "%PDF-1.7\r\n",
            "--abc--\r\n"
        );

        let error =
            validate_mime_attachments_with_validator(&validator, mime.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("ActiveSync SendMail blocked"));
    }
}
