use anyhow::{anyhow, bail, Result};
use axum::{http::HeaderMap, response::Response};
use lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{
    ActiveSyncItemState, AuditEntryInput, SubmitMessageInput, SubmittedRecipientInput,
    UpsertClientContactInput, UpsertClientEventInput,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

use crate::{
    auth::protocol_version,
    constants::{
        CALENDAR_CLASS, CONTACTS_CLASS, FOLDER_SYNC_COLLECTION_ID, MAIL_CLASS, ROOT_FOLDER_ID,
    },
    message::{
        default_sender, draft_input_from_application_data, field_text, merged_draft_input,
        parse_mime_message,
    },
    response::{empty_response, is_message_rfc822, policy_key, sync_status_node, wbxml_response},
    snapshot::{
        calendar_application_data, collection_window_size, contact_application_data,
        diff_collection_states, diff_snapshots, drafts_collection, email_application_data,
        mail_collection, parse_collection_mailbox_id, require_collection_id,
        require_sync_collections, snapshot_to_value,
    },
    store::ActiveSyncStore,
    types::{
        ActiveSyncQuery, AuthenticatedPrincipal, CollectionDefinition, CollectionStateEntry,
        SnapshotChange, SnapshotEntry, StoredSyncState,
    },
    wbxml::{decode_wbxml, encode_wbxml, WbxmlNode},
};

#[derive(Clone)]
pub struct ActiveSyncService<S> {
    store: S,
}

impl<S> ActiveSyncService<S> {
    pub fn new(store: S) -> Self {
        Self { store }
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

    pub(crate) async fn handle_request(
        &self,
        query: ActiveSyncQuery,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let command = query
            .cmd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("missing ActiveSync command"))?;
        let device_id = query
            .device_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("missing DeviceId"))?;
        let protocol_version = protocol_version(headers);
        let principal = self.authenticate(query.user.as_deref(), headers).await?;

        match command {
            "Provision" => {
                let request = decode_wbxml(body)?;
                self.handle_provision(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "FolderSync" => {
                let request = decode_wbxml(body)?;
                self.handle_folder_sync(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "GetItemEstimate" => {
                let request = decode_wbxml(body)?;
                self.handle_get_item_estimate(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "Sync" => {
                let request = decode_wbxml(body)?;
                self.handle_sync(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "ItemOperations" => {
                let request = decode_wbxml(body)?;
                self.handle_item_operations(&principal, &protocol_version, &request)
                    .await
            }
            "Ping" => {
                let request = decode_wbxml(body)?;
                self.handle_ping(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "Search" => {
                let request = decode_wbxml(body)?;
                self.handle_search(&principal, &protocol_version, &request)
                    .await
            }
            "SendMail" => {
                self.handle_send_mail(&principal, &protocol_version, headers, body)
                    .await
            }
            "SmartReply" => {
                let request = decode_wbxml(body)?;
                self.handle_smart_compose(&principal, &protocol_version, &request, "SmartReply")
                    .await
            }
            "SmartForward" => {
                let request = decode_wbxml(body)?;
                self.handle_smart_compose(&principal, &protocol_version, &request, "SmartForward")
                    .await
            }
            other => bail!("unsupported ActiveSync command: {other}"),
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
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Provision" {
            bail!("invalid Provision payload");
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
            device_information.push(WbxmlNode::with_text(18, "Status", "1"));
            response.push(device_information);
        }

        response.push(WbxmlNode::with_text(14, "Status", "1"));
        let mut policies = WbxmlNode::new(14, "Policies");
        let mut policy = WbxmlNode::new(14, "Policy");
        policy.push(WbxmlNode::with_text(
            14,
            "PolicyType",
            "MS-EAS-Provisioning-WBXML",
        ));
        policy.push(WbxmlNode::with_text(14, "Status", "1"));
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

    async fn handle_folder_sync(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "FolderSync" {
            bail!("invalid FolderSync payload");
        }

        let requested_key = request
            .child("SyncKey")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let collections = self.folder_collections(principal.account_id).await?;
        let snapshot = snapshot_to_value(
            &collections
                .iter()
                .map(|collection| SnapshotEntry {
                    server_id: collection.id.clone(),
                    fingerprint: format!(
                        "{}:{}:{}",
                        collection.class_name, collection.display_name, collection.folder_type
                    ),
                    data: json!({
                        "page": 7,
                        "name": "Folder",
                        "children": [],
                    }),
                })
                .collect::<Vec<_>>(),
        );

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
        response.push(WbxmlNode::with_text(7, "Status", "1"));
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
                        node.push(WbxmlNode::with_text(7, "ServerId", &collection.id));
                        node.push(WbxmlNode::with_text(7, "ParentId", ROOT_FOLDER_ID));
                        node.push(WbxmlNode::with_text(
                            7,
                            "DisplayName",
                            &collection.display_name,
                        ));
                        node.push(WbxmlNode::with_text(7, "Type", &collection.folder_type));
                        changes_node.push(node);
                    }
                }
                "Update" => {
                    if let Some(collection) = collections
                        .iter()
                        .find(|collection| collection.id == change.server_id)
                    {
                        let mut node = WbxmlNode::new(7, "Update");
                        node.push(WbxmlNode::with_text(7, "ServerId", &collection.id));
                        node.push(WbxmlNode::with_text(7, "ParentId", ROOT_FOLDER_ID));
                        node.push(WbxmlNode::with_text(
                            7,
                            "DisplayName",
                            &collection.display_name,
                        ));
                        node.push(WbxmlNode::with_text(7, "Type", &collection.folder_type));
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

        response.push(WbxmlNode::with_text(6, "Status", "1"));
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

        response.push(WbxmlNode::with_text(6, "Status", "1"));
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
        let collection_nodes = require_sync_collections(request)?;
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
        let collection_id = require_collection_id(collection_node)?;
        let sync_key = collection_node
            .child("SyncKey")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let window_size = collection_window_size(request, collection_node);

        let Some(collection) = self
            .resolve_collection(principal.account_id, &collection_id)
            .await?
        else {
            return Ok(sync_status_node(&collection_id, "9"));
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
            return Ok(sync_status_node(&collection.id, "9"));
        }

        let client_responses = if drafts_collection(&collection) {
            self.apply_draft_sync_commands(principal, &collection, collection_node)
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
        response_collection.push(WbxmlNode::with_text(0, "Status", "1"));

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
                completed_sync_state(final_state.clone())
            } else {
                StoredSyncState {
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
                    return Ok(sync_status_node(&collection.id, "9"));
                }
                let commands = self
                    .build_commands(principal.account_id, &collection, &pending_page.0)
                    .await?;
                let more_available = pending_page.1 < previous_state.pending_changes.len();
                let stored_state = if more_available {
                    StoredSyncState {
                        next_offset: pending_page.1,
                        ..previous_state.clone()
                    }
                } else {
                    completed_sync_state(previous_state.collection_state.clone())
                };
                (commands, more_available, stored_state)
            } else {
                let changed_items =
                    diff_collection_states(&previous_state.collection_state, &final_state);
                let pending_page = pending_page(&changed_items, 0, window_size);
                let commands = self
                    .build_commands(principal.account_id, &collection, &pending_page.0)
                    .await?;
                let more_available = pending_page.1 < changed_items.len();
                let stored_state = if more_available {
                    StoredSyncState {
                        collection_state: final_state,
                        pending_changes: changed_items,
                        next_offset: pending_page.1,
                    }
                } else {
                    completed_sync_state(final_state)
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

        let latest_sync_state = decode_sync_state(&latest_state.snapshot_json)?;
        if sync_state_is_complete(&latest_sync_state) && latest_state.sync_key != requested_key {
            return Ok(None);
        }

        Ok(Some(requested_state))
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
    ) -> Result<WbxmlNode> {
        let mut commands = WbxmlNode::new(0, "Commands");
        let item_nodes = self
            .fetch_collection_nodes(account_id, collection, page_changes)
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
                nodes.insert(
                    email.id.to_string(),
                    email_application_data(&email, &attachments).pipe(value_to_wbxml),
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
                    add.push(WbxmlNode::with_text(0, "Status", "1"));
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
                    change.push(WbxmlNode::with_text(0, "Status", "1"));
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
                    delete.push(WbxmlNode::with_text(0, "Status", "1"));
                    responses.push(delete);
                }
                "Fetch" => {}
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
                    add.push(WbxmlNode::with_text(0, "Status", "1"));
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
                    change.push(WbxmlNode::with_text(0, "Status", "1"));
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
                    delete.push(WbxmlNode::with_text(0, "Status", "1"));
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
                    add.push(WbxmlNode::with_text(0, "Status", "1"));
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
                    change.push(WbxmlNode::with_text(0, "Status", "1"));
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
                    delete.push(WbxmlNode::with_text(0, "Status", "1"));
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
                bail!("invalid SendMail payload");
            }
            request
                .child("Mime")
                .map(|node| node.text_value().as_bytes().to_vec())
                .ok_or_else(|| anyhow!("SendMail is missing MIME content"))?
        };

        let parsed = parse_mime_message(&mime_payload)?;
        validate_mime_attachments(&mime_payload)?;
        let mailbox_access = self
            .mailbox_access_for_from_address(
                principal,
                parsed.from.as_ref().map(|mailbox| mailbox.address.as_str()),
            )
            .await?;
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
        self.store
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
            .await?;

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
            bail!("invalid ItemOperations payload");
        }

        let mut root = WbxmlNode::new(20, "ItemOperations");
        root.push(WbxmlNode::with_text(20, "Status", "1"));
        let mut response = WbxmlNode::new(20, "Response");

        for fetch in request.children_named("Fetch") {
            response.push(self.handle_item_operations_fetch(principal, fetch).await?);
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
                node.push(WbxmlNode::with_text(20, "Status", "1"));
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
                node.push(WbxmlNode::with_text(20, "Status", "6"));
            }
            return Ok(node);
        }

        let Some(server_id) = fetch
            .child("ServerId")
            .map(|value| value.text_value().trim())
        else {
            node.push(WbxmlNode::with_text(20, "Status", "2"));
            return Ok(node);
        };
        let message_id = Uuid::parse_str(server_id)?;
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
                .ok_or_else(|| anyhow!("collection not found"))?;
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
            node.push(WbxmlNode::with_text(20, "Status", "1"));
            if let Some(collection_id) = fetch.child("CollectionId") {
                node.push(WbxmlNode::with_text(
                    0,
                    "CollectionId",
                    collection_id.text_value(),
                ));
            }
            node.push(WbxmlNode::with_text(0, "ServerId", email.id.to_string()));
            let mut properties = WbxmlNode::new(20, "Properties");
            properties.push(email_application_data(&email, &attachments).pipe(value_to_wbxml));
            node.push(properties);
        } else {
            node.push(WbxmlNode::with_text(20, "Status", "6"));
        }

        Ok(node)
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

        let folders = request
            .child("Folders")
            .map(|folders| folders.children_named("Folder"))
            .unwrap_or_default();
        if folders.is_empty() {
            let mut response = WbxmlNode::new(13, "Ping");
            response.push(WbxmlNode::with_text(13, "Status", "3"));
            return wbxml_response(protocol_version, encode_wbxml(&response));
        }

        let collections = self.folder_collections(principal.account_id).await?;
        let monitored = match self
            .resolve_ping_collections(principal, device_id, &collections, &folders)
            .await?
        {
            Some(monitored) => monitored,
            None => {
                let mut response = WbxmlNode::new(13, "Ping");
                response.push(WbxmlNode::with_text(13, "Status", "3"));
                return wbxml_response(protocol_version, encode_wbxml(&response));
            }
        };

        let mut changed = Vec::new();
        for (collection, previous_state) in monitored {
            let current_state = self
                .collection_state(principal.account_id, &collection)
                .await?;
            if !diff_collection_states(&previous_state.collection_state, &current_state).is_empty()
            {
                changed.push(collection.id);
            }
        }

        let mut response = WbxmlNode::new(13, "Ping");
        response.push(WbxmlNode::with_text(
            13,
            "Status",
            if changed.is_empty() { "1" } else { "2" },
        ));
        if !changed.is_empty() {
            let mut folders_node = WbxmlNode::new(13, "Folders");
            for collection_id in changed {
                let mut folder = WbxmlNode::new(13, "Folder");
                folder.push(WbxmlNode::with_text(13, "Id", collection_id));
                folders_node.push(folder);
            }
            response.push(folders_node);
        }

        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    async fn resolve_ping_collections(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        collections: &[CollectionDefinition],
        folders: &[&WbxmlNode],
    ) -> Result<Option<Vec<(CollectionDefinition, StoredSyncState)>>> {
        let mut monitored = Vec::new();
        for folder in folders {
            let Some(collection_id) = folder
                .child("Id")
                .map(|node| node.text_value().trim().to_string())
                .filter(|value| !value.is_empty())
            else {
                return Ok(None);
            };
            let Some(collection) = collections.iter().find(|entry| entry.id == collection_id)
            else {
                return Ok(None);
            };
            let Some(state) = self
                .store
                .fetch_latest_activesync_sync_state(principal.account_id, device_id, &collection.id)
                .await?
            else {
                return Ok(None);
            };
            monitored.push((collection.clone(), decode_sync_state(&state.snapshot_json)?));
        }

        Ok(Some(monitored))
    }

    async fn handle_search(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Search" {
            bail!("invalid Search payload");
        }

        let store = request
            .child("Store")
            .ok_or_else(|| anyhow!("Search payload is missing Store"))?;
        let query_text = search_query_text(store);
        let (start, end) = parse_range(
            store
                .child("Options")
                .and_then(|options| options.child("Range"))
                .or_else(|| store.child("Range"))
                .map(|node| node.text_value()),
        );
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
        response.push(WbxmlNode::with_text(15, "Status", "1"));
        let mut response_store = WbxmlNode::new(15, "Store");
        response_store.push(WbxmlNode::with_text(15, "Status", "1"));
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
            properties.push(email_application_data(&email, &attachments).pipe(value_to_wbxml));
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
        command_name: &str,
    ) -> Result<Response> {
        if request.name != command_name {
            bail!("invalid {command_name} payload");
        }

        let (source_mailbox_access, source_message) =
            self.resolve_source_message(principal, request).await?;
        let mime_payload = request
            .child("Mime")
            .map(|node| node.text_value().as_bytes().to_vec())
            .ok_or_else(|| anyhow!("{command_name} is missing MIME content"))?;
        validate_mime_attachments(&mime_payload)?;
        let parsed = parse_mime_message(&mime_payload)?;
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
        self.store
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
            .await?;

        let mut response = WbxmlNode::new(21, command_name);
        response.push(WbxmlNode::with_text(21, "Status", "1"));
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
            for mailbox in self.store.fetch_jmap_mailboxes(access.account_id).await? {
                let display_name = if access.is_owned {
                    mailbox.name.clone()
                } else {
                    format!("{} / {}", access.email, mailbox.name)
                };
                let folder = match mailbox.role.as_str() {
                    "inbox" => Some(("2", display_name)),
                    "sent" => Some(("5", display_name)),
                    "drafts" => Some(("3", display_name)),
                    _ => None,
                };
                if let Some((folder_type, display_name)) = folder {
                    collections.push(CollectionDefinition {
                        id: mailbox.id.to_string(),
                        account_id: access.account_id,
                        class_name: MAIL_CLASS.to_string(),
                        display_name,
                        folder_type: folder_type.to_string(),
                        mailbox_id: Some(mailbox.id),
                    });
                }
            }
        }

        collections.push(CollectionDefinition {
            id: "contacts".to_string(),
            account_id,
            class_name: CONTACTS_CLASS.to_string(),
            display_name: "Contacts".to_string(),
            folder_type: "9".to_string(),
            mailbox_id: None,
        });
        collections.push(CollectionDefinition {
            id: "calendar".to_string(),
            account_id,
            class_name: CALENDAR_CLASS.to_string(),
            display_name: "Calendar".to_string(),
            folder_type: "8".to_string(),
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
    Ok(completed_sync_state(collection_state))
}

fn completed_sync_state(collection_state: Vec<CollectionStateEntry>) -> StoredSyncState {
    StoredSyncState {
        collection_state,
        pending_changes: Vec::new(),
        next_offset: 0,
    }
}

fn sync_state_is_complete(state: &StoredSyncState) -> bool {
    state.next_offset >= state.pending_changes.len()
}

fn has_client_commands(collection_node: &WbxmlNode) -> bool {
    collection_node
        .child("Commands")
        .map(|commands| !commands.children.is_empty())
        .unwrap_or(false)
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
        .or_else(|| field_text(application_data, "HomePhoneNumber"))
        .or_else(|| existing.map(|contact| contact.phone.clone()))
        .unwrap_or_default();

    Ok(UpsertClientContactInput {
        id,
        account_id,
        name,
        role: existing
            .map(|contact| contact.role.clone())
            .unwrap_or_default(),
        email,
        phone,
        team: existing
            .map(|contact| contact.team.clone())
            .unwrap_or_default(),
        notes: existing
            .map(|contact| contact.notes.clone())
            .unwrap_or_default(),
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
    let attendees = field_text(application_data, "OrganizerName")
        .or_else(|| attendees_from_nodes(application_data))
        .or_else(|| existing.map(|event| event.attendees.clone()))
        .unwrap_or_default();
    let notes = application_data
        .child("Body")
        .and_then(|body| body.child("Data"))
        .map(|node| node.text_value().trim().to_string())
        .or_else(|| existing.map(|event| event.notes.clone()))
        .unwrap_or_default();

    Ok(UpsertClientEventInput {
        id,
        account_id,
        date,
        time,
        time_zone: field_text(application_data, "TimeZone")
            .or_else(|| existing.map(|event| event.time_zone.clone()))
            .unwrap_or_default(),
        duration_minutes,
        recurrence_rule: existing
            .map(|event| event.recurrence_rule.clone())
            .unwrap_or_default(),
        title: field_text(application_data, "Subject")
            .or_else(|| existing.map(|event| event.title.clone()))
            .unwrap_or_default(),
        location: field_text(application_data, "Location")
            .or_else(|| existing.map(|event| event.location.clone()))
            .unwrap_or_default(),
        attendees,
        attendees_json: existing
            .map(|event| event.attendees_json.clone())
            .unwrap_or_default(),
        notes,
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
    let date_value = date.replace('-', "").parse::<i64>()?;
    let time_value = time.replace(':', "").parse::<i64>()?;
    Ok(date_value * 1440 + (time_value / 100) * 60 + (time_value % 100))
}

fn attendees_from_nodes(application_data: &WbxmlNode) -> Option<String> {
    let attendees = application_data
        .child("Attendees")?
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
            if !name.is_empty() && !email.is_empty() {
                Some(format!("{name} <{email}>"))
            } else if !name.is_empty() {
                Some(name.to_string())
            } else if !email.is_empty() {
                Some(email.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    (!attendees.is_empty()).then(|| attendees.join(", "))
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

fn parse_range(value: Option<&str>) -> (u64, u64) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return (0, 49);
    };
    let Some((start, end)) = value.split_once('-') else {
        return (0, 49);
    };
    let start = start.trim().parse::<u64>().unwrap_or(0);
    let end = end.trim().parse::<u64>().unwrap_or(start + 49);
    (start, end.max(start))
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
