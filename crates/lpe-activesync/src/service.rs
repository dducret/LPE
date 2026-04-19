use anyhow::{anyhow, bail, Result};
use axum::{http::HeaderMap, response::Response};
use lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{
    ActiveSyncItemState, AuditEntryInput, SubmitMessageInput,
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
    message::{draft_input_from_application_data, merged_draft_input, parse_mime_message},
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
            "Sync" => {
                let request = decode_wbxml(body)?;
                self.handle_sync(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "SendMail" => {
                self.handle_send_mail(&principal, &protocol_version, headers, body)
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
        authenticate_account(&self.store, hinted_user, headers).await
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
            self.store
                .fetch_activesync_sync_state(
                    principal.account_id,
                    device_id,
                    FOLDER_SYNC_COLLECTION_ID,
                    &requested_key,
                )
                .await?
                .map(|state| serde_json::from_str(&state.snapshot_json))
                .transpose()?
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
            self.store
                .fetch_activesync_sync_state(
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
            self.apply_draft_sync_commands(principal, collection_node)
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
                    .build_commands(
                        principal.account_id,
                        &collection,
                        &pending_page.0,
                    )
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

    async fn collection_state(
        &self,
        account_id: Uuid,
        collection: &CollectionDefinition,
    ) -> Result<Vec<CollectionStateEntry>> {
        let mut state = if mail_collection(collection) {
            let mailbox_id = parse_collection_mailbox_id(collection)?;
            self.fetch_all_mail_states(account_id, mailbox_id).await?
        } else if collection.class_name == CONTACTS_CLASS {
            self.store.fetch_activesync_contact_states(account_id).await?
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
            for email in self.store.fetch_jmap_emails(account_id, &ids).await? {
                nodes.insert(email.id.to_string(), email_application_data(&email).pipe(value_to_wbxml));
            }
        } else if collection.class_name == CONTACTS_CLASS {
            for contact in self.store.fetch_client_contacts_by_ids(account_id, &ids).await? {
                nodes.insert(
                    contact.id.to_string(),
                    contact_application_data(&contact).pipe(value_to_wbxml),
                );
            }
        } else if collection.class_name == CALENDAR_CLASS {
            for event in self.store.fetch_client_events_by_ids(account_id, &ids).await? {
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
        let current_state = self.fetch_collection_states_by_ids(account_id, collection, &ids).await?;
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
                .fetch_activesync_email_states_by_ids(account_id, mailbox_id, ids)
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
                        .ok_or_else(|| anyhow!("draft add command is missing ApplicationData"))?;
                    let input = draft_input_from_application_data(
                        principal,
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
                        .fetch_jmap_draft(principal.account_id, draft_id)
                        .await?
                        .ok_or_else(|| anyhow!("draft not found"))?;
                    let application_data = command.child("ApplicationData").ok_or_else(|| {
                        anyhow!("draft change command is missing ApplicationData")
                    })?;
                    let input =
                        merged_draft_input(principal, draft_id, &existing, application_data);
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
                            principal.account_id,
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
        let from_display = parsed
            .from
            .as_ref()
            .and_then(|mailbox| mailbox.display_name.clone())
            .or_else(|| Some(principal.display_name.clone()));
        let from_address = parsed
            .from
            .map(|mailbox| mailbox.address)
            .unwrap_or_else(|| principal.email.clone());
        self.store
            .submit_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: principal.account_id,
                    source: "activesync-sendmail".to_string(),
                    from_display,
                    from_address,
                    to: parsed.to,
                    cc: parsed.cc,
                    bcc: parsed.bcc,
                    subject: parsed.subject,
                    body_text: parsed.body_text,
                    body_html_sanitized: None,
                    internet_message_id: parsed.internet_message_id,
                    mime_blob_ref: Some(format!("activesync-mime:{}", Uuid::new_v4())),
                    size_octets: mime_payload.len() as i64,
                    attachments: Vec::new(),
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

    async fn folder_collections(&self, account_id: Uuid) -> Result<Vec<CollectionDefinition>> {
        let mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        let mut collections = mailboxes
            .into_iter()
            .filter_map(|mailbox| match mailbox.role.as_str() {
                "inbox" => Some(CollectionDefinition {
                    id: mailbox.id.to_string(),
                    class_name: MAIL_CLASS.to_string(),
                    display_name: "Inbox".to_string(),
                    folder_type: "2".to_string(),
                    mailbox_id: Some(mailbox.id),
                }),
                "sent" => Some(CollectionDefinition {
                    id: mailbox.id.to_string(),
                    class_name: MAIL_CLASS.to_string(),
                    display_name: "Sent".to_string(),
                    folder_type: "5".to_string(),
                    mailbox_id: Some(mailbox.id),
                }),
                "drafts" => Some(CollectionDefinition {
                    id: mailbox.id.to_string(),
                    class_name: MAIL_CLASS.to_string(),
                    display_name: "Drafts".to_string(),
                    folder_type: "3".to_string(),
                    mailbox_id: Some(mailbox.id),
                }),
                _ => None,
            })
            .collect::<Vec<_>>();

        collections.push(CollectionDefinition {
            id: "contacts".to_string(),
            class_name: CONTACTS_CLASS.to_string(),
            display_name: "Contacts".to_string(),
            folder_type: "9".to_string(),
            mailbox_id: None,
        });
        collections.push(CollectionDefinition {
            id: "calendar".to_string(),
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
