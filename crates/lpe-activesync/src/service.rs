use anyhow::{anyhow, bail, Result};
use axum::{http::HeaderMap, response::Response};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{ActiveSyncItemState, AuditEntryInput};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

mod application_data;
mod body_preferences;
mod folders;
mod get_item_estimate;
mod item_operations;
mod mime_validation;
mod move_items;
mod ping;
mod provisioning;
mod search;
mod submission;
mod sync_helpers;

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

use crate::{
    constants::{CALENDAR_CLASS, CONTACTS_CLASS, FOLDER_SYNC_COLLECTION_ID, MAIL_CLASS},
    message::{draft_input_from_application_data, merged_draft_input},
    protocol::{ActiveSyncCommand, ActiveSyncFolderType, ActiveSyncStatus, BodyPreferenceType},
    response::{sync_status_node, wbxml_response},
    snapshot::{
        calendar_application_data, collection_window_size, contact_application_data,
        diff_collection_states, drafts_collection, email_application_data, mail_collection,
        parse_collection_mailbox_id, require_collection_id, require_sync_collections,
        BodyPreference,
    },
    store::ActiveSyncStore,
    types::{
        AuthenticatedPrincipal, CollectionDefinition, CollectionStateEntry, ParsedActiveSyncQuery,
        SnapshotChange, StoredSyncState,
    },
    wbxml::{decode_wbxml, encode_wbxml, WbxmlNode},
};

#[cfg(test)]
use crate::types::ActiveSyncQuery;

use application_data::{mail_flag_update, parse_contact_input, parse_event_input};
use body_preferences::{
    collection_body_preference, collection_deletes_as_moves, fetch_body_preference,
};
use mime_validation::validate_mime_attachments;
use provisioning::{header_policy_key, policy_required_response};
use sync_helpers::{
    completed_sync_state, decode_sync_state, has_client_commands, hierarchy_generation,
    hierarchy_generation_from_snapshot, pending_page, sync_collection_has_unsupported_command,
    sync_collection_status_node, value_to_wbxml,
};

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
