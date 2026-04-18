use anyhow::{anyhow, bail, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::Response,
    routing::{on, MethodFilter},
    Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_storage::{
    AccountLogin, ActiveSyncSyncState, AuditEntryInput, AuthenticatedAccount, ClientContact,
    ClientEvent, JmapEmail, JmapMailbox, SavedDraftMessage, Storage, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
};
use uuid::Uuid;

const ACTIVE_SYNC_PATH: &str = "/Microsoft-Server-ActiveSync";
const ACTIVE_SYNC_VERSION: &str = "16.1";
const ACTIVE_SYNC_COMMANDS: &str = "FolderSync,Provision,SendMail,Sync";
const ROOT_FOLDER_ID: &str = "0";
const FOLDER_SYNC_COLLECTION_ID: &str = "__folders__";
const MAIL_CLASS: &str = "Email";
const CONTACTS_CLASS: &str = "Contacts";
const CALENDAR_CLASS: &str = "Calendar";

type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub fn router() -> Router<Storage> {
    Router::new().route(
        ACTIVE_SYNC_PATH,
        on(MethodFilter::OPTIONS, options_handler).post(post_handler),
    )
}

#[derive(Debug, Deserialize, Default)]
struct ActiveSyncQuery {
    #[serde(rename = "Cmd")]
    cmd: Option<String>,
    #[serde(rename = "User")]
    user: Option<String>,
    #[serde(rename = "DeviceId")]
    device_id: Option<String>,
    #[serde(rename = "DeviceType")]
    _device_type: Option<String>,
}

#[derive(Debug, Clone)]
struct AuthenticatedPrincipal {
    account_id: Uuid,
    email: String,
    display_name: String,
}

#[derive(Debug, Clone)]
struct CollectionDefinition {
    id: String,
    class_name: String,
    display_name: String,
    folder_type: String,
    mailbox_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct SnapshotEntry {
    server_id: String,
    fingerprint: String,
    data: Value,
}

#[derive(Debug, Clone)]
struct WbxmlNode {
    page: u8,
    name: String,
    text: Option<String>,
    children: Vec<WbxmlNode>,
}

impl WbxmlNode {
    fn new(page: u8, name: impl Into<String>) -> Self {
        Self {
            page,
            name: name.into(),
            text: None,
            children: Vec::new(),
        }
    }

    fn with_text(page: u8, name: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            page,
            name: name.into(),
            text: Some(text.into()),
            children: Vec::new(),
        }
    }

    fn push(&mut self, child: WbxmlNode) {
        self.children.push(child);
    }

    fn child(&self, name: &str) -> Option<&WbxmlNode> {
        self.children.iter().find(|child| child.name == name)
    }

    fn text_value(&self) -> &str {
        self.text.as_deref().unwrap_or("")
    }
}

pub trait ActiveSyncStore: Clone + Send + Sync + 'static {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>>;
    fn fetch_account_login<'a>(&'a self, email: &'a str)
        -> StoreFuture<'a, Option<AccountLogin>>;
    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>;
    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, lpe_storage::JmapEmailQuery>;
    fn fetch_jmap_emails<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>>;
    fn fetch_jmap_draft<'a>(
        &'a self,
        account_id: Uuid,
        id: Uuid,
    ) -> StoreFuture<'a, Option<JmapEmail>>;
    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage>;
    fn delete_draft_message<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;
    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage>;
    fn fetch_client_contacts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ClientContact>>;
    fn fetch_client_events<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<ClientEvent>>;
    fn store_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
        snapshot: Value,
    ) -> StoreFuture<'a, ()>;
    fn fetch_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncSyncState>>;
}

impl ActiveSyncStore for Storage {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
        Box::pin(async move { self.fetch_account_session(token).await })
    }

    fn fetch_account_login<'a>(
        &'a self,
        email: &'a str,
    ) -> StoreFuture<'a, Option<AccountLogin>> {
        Box::pin(async move { self.fetch_account_login(email).await })
    }

    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        Box::pin(async move { self.fetch_jmap_mailboxes(account_id).await })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, lpe_storage::JmapEmailQuery> {
        Box::pin(async move {
            self.query_jmap_email_ids(account_id, mailbox_id, position, limit)
                .await
        })
    }

    fn fetch_jmap_emails<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        Box::pin(async move { self.fetch_jmap_emails(account_id, ids).await })
    }

    fn fetch_jmap_draft<'a>(
        &'a self,
        account_id: Uuid,
        id: Uuid,
    ) -> StoreFuture<'a, Option<JmapEmail>> {
        Box::pin(async move { self.fetch_jmap_draft(account_id, id).await })
    }

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage> {
        Box::pin(async move { self.save_draft_message(input, audit).await })
    }

    fn delete_draft_message<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_draft_message(account_id, message_id, audit).await })
    }

    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        Box::pin(async move { self.submit_message(input, audit).await })
    }

    fn fetch_client_contacts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ClientContact>> {
        Box::pin(async move { self.fetch_client_contacts(account_id).await })
    }

    fn fetch_client_events<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<ClientEvent>> {
        Box::pin(async move { self.fetch_client_events(account_id).await })
    }

    fn store_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
        snapshot: Value,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.store_activesync_sync_state(
                account_id,
                device_id,
                collection_id,
                sync_key,
                &snapshot,
            )
            .await
        })
    }

    fn fetch_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncSyncState>> {
        Box::pin(async move {
            self.fetch_activesync_sync_state(account_id, device_id, collection_id, sync_key)
                .await
        })
    }
}

async fn options_handler() -> Response {
    empty_response()
}

async fn post_handler(
    State(storage): State<Storage>,
    Query(query): Query<ActiveSyncQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let service = ActiveSyncService::new(storage);
    service
        .handle_request(query, &headers, body.as_ref())
        .await
        .map_err(http_error)
}

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
    async fn handle_request(
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
                self.handle_folder_sync(&principal, device_id, &request).await
            }
            "Sync" => {
                let request = decode_wbxml(body)?;
                self.handle_sync(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "SendMail" => self
                .handle_send_mail(&principal, &protocol_version, headers, body)
                .await,
            other => bail!("unsupported ActiveSync command: {other}"),
        }
    }

    async fn authenticate(
        &self,
        hinted_user: Option<&str>,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedPrincipal> {
        if let Some(token) = bearer_token(headers) {
            if let Some(account) = self.store.fetch_account_session(&token).await? {
                return Ok(AuthenticatedPrincipal {
                    account_id: account.account_id,
                    email: account.email,
                    display_name: account.display_name,
                });
            }
        }

        if let Some((username, password)) = basic_credentials(headers)? {
            let login = self
                .store
                .fetch_account_login(&normalize_login_name(&username, hinted_user))
                .await?
                .ok_or_else(|| anyhow!("invalid credentials"))?;
            if login.status != "active" || !verify_password(&login.password_hash, &password) {
                bail!("invalid credentials");
            }
            return Ok(AuthenticatedPrincipal {
                account_id: login.account_id,
                email: login.email,
                display_name: login.display_name,
            });
        }

        bail!("missing account authentication");
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

        let policy_key = policy_key(principal.account_id, device_id);
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

        if client_status.as_deref() == Some("1") && requested_key == policy_key {
            policy.push(WbxmlNode::with_text(14, "PolicyKey", policy_key));
        } else {
            policy.push(WbxmlNode::with_text(14, "PolicyKey", policy_key));
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
                .map(|state| state.snapshot)
        };

        if !requested_key.is_empty() && requested_key != "0" && old_snapshot.is_none() {
            let mut response = WbxmlNode::new(7, "FolderSync");
            response.push(WbxmlNode::with_text(7, "Status", "9"));
            return wbxml_response(ACTIVE_SYNC_VERSION, encode_wbxml(&response));
        }

        let new_key = Uuid::new_v4().to_string();
        self.store
            .store_activesync_sync_state(
                principal.account_id,
                device_id,
                FOLDER_SYNC_COLLECTION_ID,
                &new_key,
                snapshot.clone(),
            )
            .await?;

        let mut response = WbxmlNode::new(7, "FolderSync");
        response.push(WbxmlNode::with_text(7, "Status", "1"));
        response.push(WbxmlNode::with_text(7, "SyncKey", new_key));

        let changes = diff_snapshots(old_snapshot.as_ref(), &snapshot);
        let mut changes_node = WbxmlNode::new(7, "Changes");
        changes_node.push(WbxmlNode::with_text(
            7,
            "Count",
            changes.len().to_string(),
        ));
        for change in changes {
            match change.kind.as_str() {
                "Add" => {
                    if let Some(collection) =
                        collections.iter().find(|collection| collection.id == change.server_id)
                    {
                        let mut node = WbxmlNode::new(7, "Add");
                        node.push(WbxmlNode::with_text(7, "ServerId", &collection.id));
                        node.push(WbxmlNode::with_text(7, "ParentId", ROOT_FOLDER_ID));
                        node.push(WbxmlNode::with_text(7, "DisplayName", &collection.display_name));
                        node.push(WbxmlNode::with_text(7, "Type", &collection.folder_type));
                        changes_node.push(node);
                    }
                }
                "Update" => {
                    if let Some(collection) =
                        collections.iter().find(|collection| collection.id == change.server_id)
                    {
                        let mut node = WbxmlNode::new(7, "Update");
                        node.push(WbxmlNode::with_text(7, "ServerId", &collection.id));
                        node.push(WbxmlNode::with_text(7, "ParentId", ROOT_FOLDER_ID));
                        node.push(WbxmlNode::with_text(7, "DisplayName", &collection.display_name));
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
        wbxml_response(ACTIVE_SYNC_VERSION, encode_wbxml(&response))
    }

    async fn handle_sync(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Sync" {
            bail!("invalid Sync payload");
        }

        let collection_node = request
            .child("Collections")
            .and_then(|collections| collections.child("Collection"))
            .ok_or_else(|| anyhow!("Sync request must include one collection"))?;
        let collection_id = collection_node
            .child("CollectionId")
            .map(|node| node.text_value().trim().to_string())
            .ok_or_else(|| anyhow!("Sync collection is missing CollectionId"))?;
        let sync_key = collection_node
            .child("SyncKey")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();

        let collection = self
            .resolve_collection(principal.account_id, &collection_id)
            .await?
            .ok_or_else(|| anyhow!("unknown collection id"))?;
        let previous_snapshot = if sync_key == "0" || sync_key.is_empty() {
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
                .map(|state| state.snapshot)
        };

        if !sync_key.is_empty() && sync_key != "0" && previous_snapshot.is_none() {
            return Ok(sync_status_response(protocol_version, &collection.id, "9"));
        }

        let client_responses = if collection.class_name == MAIL_CLASS
            && collection.display_name == "Drafts"
        {
            self.apply_draft_sync_commands(principal, collection_node).await?
        } else {
            Vec::new()
        };

        let final_snapshot = self
            .collection_snapshot(principal.account_id, &collection)
            .await?;
        let next_key = Uuid::new_v4().to_string();
        self.store
            .store_activesync_sync_state(
                principal.account_id,
                device_id,
                &collection.id,
                &next_key,
                final_snapshot.clone(),
            )
            .await?;

        let changed_items = diff_snapshots(previous_snapshot.as_ref(), &final_snapshot);
        let current_items = snapshot_map(&final_snapshot);
        let mut response = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut response_collection = WbxmlNode::new(0, "Collection");
        response_collection.push(WbxmlNode::with_text(0, "Class", &collection.class_name));
        response_collection.push(WbxmlNode::with_text(0, "SyncKey", next_key));
        response_collection.push(WbxmlNode::with_text(0, "CollectionId", &collection.id));
        response_collection.push(WbxmlNode::with_text(0, "Status", "1"));

        if !client_responses.is_empty() {
            let mut responses = WbxmlNode::new(0, "Responses");
            for client_response in client_responses {
                responses.push(client_response);
            }
            response_collection.push(responses);
        }

        if !changed_items.is_empty() {
            let mut commands = WbxmlNode::new(0, "Commands");
            for change in changed_items {
                match change.kind.as_str() {
                    "Add" | "Update" => {
                        if let Some(item) = current_items.get(&change.server_id) {
                            let mut node = WbxmlNode::new(
                                0,
                                if change.kind == "Add" { "Add" } else { "Change" },
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
            response_collection.push(commands);
        }

        collections.push(response_collection);
        response.push(collections);
        wbxml_response(protocol_version, encode_wbxml(&response))
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
                    add.push(WbxmlNode::with_text(0, "ServerId", saved.message_id.to_string()));
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
                    let application_data = command
                        .child("ApplicationData")
                        .ok_or_else(|| anyhow!("draft change command is missing ApplicationData"))?;
                    let input = merged_draft_input(principal, draft_id, &existing, application_data);
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
        self.store
            .submit_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: principal.account_id,
                    source: "activesync-sendmail".to_string(),
                    from_display: Some(principal.display_name.clone()),
                    from_address: principal.email.clone(),
                    to: parsed.to,
                    cc: parsed.cc,
                    bcc: parsed.bcc,
                    subject: parsed.subject,
                    body_text: parsed.body_text,
                    body_html_sanitized: None,
                    internet_message_id: parsed.internet_message_id,
                    mime_blob_ref: Some(format!("activesync-mime:{}", Uuid::new_v4())),
                    size_octets: mime_payload.len() as i64,
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

    async fn collection_snapshot(&self, account_id: Uuid, collection: &CollectionDefinition) -> Result<Value> {
        match collection.class_name.as_str() {
            MAIL_CLASS => {
                let mailbox_id = collection.mailbox_id.ok_or_else(|| anyhow!("mailbox missing"))?;
                let ids = self
                    .store
                    .query_jmap_email_ids(account_id, Some(mailbox_id), 0, 250)
                    .await?
                    .ids;
                let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
                let entries = emails
                    .into_iter()
                    .map(|email| SnapshotEntry {
                        server_id: email.id.to_string(),
                        fingerprint: fingerprint_email(&email),
                        data: email_application_data(&email),
                    })
                    .collect::<Vec<_>>();
                Ok(snapshot_to_value(&entries))
            }
            CONTACTS_CLASS => {
                let entries = self
                    .store
                    .fetch_client_contacts(account_id)
                    .await?
                    .into_iter()
                    .map(|contact| SnapshotEntry {
                        server_id: contact.id.to_string(),
                        fingerprint: fingerprint_contact(&contact),
                        data: contact_application_data(&contact),
                    })
                    .collect::<Vec<_>>();
                Ok(snapshot_to_value(&entries))
            }
            CALENDAR_CLASS => {
                let entries = self
                    .store
                    .fetch_client_events(account_id)
                    .await?
                    .into_iter()
                    .map(|event| SnapshotEntry {
                        server_id: event.id.to_string(),
                        fingerprint: fingerprint_event(&event),
                        data: calendar_application_data(&event),
                    })
                    .collect::<Vec<_>>();
                Ok(snapshot_to_value(&entries))
            }
            _ => bail!("unsupported collection class"),
        }
    }
}

fn fingerprint_email(email: &JmapEmail) -> String {
    let recipients = |values: &[lpe_storage::JmapEmailAddress]| {
        values
            .iter()
            .map(|recipient| recipient.address.as_str())
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
        email.subject,
        email.preview,
        email.body_text,
        email.sent_at.clone().unwrap_or_else(|| email.received_at.clone()),
        email.unread,
        email.flagged,
        recipients(&email.to),
        recipients(&email.cc),
        recipients(&email.bcc),
    )
}

fn fingerprint_contact(contact: &ClientContact) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        contact.name, contact.role, contact.email, contact.phone, contact.team, contact.notes
    )
}

fn fingerprint_event(event: &ClientEvent) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        event.date, event.time, event.title, event.location, event.attendees, event.notes
    )
}

fn email_application_data(email: &JmapEmail) -> Value {
    let to = email
        .to
        .iter()
        .map(format_email_address)
        .collect::<Vec<_>>()
        .join(", ");
    let cc = email
        .cc
        .iter()
        .map(format_email_address)
        .collect::<Vec<_>>()
        .join(", ");

    json!({
        "page": 0,
        "name": "ApplicationData",
        "children": [
            {"page": 2, "name": "Subject", "text": email.subject},
            {"page": 2, "name": "From", "text": email.from_display.as_deref().map(|display| format!("{display} <{}>", email.from_address)).unwrap_or_else(|| email.from_address.clone())},
            {"page": 2, "name": "To", "text": to},
            {"page": 2, "name": "Cc", "text": cc},
            {"page": 2, "name": "DisplayTo", "text": to},
            {"page": 2, "name": "Read", "text": if email.unread { "0" } else { "1" }},
            {"page": 2, "name": "Importance", "text": "1"},
            {"page": 2, "name": "MessageClass", "text": "IPM.Note"},
            {"page": 2, "name": "DateReceived", "text": activesync_timestamp(email.sent_at.as_deref().unwrap_or(&email.received_at))},
            {
                "page": 17,
                "name": "Body",
                "children": [
                    {"page": 17, "name": "Type", "text": "1"},
                    {"page": 17, "name": "EstimatedDataSize", "text": email.body_text.len().to_string()},
                    {"page": 17, "name": "Data", "text": email.body_text},
                    {"page": 17, "name": "Truncated", "text": "0"}
                ]
            }
        ]
    })
}

fn contact_application_data(contact: &ClientContact) -> Value {
    let (first_name, last_name) = split_name(&contact.name);
    json!({
        "page": 0,
        "name": "ApplicationData",
        "children": [
            {"page": 1, "name": "FileAs", "text": contact.name},
            {"page": 1, "name": "FirstName", "text": first_name},
            {"page": 1, "name": "LastName", "text": last_name},
            {"page": 1, "name": "Email1Address", "text": contact.email},
            {"page": 1, "name": "MobilePhoneNumber", "text": contact.phone},
            {"page": 1, "name": "HomePhoneNumber", "text": contact.phone}
        ]
    })
}

fn calendar_application_data(event: &ClientEvent) -> Value {
    json!({
        "page": 0,
        "name": "ApplicationData",
        "children": [
            {"page": 4, "name": "Subject", "text": event.title},
            {"page": 4, "name": "StartTime", "text": format!("{}T{}:00Z", event.date.replace('-', ""), event.time.replace(':', ""))},
            {"page": 4, "name": "EndTime", "text": format!("{}T{}:00Z", event.date.replace('-', ""), event.time.replace(':', ""))},
            {"page": 4, "name": "Location", "text": event.location},
            {"page": 4, "name": "OrganizerName", "text": event.attendees},
            {"page": 4, "name": "OrganizerEmail", "text": ""},
            {
                "page": 17,
                "name": "Body",
                "children": [
                    {"page": 17, "name": "Type", "text": "1"},
                    {"page": 17, "name": "EstimatedDataSize", "text": event.notes.len().to_string()},
                    {"page": 17, "name": "Data", "text": event.notes},
                    {"page": 17, "name": "Truncated", "text": "0"}
                ]
            }
        ]
    })
}

fn snapshot_to_value(entries: &[SnapshotEntry]) -> Value {
    Value::Array(
        entries
            .iter()
            .map(|entry| {
                json!({
                    "id": entry.server_id,
                    "fingerprint": entry.fingerprint,
                    "data": entry.data,
                })
            })
            .collect(),
    )
}

fn snapshot_map(snapshot: &Value) -> HashMap<String, WbxmlNode> {
    let mut map = HashMap::new();
    if let Value::Array(entries) = snapshot {
        for entry in entries {
            let Some(id) = entry.get("id").and_then(Value::as_str) else {
                continue;
            };
            if let Some(data) = entry.get("data").and_then(Value::as_object) {
                map.insert(id.to_string(), value_to_node(data));
            }
        }
    }
    map
}

#[derive(Debug)]
struct SnapshotChange {
    kind: String,
    server_id: String,
}

fn diff_snapshots(previous: Option<&Value>, current: &Value) -> Vec<SnapshotChange> {
    let previous_map = snapshot_fingerprints(previous);
    let current_map = snapshot_fingerprints(Some(current));
    let mut changes = Vec::new();

    for (server_id, fingerprint) in &current_map {
        match previous_map.get(server_id) {
            None => changes.push(SnapshotChange {
                kind: "Add".to_string(),
                server_id: server_id.clone(),
            }),
            Some(previous_fingerprint) if previous_fingerprint != fingerprint => {
                changes.push(SnapshotChange {
                    kind: "Update".to_string(),
                    server_id: server_id.clone(),
                });
            }
            _ => {}
        }
    }

    for server_id in previous_map.keys() {
        if !current_map.contains_key(server_id) {
            changes.push(SnapshotChange {
                kind: "Delete".to_string(),
                server_id: server_id.clone(),
            });
        }
    }

    changes
}

fn snapshot_fingerprints(snapshot: Option<&Value>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let Some(Value::Array(entries)) = snapshot else {
        return map;
    };

    for entry in entries {
        let Some(id) = entry.get("id").and_then(Value::as_str) else {
            continue;
        };
        let Some(fingerprint) = entry.get("fingerprint").and_then(Value::as_str) else {
            continue;
        };
        map.insert(id.to_string(), fingerprint.to_string());
    }

    map
}

fn protocol_version(headers: &HeaderMap) -> String {
    headers
        .get("ms-asprotocolversion")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(ACTIVE_SYNC_VERSION)
        .to_string()
}

fn normalize_login_name(username: &str, hinted_user: Option<&str>) -> String {
    if username.contains('@') {
        username.trim().to_lowercase()
    } else {
        hinted_user
            .unwrap_or(username)
            .trim()
            .to_lowercase()
    }
}

fn verify_password(password_hash: &str, password: &str) -> bool {
    PasswordHash::new(password_hash)
        .ok()
        .and_then(|parsed| {
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .ok()
        })
        .is_some()
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn basic_credentials(headers: &HeaderMap) -> Result<Option<(String, String)>> {
    let Some(value) = headers.get("authorization").and_then(|value| value.to_str().ok()) else {
        return Ok(None);
    };
    let Some(encoded) = value.strip_prefix("Basic ") else {
        return Ok(None);
    };
    let decoded = BASE64.decode(encoded.trim())?;
    let decoded = String::from_utf8(decoded)?;
    let (username, password) = decoded
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid basic authorization header"))?;
    Ok(Some((username.to_string(), password.to_string())))
}

fn empty_response() -> Response {
    let mut response = Response::new(axum::body::Body::empty());
    *response.status_mut() = StatusCode::OK;
    add_common_headers(response.headers_mut());
    response
}

fn wbxml_response(protocol_version: &str, body: Vec<u8>) -> Result<Response> {
    let mut response = Response::new(axum::body::Body::from(body));
    *response.status_mut() = StatusCode::OK;
    add_common_headers(response.headers_mut());
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/vnd.ms-sync.wbxml"),
    );
    response.headers_mut().insert(
        "ms-asprotocolversion",
        HeaderValue::from_str(protocol_version)?,
    );
    Ok(response)
}

fn add_common_headers(headers: &mut HeaderMap) {
    headers.insert("allow", HeaderValue::from_static("OPTIONS, POST"));
    headers.insert(
        "ms-asprotocolversions",
        HeaderValue::from_static(ACTIVE_SYNC_VERSION),
    );
    headers.insert(
        "ms-asprotocolcommands",
        HeaderValue::from_static(ACTIVE_SYNC_COMMANDS),
    );
    headers.insert("public", HeaderValue::from_static("OPTIONS, POST"));
    headers.insert("dav", HeaderValue::from_static("1,2"));
}

fn http_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, error.to_string())
}

fn sync_status_response(protocol_version: &str, collection_id: &str, status: &str) -> Response {
    let mut sync = WbxmlNode::new(0, "Sync");
    let mut collections = WbxmlNode::new(0, "Collections");
    let mut collection = WbxmlNode::new(0, "Collection");
    collection.push(WbxmlNode::with_text(0, "CollectionId", collection_id));
    collection.push(WbxmlNode::with_text(0, "Status", status));
    collections.push(collection);
    sync.push(collections);
    wbxml_response(protocol_version, encode_wbxml(&sync)).unwrap_or_else(|_| empty_response())
}

fn policy_key(account_id: Uuid, device_id: &str) -> String {
    let seed = format!("{}:{}", account_id, device_id);
    let mut value: u32 = 0;
    for byte in seed.bytes() {
        value = value.wrapping_mul(33).wrapping_add(byte as u32);
    }
    value.max(1).to_string()
}

fn is_message_rfc822(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().starts_with("message/rfc822"))
        .unwrap_or(false)
}

struct ParsedMimeMessage {
    to: Vec<SubmittedRecipientInput>,
    cc: Vec<SubmittedRecipientInput>,
    bcc: Vec<SubmittedRecipientInput>,
    subject: String,
    body_text: String,
    internet_message_id: Option<String>,
}

fn parse_mime_message(bytes: &[u8]) -> Result<ParsedMimeMessage> {
    let raw = String::from_utf8_lossy(bytes);
    let (header_block, body_block) = raw
        .split_once("\r\n\r\n")
        .or_else(|| raw.split_once("\n\n"))
        .unwrap_or((raw.as_ref(), ""));
    let headers = parse_rfc822_headers(header_block);
    Ok(ParsedMimeMessage {
        to: parse_address_list(headers.get("to").map(String::as_str).unwrap_or("")),
        cc: parse_address_list(headers.get("cc").map(String::as_str).unwrap_or("")),
        bcc: parse_address_list(headers.get("bcc").map(String::as_str).unwrap_or("")),
        subject: headers
            .get("subject")
            .map(|value| value.trim().to_string())
            .unwrap_or_default(),
        body_text: body_block.trim().to_string(),
        internet_message_id: headers
            .get("message-id")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
    })
}

fn parse_rfc822_headers(block: &str) -> HashMap<String, String> {
    let mut headers: HashMap<String, String> = HashMap::new();
    let mut current_name = String::new();
    for line in block.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(value) = headers.get_mut(&current_name) {
                value.push(' ');
                value.push_str(line.trim());
            }
            continue;
        }

        if let Some((name, value)) = line.split_once(':') {
            current_name = name.trim().to_lowercase();
            headers.insert(current_name.clone(), value.trim().to_string());
        }
    }
    headers
}

fn parse_address_list(value: &str) -> Vec<SubmittedRecipientInput> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            if let (Some(start), Some(end)) = (entry.find('<'), entry.find('>')) {
                SubmittedRecipientInput {
                    address: entry[start + 1..end].trim().to_string(),
                    display_name: Some(entry[..start].trim_matches('"').trim().to_string())
                        .filter(|name| !name.is_empty()),
                }
            } else {
                SubmittedRecipientInput {
                    address: entry.to_string(),
                    display_name: None,
                }
            }
        })
        .collect()
}

fn format_email_address(address: &lpe_storage::JmapEmailAddress) -> String {
    address
        .display_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .map(|name| format!("{name} <{}>", address.address))
        .unwrap_or_else(|| address.address.clone())
}

fn split_name(value: &str) -> (String, String) {
    let trimmed = value.trim();
    if let Some((first, last)) = trimmed.split_once(' ') {
        (first.trim().to_string(), last.trim().to_string())
    } else {
        (trimmed.to_string(), String::new())
    }
}

fn activesync_timestamp(value: &str) -> String {
    value
        .replace('-', "")
        .replace(':', "")
        .replace("\"", "")
}

fn merged_draft_input(
    principal: &AuthenticatedPrincipal,
    draft_id: Uuid,
    existing: &JmapEmail,
    application_data: &WbxmlNode,
) -> SubmitMessageInput {
    SubmitMessageInput {
        draft_message_id: Some(draft_id),
        account_id: principal.account_id,
        source: "activesync-sync-change".to_string(),
        from_display: Some(principal.display_name.clone()),
        from_address: field_text(application_data, "From").unwrap_or_else(|| existing.from_address.clone()),
        to: field_text(application_data, "To")
            .map(|value| parse_address_list(&value))
            .unwrap_or_else(|| {
                existing
                    .to
                    .iter()
                    .map(|recipient| SubmittedRecipientInput {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect()
            }),
        cc: field_text(application_data, "Cc")
            .map(|value| parse_address_list(&value))
            .unwrap_or_else(|| {
                existing
                    .cc
                    .iter()
                    .map(|recipient| SubmittedRecipientInput {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect()
            }),
        bcc: existing
            .bcc
            .iter()
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect(),
        subject: field_text(application_data, "Subject").unwrap_or_else(|| existing.subject.clone()),
        body_text: application_data
            .child("Body")
            .and_then(|body| body.child("Data"))
            .map(|node| node.text_value().to_string())
            .unwrap_or_else(|| existing.body_text.clone()),
        body_html_sanitized: existing.body_html_sanitized.clone(),
        internet_message_id: existing.internet_message_id.clone(),
        mime_blob_ref: Some(format!("draft-message:{draft_id}")),
        size_octets: existing.size_octets,
    }
}

fn draft_input_from_application_data(
    principal: &AuthenticatedPrincipal,
    draft_message_id: Option<Uuid>,
    application_data: &WbxmlNode,
    source: &str,
) -> SubmitMessageInput {
    SubmitMessageInput {
        draft_message_id,
        account_id: principal.account_id,
        source: source.to_string(),
        from_display: Some(principal.display_name.clone()),
        from_address: field_text(application_data, "From").unwrap_or_else(|| principal.email.clone()),
        to: field_text(application_data, "To")
            .map(|value| parse_address_list(&value))
            .unwrap_or_default(),
        cc: field_text(application_data, "Cc")
            .map(|value| parse_address_list(&value))
            .unwrap_or_default(),
        bcc: Vec::new(),
        subject: field_text(application_data, "Subject").unwrap_or_default(),
        body_text: application_data
            .child("Body")
            .and_then(|body| body.child("Data"))
            .map(|node| node.text_value().to_string())
            .unwrap_or_default(),
        body_html_sanitized: None,
        internet_message_id: None,
        mime_blob_ref: None,
        size_octets: 0,
    }
}

fn field_text(node: &WbxmlNode, name: &str) -> Option<String> {
    node.child(name)
        .map(|child| child.text_value().trim().to_string())
        .filter(|value| !value.is_empty())
}

fn value_to_node(data: &serde_json::Map<String, Value>) -> WbxmlNode {
    let page = data
        .get("page")
        .and_then(Value::as_u64)
        .unwrap_or(0) as u8;
    let name = data
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("ApplicationData");
    let mut node = WbxmlNode::new(page, name);
    node.text = data.get("text").and_then(Value::as_str).map(ToString::to_string);
    if let Some(Value::Array(children)) = data.get("children") {
        for child in children {
            if let Some(object) = child.as_object() {
                node.push(value_to_node(object));
            }
        }
    }
    node
}

fn encode_wbxml(root: &WbxmlNode) -> Vec<u8> {
    let mut out = vec![0x03, 0x01, 0x6A, 0x00];
    let mut page = 0u8;
    encode_node(root, &mut page, &mut out);
    out
}

fn encode_node(node: &WbxmlNode, current_page: &mut u8, out: &mut Vec<u8>) {
    if node.page != *current_page {
        out.push(0x00);
        out.push(node.page);
        *current_page = node.page;
    }

    let token = token_for(node.page, &node.name).unwrap_or(0x05);
    let has_content = node.text.is_some() || !node.children.is_empty();
    out.push(if has_content { token | 0x40 } else { token });

    if let Some(text) = &node.text {
        out.push(0x03);
        out.extend_from_slice(text.as_bytes());
        out.push(0x00);
    }

    for child in &node.children {
        encode_node(child, current_page, out);
    }

    if has_content {
        out.push(0x01);
    }
}

fn decode_wbxml(bytes: &[u8]) -> Result<WbxmlNode> {
    let mut cursor = 0usize;
    if bytes.len() < 4 {
        bail!("WBXML payload is too short");
    }
    cursor += 1; // version
    let _ = read_multibyte_int(bytes, &mut cursor)?;
    let charset = read_multibyte_int(bytes, &mut cursor)?;
    if charset != 0x6A {
        bail!("unsupported WBXML charset");
    }
    let string_table_length = read_multibyte_int(bytes, &mut cursor)?;
    if string_table_length != 0 {
        bail!("WBXML string tables are not supported");
    }

    let mut current_page = 0u8;
    parse_node(bytes, &mut cursor, &mut current_page)
}

fn parse_node(bytes: &[u8], cursor: &mut usize, current_page: &mut u8) -> Result<WbxmlNode> {
    while *cursor < bytes.len() && bytes[*cursor] == 0x00 {
        *cursor += 1;
        *current_page = *bytes
            .get(*cursor)
            .ok_or_else(|| anyhow!("missing WBXML code page"))?;
        *cursor += 1;
    }

    let token = *bytes
        .get(*cursor)
        .ok_or_else(|| anyhow!("missing WBXML token"))?;
    *cursor += 1;
    if token == 0x01 {
        bail!("unexpected WBXML end token");
    }

    let has_attributes = token & 0x80 != 0;
    if has_attributes {
        bail!("WBXML attributes are not supported");
    }
    let has_content = token & 0x40 != 0;
    let name = name_for(*current_page, token & 0x3F).ok_or_else(|| anyhow!("unknown WBXML token"))?;
    let mut node = WbxmlNode::new(*current_page, name);

    if has_content {
        let mut text = String::new();
        while *cursor < bytes.len() {
            match bytes[*cursor] {
                0x00 => {
                    *cursor += 1;
                    *current_page = *bytes
                        .get(*cursor)
                        .ok_or_else(|| anyhow!("missing WBXML code page"))?;
                    *cursor += 1;
                }
                0x01 => {
                    *cursor += 1;
                    break;
                }
                0x03 => {
                    *cursor += 1;
                    text.push_str(&read_inline_string(bytes, cursor)?);
                }
                0xC3 => {
                    *cursor += 1;
                    let length = read_multibyte_int(bytes, cursor)? as usize;
                    let chunk = bytes
                        .get(*cursor..*cursor + length)
                        .ok_or_else(|| anyhow!("invalid WBXML opaque block"))?;
                    text.push_str(&String::from_utf8_lossy(chunk));
                    *cursor += length;
                }
                _ => node.children.push(parse_node(bytes, cursor, current_page)?),
            }
        }
        if !text.is_empty() {
            node.text = Some(text);
        }
    }

    Ok(node)
}

fn read_multibyte_int(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let mut value = 0u32;
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| anyhow!("unexpected end of WBXML payload"))?;
        *cursor += 1;
        value = (value << 7) | (byte & 0x7F) as u32;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
}

fn read_inline_string(bytes: &[u8], cursor: &mut usize) -> Result<String> {
    let start = *cursor;
    while *cursor < bytes.len() && bytes[*cursor] != 0x00 {
        *cursor += 1;
    }
    let value = String::from_utf8(bytes[start..*cursor].to_vec())?;
    *cursor += 1;
    Ok(value)
}

fn token_for(page: u8, name: &str) -> Option<u8> {
    match (page, name) {
        (0, "Sync") => Some(0x05),
        (0, "Responses") => Some(0x06),
        (0, "Add") => Some(0x07),
        (0, "Change") => Some(0x08),
        (0, "Delete") => Some(0x09),
        (0, "SyncKey") => Some(0x0B),
        (0, "ClientId") => Some(0x0C),
        (0, "ServerId") => Some(0x0D),
        (0, "Status") => Some(0x0E),
        (0, "Collection") => Some(0x0F),
        (0, "Class") => Some(0x10),
        (0, "CollectionId") => Some(0x12),
        (0, "Commands") => Some(0x16),
        (0, "Collections") => Some(0x1C),
        (0, "ApplicationData") => Some(0x1D),
        (1, "Email1Address") => Some(0x1B),
        (1, "FileAs") => Some(0x1E),
        (1, "FirstName") => Some(0x1F),
        (1, "HomePhoneNumber") => Some(0x27),
        (1, "LastName") => Some(0x29),
        (1, "MobilePhoneNumber") => Some(0x2B),
        (2, "DateReceived") => Some(0x0F),
        (2, "DisplayTo") => Some(0x11),
        (2, "Importance") => Some(0x12),
        (2, "MessageClass") => Some(0x13),
        (2, "Subject") => Some(0x14),
        (2, "Read") => Some(0x15),
        (2, "To") => Some(0x16),
        (2, "Cc") => Some(0x17),
        (2, "From") => Some(0x18),
        (4, "Attendees") => Some(0x07),
        (4, "Attendee") => Some(0x08),
        (4, "Email") => Some(0x09),
        (4, "Name") => Some(0x0A),
        (4, "EndTime") => Some(0x12),
        (4, "Location") => Some(0x17),
        (4, "OrganizerEmail") => Some(0x19),
        (4, "OrganizerName") => Some(0x1A),
        (4, "Reminder") => Some(0x24),
        (4, "Subject") => Some(0x26),
        (4, "StartTime") => Some(0x27),
        (7, "DisplayName") => Some(0x07),
        (7, "ServerId") => Some(0x08),
        (7, "ParentId") => Some(0x09),
        (7, "Type") => Some(0x0A),
        (7, "Status") => Some(0x0C),
        (7, "Changes") => Some(0x0E),
        (7, "Add") => Some(0x0F),
        (7, "Delete") => Some(0x10),
        (7, "Update") => Some(0x11),
        (7, "SyncKey") => Some(0x12),
        (7, "FolderSync") => Some(0x16),
        (7, "Count") => Some(0x17),
        (14, "Provision") => Some(0x05),
        (14, "Policies") => Some(0x06),
        (14, "Policy") => Some(0x07),
        (14, "PolicyType") => Some(0x08),
        (14, "PolicyKey") => Some(0x09),
        (14, "Data") => Some(0x0A),
        (14, "Status") => Some(0x0B),
        (14, "EASProvisionDoc") => Some(0x0D),
        (14, "DevicePasswordEnabled") => Some(0x0E),
        (14, "AlphanumericDevicePasswordRequired") => Some(0x0F),
        (14, "AttachmentsEnabled") => Some(0x13),
        (14, "MinDevicePasswordLength") => Some(0x14),
        (14, "AllowSimpleDevicePassword") => Some(0x18),
        (14, "AllowStorageCard") => Some(0x1B),
        (14, "AllowCamera") => Some(0x1C),
        (14, "RequireDeviceEncryption") => Some(0x1D),
        (14, "AllowWiFi") => Some(0x21),
        (14, "AllowTextMessaging") => Some(0x22),
        (14, "AllowPOPIMAPEmail") => Some(0x23),
        (14, "AllowBrowser") => Some(0x33),
        (14, "AllowConsumerEmail") => Some(0x34),
        (17, "Type") => Some(0x06),
        (17, "Body") => Some(0x0A),
        (17, "Data") => Some(0x0B),
        (17, "EstimatedDataSize") => Some(0x0C),
        (17, "Truncated") => Some(0x0D),
        (18, "Status") => Some(0x06),
        (18, "Set") => Some(0x08),
        (18, "DeviceInformation") => Some(0x16),
        (18, "Model") => Some(0x17),
        (18, "IMEI") => Some(0x18),
        (18, "FriendlyName") => Some(0x19),
        (18, "OS") => Some(0x1A),
        (18, "OSLanguage") => Some(0x1B),
        (18, "PhoneNumber") => Some(0x1C),
        (18, "UserAgent") => Some(0x20),
        (18, "MobileOperator") => Some(0x22),
        (21, "SendMail") => Some(0x05),
        (21, "SaveInSentItems") => Some(0x08),
        (21, "Mime") => Some(0x10),
        (21, "ClientId") => Some(0x11),
        (21, "Status") => Some(0x12),
        _ => None,
    }
}

fn name_for(page: u8, token: u8) -> Option<&'static str> {
    match (page, token) {
        (0, 0x05) => Some("Sync"),
        (0, 0x06) => Some("Responses"),
        (0, 0x07) => Some("Add"),
        (0, 0x08) => Some("Change"),
        (0, 0x09) => Some("Delete"),
        (0, 0x0B) => Some("SyncKey"),
        (0, 0x0C) => Some("ClientId"),
        (0, 0x0D) => Some("ServerId"),
        (0, 0x0E) => Some("Status"),
        (0, 0x0F) => Some("Collection"),
        (0, 0x10) => Some("Class"),
        (0, 0x12) => Some("CollectionId"),
        (0, 0x16) => Some("Commands"),
        (0, 0x1C) => Some("Collections"),
        (0, 0x1D) => Some("ApplicationData"),
        (1, 0x1B) => Some("Email1Address"),
        (1, 0x1E) => Some("FileAs"),
        (1, 0x1F) => Some("FirstName"),
        (1, 0x27) => Some("HomePhoneNumber"),
        (1, 0x29) => Some("LastName"),
        (1, 0x2B) => Some("MobilePhoneNumber"),
        (2, 0x0F) => Some("DateReceived"),
        (2, 0x11) => Some("DisplayTo"),
        (2, 0x12) => Some("Importance"),
        (2, 0x13) => Some("MessageClass"),
        (2, 0x14) => Some("Subject"),
        (2, 0x15) => Some("Read"),
        (2, 0x16) => Some("To"),
        (2, 0x17) => Some("Cc"),
        (2, 0x18) => Some("From"),
        (4, 0x07) => Some("Attendees"),
        (4, 0x08) => Some("Attendee"),
        (4, 0x09) => Some("Email"),
        (4, 0x0A) => Some("Name"),
        (4, 0x12) => Some("EndTime"),
        (4, 0x17) => Some("Location"),
        (4, 0x19) => Some("OrganizerEmail"),
        (4, 0x1A) => Some("OrganizerName"),
        (4, 0x24) => Some("Reminder"),
        (4, 0x26) => Some("Subject"),
        (4, 0x27) => Some("StartTime"),
        (7, 0x07) => Some("DisplayName"),
        (7, 0x08) => Some("ServerId"),
        (7, 0x09) => Some("ParentId"),
        (7, 0x0A) => Some("Type"),
        (7, 0x0C) => Some("Status"),
        (7, 0x0E) => Some("Changes"),
        (7, 0x0F) => Some("Add"),
        (7, 0x10) => Some("Delete"),
        (7, 0x11) => Some("Update"),
        (7, 0x12) => Some("SyncKey"),
        (7, 0x16) => Some("FolderSync"),
        (7, 0x17) => Some("Count"),
        (14, 0x05) => Some("Provision"),
        (14, 0x06) => Some("Policies"),
        (14, 0x07) => Some("Policy"),
        (14, 0x08) => Some("PolicyType"),
        (14, 0x09) => Some("PolicyKey"),
        (14, 0x0A) => Some("Data"),
        (14, 0x0B) => Some("Status"),
        (14, 0x0D) => Some("EASProvisionDoc"),
        (14, 0x0E) => Some("DevicePasswordEnabled"),
        (14, 0x0F) => Some("AlphanumericDevicePasswordRequired"),
        (14, 0x13) => Some("AttachmentsEnabled"),
        (14, 0x14) => Some("MinDevicePasswordLength"),
        (14, 0x18) => Some("AllowSimpleDevicePassword"),
        (14, 0x1B) => Some("AllowStorageCard"),
        (14, 0x1C) => Some("AllowCamera"),
        (14, 0x1D) => Some("RequireDeviceEncryption"),
        (14, 0x21) => Some("AllowWiFi"),
        (14, 0x22) => Some("AllowTextMessaging"),
        (14, 0x23) => Some("AllowPOPIMAPEmail"),
        (14, 0x33) => Some("AllowBrowser"),
        (14, 0x34) => Some("AllowConsumerEmail"),
        (17, 0x06) => Some("Type"),
        (17, 0x0A) => Some("Body"),
        (17, 0x0B) => Some("Data"),
        (17, 0x0C) => Some("EstimatedDataSize"),
        (17, 0x0D) => Some("Truncated"),
        (18, 0x06) => Some("Status"),
        (18, 0x08) => Some("Set"),
        (18, 0x16) => Some("DeviceInformation"),
        (18, 0x17) => Some("Model"),
        (18, 0x18) => Some("IMEI"),
        (18, 0x19) => Some("FriendlyName"),
        (18, 0x1A) => Some("OS"),
        (18, 0x1B) => Some("OSLanguage"),
        (18, 0x1C) => Some("PhoneNumber"),
        (18, 0x20) => Some("UserAgent"),
        (18, 0x22) => Some("MobileOperator"),
        (21, 0x05) => Some("SendMail"),
        (21, 0x08) => Some("SaveInSentItems"),
        (21, 0x10) => Some("Mime"),
        (21, 0x11) => Some("ClientId"),
        (21, 0x12) => Some("Status"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lpe_storage::JmapEmailQuery;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct FakeStore {
        session: Option<AuthenticatedAccount>,
        login: Option<AccountLogin>,
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        contacts: Vec<ClientContact>,
        events: Vec<ClientEvent>,
        saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
        submitted_messages: Arc<Mutex<Vec<SubmitMessageInput>>>,
        sync_states: Arc<Mutex<HashMap<String, ActiveSyncSyncState>>>,
    }

    impl FakeStore {
        fn account() -> AuthenticatedAccount {
            AuthenticatedAccount {
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                email: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                expires_at: "2026-04-18T10:00:00Z".to_string(),
            }
        }

        fn draft_mailbox() -> JmapMailbox {
            JmapMailbox {
                id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                role: "drafts".to_string(),
                name: "Drafts".to_string(),
                sort_order: 10,
                total_emails: 1,
                unread_emails: 0,
            }
        }

        fn inbox_mailbox() -> JmapMailbox {
            JmapMailbox {
                id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
                role: "inbox".to_string(),
                name: "Inbox".to_string(),
                sort_order: 1,
                total_emails: 1,
                unread_emails: 1,
            }
        }

    }

    impl ActiveSyncStore for FakeStore {
        fn fetch_account_session<'a>(
            &'a self,
            token: &'a str,
        ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
            let session = if token == "token" {
                self.session.clone()
            } else {
                None
            };
            Box::pin(async move { Ok(session) })
        }

        fn fetch_account_login<'a>(
            &'a self,
            _email: &'a str,
        ) -> StoreFuture<'a, Option<AccountLogin>> {
            let login = self.login.clone();
            Box::pin(async move { Ok(login) })
        }

        fn fetch_jmap_mailboxes<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
            let mailboxes = self.mailboxes.clone();
            Box::pin(async move { Ok(mailboxes) })
        }

        fn query_jmap_email_ids<'a>(
            &'a self,
            _account_id: Uuid,
            mailbox_id: Option<Uuid>,
            _position: u64,
            _limit: u64,
        ) -> StoreFuture<'a, JmapEmailQuery> {
            let ids = self
                .emails
                .iter()
                .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
                .map(|email| email.id)
                .collect::<Vec<_>>();
            Box::pin(async move {
                Ok(JmapEmailQuery {
                    total: ids.len() as u64,
                    ids,
                })
            })
        }

        fn fetch_jmap_emails<'a>(
            &'a self,
            _account_id: Uuid,
            ids: &'a [Uuid],
        ) -> StoreFuture<'a, Vec<JmapEmail>> {
            let emails = self
                .emails
                .iter()
                .filter(|email| ids.contains(&email.id))
                .cloned()
                .collect::<Vec<_>>();
            Box::pin(async move { Ok(emails) })
        }

        fn fetch_jmap_draft<'a>(
            &'a self,
            _account_id: Uuid,
            id: Uuid,
        ) -> StoreFuture<'a, Option<JmapEmail>> {
            let email = self.emails.iter().find(|email| email.id == id).cloned();
            Box::pin(async move { Ok(email) })
        }

        fn save_draft_message<'a>(
            &'a self,
            input: SubmitMessageInput,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, SavedDraftMessage> {
            self.saved_drafts.lock().unwrap().push(input.clone());
            Box::pin(async move {
                Ok(SavedDraftMessage {
                    message_id: input
                        .draft_message_id
                        .unwrap_or_else(|| Uuid::parse_str("10101010-1010-1010-1010-101010101010").unwrap()),
                    account_id: input.account_id,
                    draft_mailbox_id: FakeStore::draft_mailbox().id,
                    delivery_status: "draft".to_string(),
                })
            })
        }

        fn delete_draft_message<'a>(
            &'a self,
            _account_id: Uuid,
            _message_id: Uuid,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }

        fn submit_message<'a>(
            &'a self,
            input: SubmitMessageInput,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, SubmittedMessage> {
            self.submitted_messages.lock().unwrap().push(input.clone());
            Box::pin(async move {
                Ok(SubmittedMessage {
                    message_id: Uuid::new_v4(),
                    thread_id: Uuid::new_v4(),
                    account_id: input.account_id,
                    sent_mailbox_id: Uuid::new_v4(),
                    outbound_queue_id: Uuid::new_v4(),
                    delivery_status: "queued".to_string(),
                })
            })
        }

        fn fetch_client_contacts<'a>(
            &'a self,
            _account_id: Uuid,
        ) -> StoreFuture<'a, Vec<ClientContact>> {
            let contacts = self.contacts.clone();
            Box::pin(async move { Ok(contacts) })
        }

        fn fetch_client_events<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<ClientEvent>> {
            let events = self.events.clone();
            Box::pin(async move { Ok(events) })
        }

        fn store_activesync_sync_state<'a>(
            &'a self,
            account_id: Uuid,
            device_id: &'a str,
            collection_id: &'a str,
            sync_key: &'a str,
            snapshot: Value,
        ) -> StoreFuture<'a, ()> {
            let key = format!("{account_id}:{device_id}:{collection_id}:{sync_key}");
            self.sync_states.lock().unwrap().insert(
                key,
                ActiveSyncSyncState {
                    sync_key: sync_key.to_string(),
                    snapshot,
                },
            );
            Box::pin(async move { Ok(()) })
        }

        fn fetch_activesync_sync_state<'a>(
            &'a self,
            account_id: Uuid,
            device_id: &'a str,
            collection_id: &'a str,
            sync_key: &'a str,
        ) -> StoreFuture<'a, Option<ActiveSyncSyncState>> {
            let key = format!("{account_id}:{device_id}:{collection_id}:{sync_key}");
            let state = self.sync_states.lock().unwrap().get(&key).cloned();
            Box::pin(async move { Ok(state) })
        }
    }

    fn bearer_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer token"),
        );
        headers
    }

    fn mime_headers() -> HeaderMap {
        let mut headers = bearer_headers();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("message/rfc822"),
        );
        headers
    }

    #[test]
    fn wbxml_roundtrip_preserves_tokens_and_text() {
        let mut root = WbxmlNode::new(7, "FolderSync");
        root.push(WbxmlNode::with_text(7, "SyncKey", "1"));
        let bytes = encode_wbxml(&root);
        let decoded = decode_wbxml(&bytes).unwrap();

        assert_eq!(decoded.name, "FolderSync");
        assert_eq!(decoded.child("SyncKey").unwrap().text_value(), "1");
    }

    #[tokio::test]
    async fn folder_sync_returns_mail_and_collaboration_collections() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::inbox_mailbox(), FakeStore::draft_mailbox()],
            ..Default::default()
        };
        let service = ActiveSyncService::new(store);
        let request = encode_wbxml(&{
            let mut node = WbxmlNode::new(7, "FolderSync");
            node.push(WbxmlNode::with_text(7, "SyncKey", "0"));
            node
        });

        let response = service
            .handle_request(
                ActiveSyncQuery {
                    cmd: Some("FolderSync".to_string()),
                    user: Some("alice@example.test".to_string()),
                    device_id: Some("dev1".to_string()),
                    _device_type: Some("phone".to_string()),
                },
                &bearer_headers(),
                &request,
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn sync_add_command_saves_draft_through_canonical_storage() {
        let draft_mailbox = FakeStore::draft_mailbox();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![draft_mailbox.clone()],
            ..Default::default()
        };
        let service = ActiveSyncService::new(store.clone());

        let request = encode_wbxml(&{
            let mut sync = WbxmlNode::new(0, "Sync");
            let mut collections = WbxmlNode::new(0, "Collections");
            let mut collection = WbxmlNode::new(0, "Collection");
            collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
            collection.push(WbxmlNode::with_text(0, "CollectionId", draft_mailbox.id.to_string()));
            let mut commands = WbxmlNode::new(0, "Commands");
            let mut add = WbxmlNode::new(0, "Add");
            add.push(WbxmlNode::with_text(0, "ClientId", "c1"));
            let mut app_data = WbxmlNode::new(0, "ApplicationData");
            app_data.push(WbxmlNode::with_text(2, "To", "bob@example.test"));
            app_data.push(WbxmlNode::with_text(2, "Subject", "Draft"));
            let mut body = WbxmlNode::new(17, "Body");
            body.push(WbxmlNode::with_text(17, "Data", "Draft body"));
            app_data.push(body);
            add.push(app_data);
            commands.push(add);
            collection.push(commands);
            collections.push(collection);
            sync.push(collections);
            sync
        });

        service
            .handle_request(
                ActiveSyncQuery {
                    cmd: Some("Sync".to_string()),
                    user: Some("alice@example.test".to_string()),
                    device_id: Some("dev1".to_string()),
                    _device_type: Some("phone".to_string()),
                },
                &bearer_headers(),
                &request,
            )
            .await
            .unwrap();

        let saved = store.saved_drafts.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].subject, "Draft");
        assert_eq!(saved[0].to[0].address, "bob@example.test");
    }

    #[tokio::test]
    async fn send_mail_uses_canonical_submission_model() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = ActiveSyncService::new(store.clone());

        service
            .handle_request(
                ActiveSyncQuery {
                    cmd: Some("SendMail".to_string()),
                    user: Some("alice@example.test".to_string()),
                    device_id: Some("dev1".to_string()),
                    _device_type: Some("phone".to_string()),
                },
                &mime_headers(),
                b"To: Bob <bob@example.test>\r\nSubject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap();

        let submitted = store.submitted_messages.lock().unwrap();
        assert_eq!(submitted.len(), 1);
        assert_eq!(submitted[0].source, "activesync-sendmail");
        assert_eq!(submitted[0].subject, "Hello");
        assert_eq!(submitted[0].to[0].address, "bob@example.test");
    }
}
