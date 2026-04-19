use anyhow::{anyhow, bail, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use lpe_magika::{
    collect_mime_attachment_parts, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_storage::{
    AttachmentUploadInput, AuditEntryInput, AuthenticatedAccount, JmapEmail, JmapEmailAddress,
    JmapEmailQuery, JmapEmailSubmission, JmapImportedEmailInput, JmapMailbox,
    JmapMailboxCreateInput, JmapMailboxUpdateInput, JmapQuota, JmapUploadBlob,
    SavedDraftMessage, Storage, SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};
use uuid::Uuid;

const JMAP_CORE_CAPABILITY: &str = "urn:ietf:params:jmap:core";
const JMAP_MAIL_CAPABILITY: &str = "urn:ietf:params:jmap:mail";
const JMAP_SUBMISSION_CAPABILITY: &str = "urn:ietf:params:jmap:submission";
const SESSION_STATE: &str = "mvp-1";
const MAX_QUERY_LIMIT: u64 = 250;
const DEFAULT_GET_LIMIT: u64 = 100;

type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;
type HttpResult<T> = std::result::Result<Json<T>, (StatusCode, String)>;

pub fn router() -> Router<Storage> {
    Router::new()
        .route("/session", get(session_handler))
        .route("/api", post(api_handler))
        .route("/upload/{account_id}", post(upload_handler))
        .route(
            "/download/{account_id}/{blob_id}/{name}",
            get(download_handler),
        )
}

#[derive(Clone)]
pub struct JmapService<S, V = lpe_magika::SystemDetector> {
    store: S,
    validator: Validator<V>,
}

impl<S> JmapService<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            validator: Validator::from_env(),
        }
    }
}

impl<S, V> JmapService<S, V> {
    pub fn new_with_validator(store: S, validator: Validator<V>) -> Self {
        Self { store, validator }
    }
}

pub trait JmapStore: Clone + Send + Sync + 'static {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>>;
    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>;
    fn fetch_jmap_mailbox_ids<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>>;
    fn create_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox>;
    fn update_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox>;
    fn destroy_jmap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;
    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery>;
    fn fetch_all_jmap_email_ids<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>>;
    fn fetch_all_jmap_thread_ids<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>>;
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
    fn fetch_jmap_email_submissions<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmailSubmission>>;
    fn fetch_jmap_quota<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, JmapQuota>;
    fn save_jmap_upload_blob<'a>(
        &'a self,
        account_id: Uuid,
        media_type: &'a str,
        blob_bytes: &'a [u8],
    ) -> StoreFuture<'a, JmapUploadBlob>;
    fn fetch_jmap_upload_blob<'a>(
        &'a self,
        account_id: Uuid,
        blob_id: Uuid,
    ) -> StoreFuture<'a, Option<JmapUploadBlob>>;
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
    fn submit_draft_message<'a>(
        &'a self,
        account_id: Uuid,
        draft_message_id: Uuid,
        source: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage>;
    fn copy_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;
    fn import_jmap_email<'a>(
        &'a self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;
}

impl JmapStore for Storage {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
        Box::pin(async move { self.fetch_account_session(token).await })
    }

    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        Box::pin(async move { self.fetch_jmap_mailboxes(account_id).await })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery> {
        Box::pin(async move {
            self.query_jmap_email_ids(account_id, mailbox_id, search_text, position, limit)
                .await
        })
    }

    fn fetch_jmap_mailbox_ids<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>> {
        Box::pin(async move { self.fetch_jmap_mailbox_ids(account_id).await })
    }

    fn create_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        Box::pin(async move { self.create_jmap_mailbox(input, audit).await })
    }

    fn update_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        Box::pin(async move { self.update_jmap_mailbox(input, audit).await })
    }

    fn destroy_jmap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.destroy_jmap_mailbox(account_id, mailbox_id, audit)
                .await
        })
    }

    fn fetch_all_jmap_email_ids<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>> {
        Box::pin(async move { self.fetch_all_jmap_email_ids(account_id).await })
    }

    fn fetch_all_jmap_thread_ids<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>> {
        Box::pin(async move { self.fetch_all_jmap_thread_ids(account_id).await })
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

    fn fetch_jmap_email_submissions<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmailSubmission>> {
        Box::pin(async move { self.fetch_jmap_email_submissions(account_id, ids).await })
    }

    fn fetch_jmap_quota<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, JmapQuota> {
        Box::pin(async move { self.fetch_jmap_quota(account_id).await })
    }

    fn save_jmap_upload_blob<'a>(
        &'a self,
        account_id: Uuid,
        media_type: &'a str,
        blob_bytes: &'a [u8],
    ) -> StoreFuture<'a, JmapUploadBlob> {
        Box::pin(async move {
            self.save_jmap_upload_blob(account_id, media_type, blob_bytes)
                .await
        })
    }

    fn fetch_jmap_upload_blob<'a>(
        &'a self,
        account_id: Uuid,
        blob_id: Uuid,
    ) -> StoreFuture<'a, Option<JmapUploadBlob>> {
        Box::pin(async move { self.fetch_jmap_upload_blob(account_id, blob_id).await })
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
        Box::pin(async move {
            self.delete_draft_message(account_id, message_id, audit)
                .await
        })
    }

    fn submit_draft_message<'a>(
        &'a self,
        account_id: Uuid,
        draft_message_id: Uuid,
        source: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        Box::pin(async move {
            self.submit_draft_message(account_id, draft_message_id, source, audit)
                .await
        })
    }

    fn copy_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.copy_jmap_email(account_id, message_id, target_mailbox_id, audit)
                .await
        })
    }

    fn import_jmap_email<'a>(
        &'a self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move { self.import_jmap_email(input, audit).await })
    }
}

#[derive(Debug, Deserialize)]
pub struct JmapApiRequest {
    #[serde(rename = "using", default)]
    using_capabilities: Vec<String>,
    #[serde(rename = "methodCalls", default)]
    method_calls: Vec<JmapMethodCall>,
}

#[derive(Debug, Deserialize)]
pub struct JmapMethodCall(pub String, pub Value, pub String);

#[derive(Debug, Serialize)]
pub struct JmapApiResponse {
    #[serde(rename = "methodResponses")]
    method_responses: Vec<JmapMethodResponse>,
    #[serde(rename = "createdIds", skip_serializing_if = "HashMap::is_empty")]
    created_ids: HashMap<String, String>,
    #[serde(rename = "sessionState")]
    session_state: String,
}

#[derive(Debug, Serialize)]
pub struct JmapMethodResponse(pub String, pub Value, pub String);

#[derive(Debug, Serialize)]
pub struct SessionDocument {
    capabilities: HashMap<String, Value>,
    accounts: HashMap<String, SessionAccount>,
    #[serde(rename = "primaryAccounts")]
    primary_accounts: HashMap<String, String>,
    username: String,
    #[serde(rename = "apiUrl")]
    api_url: String,
    #[serde(rename = "downloadUrl")]
    download_url: String,
    #[serde(rename = "uploadUrl")]
    upload_url: String,
    #[serde(rename = "eventSourceUrl")]
    event_source_url: Option<String>,
    state: String,
}

#[derive(Debug, Serialize)]
pub struct SessionAccount {
    name: String,
    #[serde(rename = "isPersonal")]
    is_personal: bool,
    #[serde(rename = "isReadOnly")]
    is_read_only: bool,
    #[serde(rename = "accountCapabilities")]
    account_capabilities: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MailboxGetArguments {
    account_id: Option<String>,
    ids: Option<Vec<String>>,
    properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MailboxQueryArguments {
    account_id: Option<String>,
    position: Option<u64>,
    limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MailboxSetArguments {
    account_id: Option<String>,
    create: Option<HashMap<String, Value>>,
    update: Option<HashMap<String, Value>>,
    destroy: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChangesArguments {
    account_id: Option<String>,
    since_state: String,
    max_changes: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailGetArguments {
    account_id: Option<String>,
    ids: Option<Vec<String>>,
    properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailQueryArguments {
    account_id: Option<String>,
    position: Option<u64>,
    limit: Option<u64>,
    filter: Option<EmailQueryFilter>,
    sort: Option<Vec<EmailQuerySort>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailQueryFilter {
    in_mailbox: Option<String>,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailQuerySort {
    property: String,
    is_ascending: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailSetArguments {
    account_id: Option<String>,
    create: Option<HashMap<String, Value>>,
    update: Option<HashMap<String, Value>>,
    destroy: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailSubmissionSetArguments {
    account_id: Option<String>,
    create: Option<HashMap<String, Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailSubmissionGetArguments {
    account_id: Option<String>,
    ids: Option<Vec<String>>,
    properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityGetArguments {
    account_id: Option<String>,
    ids: Option<Vec<String>>,
    properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadGetArguments {
    account_id: Option<String>,
    ids: Option<Vec<String>>,
    properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchSnippetGetArguments {
    account_id: Option<String>,
    email_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailCopyArguments {
    from_account_id: String,
    account_id: Option<String>,
    create: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmailImportArguments {
    account_id: Option<String>,
    emails: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuotaGetArguments {
    account_id: Option<String>,
    ids: Option<Vec<String>>,
}

#[derive(Debug)]
struct MailboxCreateInput {
    name: String,
    sort_order: Option<i32>,
}

#[derive(Debug)]
struct MailboxUpdateInput {
    name: Option<String>,
    sort_order: Option<i32>,
}

#[derive(Debug, Default, Clone)]
struct DraftMutation {
    from: Option<Vec<EmailAddressInput>>,
    to: Option<Vec<EmailAddressInput>>,
    cc: Option<Vec<EmailAddressInput>>,
    bcc: Option<Vec<EmailAddressInput>>,
    subject: Option<String>,
    text_body: Option<String>,
    html_body: Option<Option<String>>,
}

#[derive(Debug, Deserialize, Clone)]
struct EmailAddressInput {
    email: String,
    #[serde(default)]
    name: Option<String>,
}

async fn session_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> HttpResult<SessionDocument> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    Ok(Json(
        service
            .session_document(authorization.as_deref())
            .await
            .map_err(http_error)?,
    ))
}

async fn api_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<JmapApiRequest>,
) -> HttpResult<JmapApiResponse> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    Ok(Json(
        service
            .handle_api_request(authorization.as_deref(), request)
            .await
            .map_err(http_error)?,
    ))
}

async fn upload_handler(
    State(storage): State<Storage>,
    axum::extract::Path(account_id): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let content_type = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let response = service
        .handle_upload(
            authorization.as_deref(),
            &account_id,
            &content_type,
            body.as_ref(),
        )
        .await
        .map_err(http_error)?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn download_handler(
    State(storage): State<Storage>,
    axum::extract::Path((account_id, blob_id, _name)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let blob = service
        .handle_download(authorization.as_deref(), &account_id, &blob_id)
        .await
        .map_err(http_error)?;
    Ok(([("content-type", blob.media_type.clone())], blob.blob_bytes))
}

impl<S: JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub async fn session_document(&self, authorization: Option<&str>) -> Result<SessionDocument> {
        let account = self.authenticate(authorization).await?;
        let account_id = account.account_id.to_string();
        let capabilities = session_capabilities();
        let mut accounts = HashMap::new();
        accounts.insert(
            account_id.clone(),
            SessionAccount {
                name: account.email.clone(),
                is_personal: true,
                is_read_only: false,
                account_capabilities: capabilities.clone(),
            },
        );

        let mut primary_accounts = HashMap::new();
        primary_accounts.insert(JMAP_CORE_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_MAIL_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_SUBMISSION_CAPABILITY.to_string(), account_id.clone());

        Ok(SessionDocument {
            capabilities,
            accounts,
            primary_accounts,
            username: account.email,
            api_url: "/jmap/api".to_string(),
            download_url: "/jmap/download/{accountId}/{blobId}/{name}".to_string(),
            upload_url: "/jmap/upload/{accountId}".to_string(),
            event_source_url: None,
            state: SESSION_STATE.to_string(),
        })
    }

    pub async fn handle_api_request(
        &self,
        authorization: Option<&str>,
        request: JmapApiRequest,
    ) -> Result<JmapApiResponse> {
        let _declared_capabilities = request.using_capabilities;
        let account = self.authenticate(authorization).await?;
        let mut method_responses = Vec::with_capacity(request.method_calls.len());
        let mut created_ids = HashMap::new();

        for JmapMethodCall(method_name, arguments, call_id) in request.method_calls {
            let response = match method_name.as_str() {
                "Mailbox/get" => self.handle_mailbox_get(&account, arguments).await,
                "Mailbox/query" => self.handle_mailbox_query(&account, arguments).await,
                "Mailbox/changes" => self.handle_mailbox_changes(&account, arguments).await,
                "Mailbox/set" => {
                    self.handle_mailbox_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Email/query" => self.handle_email_query(&account, arguments).await,
                "Email/get" => self.handle_email_get(&account, arguments).await,
                "Email/changes" => self.handle_email_changes(&account, arguments).await,
                "Email/set" => {
                    self.handle_email_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Email/copy" => {
                    self.handle_email_copy(&account, arguments, &mut created_ids)
                        .await
                }
                "Email/import" => {
                    self.handle_email_import(&account, arguments, &mut created_ids)
                        .await
                }
                "EmailSubmission/get" => {
                    self.handle_email_submission_get(&account, arguments).await
                }
                "EmailSubmission/set" => {
                    self.handle_email_submission_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Identity/get" => self.handle_identity_get(&account, arguments).await,
                "Thread/get" => self.handle_thread_get(&account, arguments).await,
                "Thread/changes" => self.handle_thread_changes(&account, arguments).await,
                "Quota/get" => self.handle_quota_get(&account, arguments).await,
                "SearchSnippet/get" => self.handle_search_snippet_get(&account, arguments).await,
                _ => Ok(method_error("unknownMethod", "method is not supported")),
            };

            let payload = match response {
                Ok(payload) => payload,
                Err(error) => method_error("invalidArguments", &error.to_string()),
            };
            method_responses.push(JmapMethodResponse(method_name, payload, call_id));
        }

        Ok(JmapApiResponse {
            method_responses,
            created_ids,
            session_state: SESSION_STATE.to_string(),
        })
    }

    async fn handle_mailbox_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: MailboxGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = mailbox_properties(arguments.properties);
        let mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;

        let requested_ids = parse_uuid_list(arguments.ids)?;
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();

        let list = mailboxes
            .iter()
            .filter(|mailbox| requested_ids.is_none() || requested_set.contains(&mailbox.id))
            .map(|mailbox| mailbox_to_value(mailbox, &properties))
            .collect::<Vec<_>>();

        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !mailboxes.iter().any(|mailbox| mailbox.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_mailbox_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: MailboxQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        mailboxes.sort_by_key(|mailbox| (mailbox.sort_order, mailbox.name.to_lowercase()));
        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = mailboxes
            .iter()
            .skip(position)
            .take(limit)
            .map(|mailbox| mailbox.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": mailboxes.len(),
        }))
    }

    async fn handle_mailbox_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = self.store.fetch_jmap_mailbox_ids(account_id).await?;
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            ids,
        ))
    }

    async fn handle_mailbox_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: MailboxSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_mailbox_create(value) {
                    Ok(input) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-create".to_string(),
                            subject: creation_id.clone(),
                        };
                        match self
                            .store
                            .create_jmap_mailbox(
                                JmapMailboxCreateInput {
                                    account_id,
                                    name: input.name,
                                    sort_order: input.sort_order,
                                },
                                audit,
                            )
                            .await
                        {
                            Ok(mailbox) => {
                                created_ids.insert(creation_id.clone(), mailbox.id.to_string());
                                created.insert(creation_id, json!({"id": mailbox.id.to_string()}));
                            }
                            Err(error) => {
                                not_created.insert(creation_id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match parse_uuid(&id).and_then(|mailbox_id| {
                    parse_mailbox_update(value).map(|input| (mailbox_id, input))
                }) {
                    Ok((mailbox_id, input)) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-update".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .update_jmap_mailbox(
                                JmapMailboxUpdateInput {
                                    account_id,
                                    mailbox_id,
                                    name: input.name,
                                    sort_order: input.sort_order,
                                },
                                audit,
                            )
                            .await
                        {
                            Ok(_) => {
                                updated.insert(id, Value::Object(Map::new()));
                            }
                            Err(error) => {
                                not_updated.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(mailbox_id) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-destroy".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .destroy_jmap_mailbox(account_id, mailbox_id, audit)
                            .await
                        {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_email_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_query_sort(arguments.sort.as_deref())?;

        let mailbox_id = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.in_mailbox.as_deref())
            .map(|value| parse_uuid(&value))
            .transpose()?;
        let search_text = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.text.as_deref());
        let position = arguments.position.unwrap_or(0);
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT);
        let query = self
            .store
            .query_jmap_email_ids(account_id, mailbox_id, search_text, position, limit)
            .await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": false,
            "position": position,
            "ids": query.ids.into_iter().map(|id| id.to_string()).collect::<Vec<_>>(),
            "total": query.total,
        }))
    }

    async fn handle_email_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = email_properties(arguments.properties);

        let ids = match parse_uuid_list(arguments.ids)? {
            Some(ids) => ids,
            None => {
                self.store
                    .query_jmap_email_ids(account_id, None, None, 0, DEFAULT_GET_LIMIT)
                    .await?
                    .ids
            }
        };

        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let not_found = ids
            .iter()
            .filter(|id| !emails.iter().any(|email| email.id == **id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": emails.iter().map(|email| email_to_value(email, &properties)).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    async fn handle_email_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            ids,
        ))
    }

    async fn handle_email_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailCopyArguments = serde_json::from_value(arguments)?;
        let from_account_id = requested_account_id(Some(&arguments.from_account_id), account)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        if from_account_id != account_id {
            bail!("cross-account Email/copy is not supported");
        }

        let mut created = Map::new();
        let mut not_created = Map::new();
        for (creation_id, value) in arguments.create {
            match parse_email_copy(value, created_ids) {
                Ok((email_id, mailbox_id)) => {
                    let audit = AuditEntryInput {
                        actor: account.email.clone(),
                        action: "jmap-email-copy".to_string(),
                        subject: creation_id.clone(),
                    };
                    match self
                        .store
                        .copy_jmap_email(account_id, email_id, mailbox_id, audit)
                        .await
                    {
                        Ok(email) => {
                            created_ids.insert(creation_id.clone(), email.id.to_string());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": email.id.to_string(),
                                    "blobId": format!("message:{}", email.id),
                                    "threadId": email.thread_id.to_string(),
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_created.insert(creation_id, set_error(&error.to_string()));
                }
            }
        }

        Ok(json!({
            "fromAccountId": from_account_id.to_string(),
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    async fn handle_email_import(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailImportArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();

        for (creation_id, value) in arguments.emails {
            match self
                .parse_email_import(account, account_id, value, created_ids)
                .await
            {
                Ok(input) => {
                    let audit = AuditEntryInput {
                        actor: account.email.clone(),
                        action: "jmap-email-import".to_string(),
                        subject: creation_id.clone(),
                    };
                    match self.store.import_jmap_email(input, audit).await {
                        Ok(email) => {
                            created_ids.insert(creation_id.clone(), email.id.to_string());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": email.id.to_string(),
                                    "blobId": format!("message:{}", email.id),
                                    "threadId": email.thread_id.to_string(),
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_created.insert(creation_id, set_error(&error.to_string()));
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    async fn handle_email_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match self
                    .create_draft(account, account_id, value, creation_id.as_str())
                    .await
                {
                    Ok(saved) => {
                        created_ids.insert(creation_id.clone(), saved.message_id.to_string());
                        created.insert(
                            creation_id,
                            json!({
                                "id": saved.message_id.to_string(),
                                "blobId": format!("draft:{}", saved.message_id),
                            }),
                        );
                    }
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match self.update_draft(account, account_id, &id, value).await {
                    Ok(_) => {
                        updated.insert(id, Value::Object(Map::new()));
                    }
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(message_id) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-email-draft-delete".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .delete_draft_message(account_id, message_id, audit)
                            .await
                        {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_email_submission_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailSubmissionSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_submission_email_id(&value, created_ids)? {
                    Some(email_id) => {
                        let message_id = parse_uuid(&email_id)?;
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-email-submit".to_string(),
                            subject: email_id.clone(),
                        };
                        match self
                            .store
                            .submit_draft_message(account_id, message_id, "jmap", audit)
                            .await
                        {
                            Ok(result) => {
                                created_ids.insert(
                                    creation_id.clone(),
                                    result.outbound_queue_id.to_string(),
                                );
                                created.insert(
                                    creation_id,
                                    json!({
                                        "id": result.outbound_queue_id.to_string(),
                                        "emailId": result.message_id.to_string(),
                                        "threadId": result.thread_id.to_string(),
                                        "undoStatus": "final",
                                    }),
                                );
                            }
                            Err(error) => {
                                not_created.insert(creation_id, set_error(&error.to_string()));
                            }
                        }
                    }
                    None => {
                        not_created.insert(
                            creation_id,
                            method_error("invalidArguments", "emailId is required"),
                        );
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    async fn handle_email_submission_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailSubmissionGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = parse_uuid_list(arguments.ids)?;
        let properties = email_submission_properties(arguments.properties);
        let ids_ref = ids.as_deref().unwrap_or(&[]);
        let submissions = self
            .store
            .fetch_jmap_email_submissions(account_id, ids_ref)
            .await?;
        let not_found = ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !submissions.iter().any(|submission| submission.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": submissions.iter().map(|submission| email_submission_to_value(submission, &properties)).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    async fn handle_identity_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: IdentityGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = identity_properties(arguments.properties);
        let identity_id = identity_id_for(account);
        let ids = arguments.ids.unwrap_or_else(|| vec![identity_id.clone()]);
        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            if id == identity_id {
                list.push(identity_to_value(account, &properties));
            } else {
                not_found.push(Value::String(id));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_thread_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ThreadGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = thread_properties(arguments.properties);
        let all_email_ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
        let emails = self
            .store
            .fetch_jmap_emails(account_id, &all_email_ids)
            .await?;
        let ids = arguments.ids.unwrap_or_else(|| {
            emails
                .iter()
                .map(|email| email.thread_id.to_string())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        });

        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            let thread_id = parse_uuid(&id)?;
            let thread_emails = emails
                .iter()
                .filter(|email| email.thread_id == thread_id)
                .map(|email| email.id.to_string())
                .collect::<Vec<_>>();
            if thread_emails.is_empty() {
                not_found.push(Value::String(id));
            } else {
                list.push(thread_to_value(thread_id, thread_emails, &properties));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_thread_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = self.store.fetch_all_jmap_thread_ids(account_id).await?;
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            ids,
        ))
    }

    async fn handle_quota_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QuotaGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let quota = self.store.fetch_jmap_quota(account_id).await?;
        let ids = arguments.ids.unwrap_or_else(|| vec![quota.id.clone()]);
        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            if id == quota.id {
                list.push(quota_to_value(&quota));
            } else {
                not_found.push(Value::String(id));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_search_snippet_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: SearchSnippetGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = parse_uuid_list(Some(arguments.email_ids))?.unwrap_or_default();
        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let not_found = ids
            .iter()
            .filter(|id| !emails.iter().any(|email| email.id == **id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "list": emails.iter().map(search_snippet_to_value).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    async fn handle_upload(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        media_type: &str,
        body: &[u8],
    ) -> Result<Value> {
        let account = self.authenticate(authorization).await?;
        let requested_account_id = requested_account_id(Some(account_id), &account)?;
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapUpload,
                declared_mime: Some(media_type.to_string()),
                filename: None,
                expected_kind: ExpectedKind::Any,
            },
            body,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP upload blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let blob = self
            .store
            .save_jmap_upload_blob(requested_account_id, media_type, body)
            .await?;

        Ok(json!({
            "accountId": requested_account_id.to_string(),
            "blobId": blob.id.to_string(),
            "type": blob.media_type,
            "size": blob.octet_size,
        }))
    }

    async fn handle_download(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        blob_id: &str,
    ) -> Result<JmapUploadBlob> {
        let account = self.authenticate(authorization).await?;
        let requested_account_id = requested_account_id(Some(account_id), &account)?;
        let blob_id = parse_uuid(blob_id)?;
        self.store
            .fetch_jmap_upload_blob(requested_account_id, blob_id)
            .await?
            .ok_or_else(|| anyhow!("blob not found"))
    }

    async fn create_draft(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        value: Value,
        creation_id: &str,
    ) -> Result<SavedDraftMessage> {
        let mutation = parse_draft_mutation(value)?;
        let from = select_from_address(mutation.from, account)?;
        let audit = AuditEntryInput {
            actor: account.email.clone(),
            action: "jmap-email-draft-create".to_string(),
            subject: creation_id.to_string(),
        };
        self.store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id,
                    source: "jmap".to_string(),
                    from_display: from.name,
                    from_address: from.email,
                    to: map_recipients(mutation.to.unwrap_or_default())?,
                    cc: map_recipients(mutation.cc.unwrap_or_default())?,
                    bcc: map_recipients(mutation.bcc.unwrap_or_default())?,
                    subject: mutation.subject.unwrap_or_default(),
                    body_text: mutation.text_body.unwrap_or_default(),
                    body_html_sanitized: mutation.html_body.unwrap_or(None),
                    internet_message_id: None,
                    mime_blob_ref: None,
                    size_octets: 0,
                    attachments: Vec::new(),
                },
                audit,
            )
            .await
    }

    async fn update_draft(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        id: &str,
        value: Value,
    ) -> Result<SavedDraftMessage> {
        let message_id = parse_uuid(id)?;
        let existing = self
            .store
            .fetch_jmap_draft(account_id, message_id)
            .await?
            .ok_or_else(|| anyhow!("draft not found"))?;
        let mutation = parse_draft_mutation(value)?;
        let from = match mutation.from {
            Some(from) => select_from_address(Some(from), account)?,
            None => EmailAddressInput {
                email: existing.from_address,
                name: existing.from_display,
            },
        };
        let audit = AuditEntryInput {
            actor: account.email.clone(),
            action: "jmap-email-draft-update".to_string(),
            subject: id.to_string(),
        };

        self.store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: Some(message_id),
                    account_id,
                    source: "jmap".to_string(),
                    from_display: from.name,
                    from_address: from.email,
                    to: mutation
                        .to
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.to)),
                    cc: mutation
                        .cc
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.cc)),
                    bcc: mutation
                        .bcc
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.bcc)),
                    subject: mutation.subject.unwrap_or(existing.subject),
                    body_text: mutation.text_body.unwrap_or(existing.body_text),
                    body_html_sanitized: mutation.html_body.unwrap_or(existing.body_html_sanitized),
                    internet_message_id: existing.internet_message_id,
                    mime_blob_ref: None,
                    size_octets: existing.size_octets,
                    attachments: Vec::new(),
                },
                audit,
            )
            .await
    }

    async fn parse_email_import(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        value: Value,
        created_ids: &HashMap<String, String>,
    ) -> Result<JmapImportedEmailInput> {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("import arguments must be an object"))?;
        let blob_id = object
            .get("blobId")
            .and_then(Value::as_str)
            .map(|value| resolve_creation_reference(value, created_ids))
            .ok_or_else(|| anyhow!("blobId is required"))?;
        let blob_id = parse_uuid(&blob_id)?;
        let mailbox_ids = object
            .get("mailboxIds")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("mailboxIds is required"))?;
        let target_mailbox_id = mailbox_ids
            .iter()
            .find(|(_, included)| included.as_bool().unwrap_or(false))
            .map(|(mailbox_id, _)| parse_uuid(mailbox_id))
            .transpose()?
            .ok_or_else(|| anyhow!("one target mailboxId is required"))?;
        let blob = self
            .store
            .fetch_jmap_upload_blob(account_id, blob_id)
            .await?
            .ok_or_else(|| anyhow!("uploaded blob not found"))?;
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapEmailImport,
                declared_mime: Some(blob.media_type.clone()),
                filename: None,
                expected_kind: ExpectedKind::Rfc822Message,
            },
            &blob.blob_bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP email import blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let parsed = parse_rfc822_message(&blob.blob_bytes)?;

        Ok(JmapImportedEmailInput {
            account_id,
            mailbox_id: target_mailbox_id,
            source: "jmap-import".to_string(),
            from_display: parsed
                .from
                .as_ref()
                .and_then(|from| from.name.clone())
                .or(Some(account.display_name.clone())),
            from_address: parsed
                .from
                .map(|from| from.email)
                .unwrap_or_else(|| account.email.clone()),
            to: map_recipients(parsed.to)?,
            cc: map_recipients(parsed.cc)?,
            bcc: Vec::new(),
            subject: parsed.subject,
            body_text: parsed.body_text,
            body_html_sanitized: None,
            internet_message_id: parsed.message_id,
            mime_blob_ref: format!("upload:{}", blob.id),
            size_octets: blob.octet_size as i64,
            received_at: None,
            attachments: parsed.attachments,
        })
    }

    async fn authenticate(&self, authorization: Option<&str>) -> Result<AuthenticatedAccount> {
        let token = bearer_token(authorization).ok_or_else(|| anyhow!("missing bearer token"))?;
        self.store
            .fetch_account_session(token)
            .await?
            .ok_or_else(|| anyhow!("invalid or expired account session"))
    }
}

fn authorization_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn bearer_token(authorization: Option<&str>) -> Option<&str> {
    authorization
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn http_error(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    let status = if message.contains("bearer token") || message.contains("expired account session")
    {
        StatusCode::UNAUTHORIZED
    } else if message.contains("Magika command")
        || message.contains("spawn Magika")
        || message.contains("Magika stdin")
    {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, message)
}

fn session_capabilities() -> HashMap<String, Value> {
    HashMap::from([
        (
            JMAP_CORE_CAPABILITY.to_string(),
            json!({
                "maxSizeUpload": 0,
                "maxCallsInRequest": 16,
                "maxConcurrentUpload": 0,
                "maxObjectsInGet": 250,
                "maxObjectsInSet": 128,
                "collationAlgorithms": ["i;ascii-casemap"],
            }),
        ),
        (
            JMAP_MAIL_CAPABILITY.to_string(),
            json!({
                "maxMailboxesPerEmail": 1,
                "maxMailboxDepth": 1,
                "emailQuerySortOptions": ["receivedAt"],
            }),
        ),
        (
            JMAP_SUBMISSION_CAPABILITY.to_string(),
            json!({
                "maxDelayedSend": 0,
            }),
        ),
    ])
}

fn requested_account_id(
    requested_account_id: Option<&str>,
    account: &AuthenticatedAccount,
) -> Result<Uuid> {
    match requested_account_id {
        Some(value) => {
            let id = parse_uuid(value)?;
            if id == account.account_id {
                Ok(id)
            } else {
                bail!("accountId does not match authenticated account");
            }
        }
        None => Ok(account.account_id),
    }
}

fn parse_uuid(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| anyhow!("invalid id: {value}"))
}

fn parse_uuid_list(value: Option<Vec<String>>) -> Result<Option<Vec<Uuid>>> {
    value
        .map(|values| values.into_iter().map(|value| parse_uuid(&value)).collect())
        .transpose()
}

fn validate_query_sort(sort: Option<&[EmailQuerySort]>) -> Result<()> {
    if let Some(sort) = sort {
        for item in sort {
            if item.property != "receivedAt" || item.is_ascending.unwrap_or(false) {
                bail!("only receivedAt descending sort is supported");
            }
        }
    }
    Ok(())
}

fn mailbox_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "role".to_string(),
                "sortOrder".to_string(),
                "totalEmails".to_string(),
                "unreadEmails".to_string(),
                "isSubscribed".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn mailbox_to_value(mailbox: &JmapMailbox, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", mailbox.id.to_string());
    insert_if(properties, &mut object, "name", mailbox.name.clone());
    insert_if(properties, &mut object, "role", mailbox.role.clone());
    insert_if(properties, &mut object, "sortOrder", mailbox.sort_order);
    insert_if(properties, &mut object, "totalEmails", mailbox.total_emails);
    insert_if(
        properties,
        &mut object,
        "unreadEmails",
        mailbox.unread_emails,
    );
    insert_if(properties, &mut object, "isSubscribed", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayReadItems": true,
                "mayAddItems": mailbox.role == "drafts",
                "mayRemoveItems": mailbox.role == "drafts",
                "maySetSeen": true,
                "maySetKeywords": true,
                "mayCreateChild": false,
                "mayRename": false,
                "mayDelete": false,
                "maySubmit": mailbox.role == "drafts",
            }),
        );
    }
    Value::Object(object)
}

fn changes_response(
    account_id: Uuid,
    since_state: &str,
    max_changes: Option<u64>,
    ids: Vec<Uuid>,
) -> Value {
    let max_changes = max_changes.unwrap_or(u64::MAX) as usize;
    if since_state == SESSION_STATE {
        json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "hasMoreChanges": false,
            "created": Vec::<String>::new(),
            "updated": Vec::<String>::new(),
            "destroyed": Vec::<String>::new(),
        })
    } else {
        let created = ids
            .into_iter()
            .take(max_changes)
            .map(|id| id.to_string())
            .collect::<Vec<_>>();
        json!({
            "accountId": account_id.to_string(),
            "oldState": since_state,
            "newState": SESSION_STATE,
            "hasMoreChanges": false,
            "created": created,
            "updated": Vec::<String>::new(),
            "destroyed": Vec::<String>::new(),
        })
    }
}

fn email_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "blobId".to_string(),
                "threadId".to_string(),
                "mailboxIds".to_string(),
                "keywords".to_string(),
                "size".to_string(),
                "receivedAt".to_string(),
                "sentAt".to_string(),
                "messageId".to_string(),
                "subject".to_string(),
                "from".to_string(),
                "to".to_string(),
                "cc".to_string(),
                "preview".to_string(),
                "hasAttachment".to_string(),
                "textBody".to_string(),
                "htmlBody".to_string(),
                "bodyValues".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn email_submission_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "emailId".to_string(),
                "threadId".to_string(),
                "identityId".to_string(),
                "envelope".to_string(),
                "sendAt".to_string(),
                "undoStatus".to_string(),
                "deliveryStatus".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn identity_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
                "replyTo".to_string(),
                "bcc".to_string(),
                "textSignature".to_string(),
                "htmlSignature".to_string(),
                "mayDelete".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn thread_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| vec!["id".to_string(), "emailIds".to_string()])
        .into_iter()
        .collect()
}

fn email_to_value(email: &JmapEmail, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", email.id.to_string());
    insert_if(
        properties,
        &mut object,
        "blobId",
        format!("message:{}", email.id),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        email.thread_id.to_string(),
    );
    if properties.contains("mailboxIds") {
        let mut mailbox_ids = Map::new();
        mailbox_ids.insert(email.mailbox_id.to_string(), Value::Bool(true));
        object.insert("mailboxIds".to_string(), Value::Object(mailbox_ids));
    }
    if properties.contains("keywords") {
        object.insert("keywords".to_string(), email_keywords(email));
    }
    insert_if(properties, &mut object, "size", email.size_octets);
    insert_if(
        properties,
        &mut object,
        "receivedAt",
        email.received_at.clone(),
    );
    if let Some(sent_at) = &email.sent_at {
        insert_if(properties, &mut object, "sentAt", sent_at.clone());
    }
    if properties.contains("messageId") {
        object.insert(
            "messageId".to_string(),
            Value::Array(
                email
                    .internet_message_id
                    .as_ref()
                    .map(|message_id| vec![Value::String(message_id.clone())])
                    .unwrap_or_default(),
            ),
        );
    }
    insert_if(properties, &mut object, "subject", email.subject.clone());
    if properties.contains("from") {
        object.insert(
            "from".to_string(),
            Value::Array(vec![address_value(&EmailAddressInput {
                email: email.from_address.clone(),
                name: email.from_display.clone(),
            })]),
        );
    }
    if properties.contains("to") {
        object.insert(
            "to".to_string(),
            Value::Array(
                email
                    .to
                    .iter()
                    .map(|recipient| {
                        address_value(&EmailAddressInput {
                            email: recipient.address.clone(),
                            name: recipient.display_name.clone(),
                        })
                    })
                    .collect(),
            ),
        );
    }
    if properties.contains("cc") {
        object.insert(
            "cc".to_string(),
            Value::Array(
                email
                    .cc
                    .iter()
                    .map(|recipient| {
                        address_value(&EmailAddressInput {
                            email: recipient.address.clone(),
                            name: recipient.display_name.clone(),
                        })
                    })
                    .collect(),
            ),
        );
    }
    if properties.contains("bcc") && !email.bcc.is_empty() {
        object.insert(
            "bcc".to_string(),
            Value::Array(
                email
                    .bcc
                    .iter()
                    .map(|recipient| {
                        address_value(&EmailAddressInput {
                            email: recipient.address.clone(),
                            name: recipient.display_name.clone(),
                        })
                    })
                    .collect(),
            ),
        );
    }
    insert_if(properties, &mut object, "preview", email.preview.clone());
    insert_if(
        properties,
        &mut object,
        "hasAttachment",
        email.has_attachments,
    );

    let mut body_values = Map::new();
    if !email.body_text.is_empty() {
        body_values.insert(
            "textBody".to_string(),
            json!({
                "value": email.body_text.clone(),
                "isEncodingProblem": false,
                "isTruncated": false,
            }),
        );
        if properties.contains("textBody") {
            object.insert(
                "textBody".to_string(),
                json!([{ "partId": "textBody", "type": "text/plain" }]),
            );
        }
    }
    if let Some(html) = &email.body_html_sanitized {
        body_values.insert(
            "htmlBody".to_string(),
            json!({
                "value": html.clone(),
                "isEncodingProblem": false,
                "isTruncated": false,
            }),
        );
        if properties.contains("htmlBody") {
            object.insert(
                "htmlBody".to_string(),
                json!([{ "partId": "htmlBody", "type": "text/html" }]),
            );
        }
    }
    if properties.contains("bodyValues") {
        object.insert("bodyValues".to_string(), Value::Object(body_values));
    }

    Value::Object(object)
}

fn email_submission_to_value(
    submission: &JmapEmailSubmission,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", submission.id.to_string());
    insert_if(
        properties,
        &mut object,
        "emailId",
        submission.email_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        submission.thread_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "identityId",
        submission.identity_email.clone(),
    );
    if properties.contains("envelope") {
        object.insert(
            "envelope".to_string(),
            json!({
                "mailFrom": {"email": submission.envelope_mail_from},
                "rcptTo": submission.envelope_rcpt_to.iter().map(|address| json!({"email": address})).collect::<Vec<_>>(),
            }),
        );
    }
    insert_if(
        properties,
        &mut object,
        "sendAt",
        submission.send_at.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "undoStatus",
        submission.undo_status.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "deliveryStatus",
        submission.delivery_status.clone(),
    );
    Value::Object(object)
}

fn identity_id_for(account: &AuthenticatedAccount) -> String {
    account.email.to_lowercase()
}

fn identity_to_value(account: &AuthenticatedAccount, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", identity_id_for(account));
    insert_if(
        properties,
        &mut object,
        "name",
        account.display_name.clone(),
    );
    insert_if(properties, &mut object, "email", account.email.clone());
    if properties.contains("replyTo") {
        object.insert("replyTo".to_string(), Value::Null);
    }
    if properties.contains("bcc") {
        object.insert("bcc".to_string(), Value::Null);
    }
    insert_if(properties, &mut object, "textSignature", "");
    insert_if(properties, &mut object, "htmlSignature", "");
    insert_if(properties, &mut object, "mayDelete", false);
    Value::Object(object)
}

fn thread_to_value(thread_id: Uuid, email_ids: Vec<String>, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", thread_id.to_string());
    if properties.contains("emailIds") {
        object.insert(
            "emailIds".to_string(),
            Value::Array(email_ids.into_iter().map(Value::String).collect()),
        );
    }
    Value::Object(object)
}

fn search_snippet_to_value(email: &JmapEmail) -> Value {
    let subject = if email.subject.is_empty() {
        email.preview.clone()
    } else {
        email.subject.clone()
    };
    let preview = if email.preview.is_empty() {
        trim_snippet(&email.body_text, 120)
    } else {
        trim_snippet(&email.preview, 120)
    };
    json!({
        "emailId": email.id.to_string(),
        "subject": subject,
        "preview": preview,
    })
}

fn trim_snippet(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        normalized.chars().take(max_chars).collect::<String>()
    }
}

fn quota_to_value(quota: &JmapQuota) -> Value {
    json!({
        "id": quota.id,
        "name": quota.name,
        "used": quota.used,
        "hardLimit": quota.hard_limit,
        "scope": "account",
    })
}

fn email_keywords(email: &JmapEmail) -> Value {
    let mut keywords = Map::new();
    if email.mailbox_role == "drafts" {
        keywords.insert("$draft".to_string(), Value::Bool(true));
    }
    if !email.unread {
        keywords.insert("$seen".to_string(), Value::Bool(true));
    }
    if email.flagged {
        keywords.insert("$flagged".to_string(), Value::Bool(true));
    }
    Value::Object(keywords)
}

fn insert_if<T: Serialize>(
    properties: &HashSet<String>,
    object: &mut Map<String, Value>,
    key: &str,
    value: T,
) {
    if properties.contains(key) {
        object.insert(
            key.to_string(),
            serde_json::to_value(value).unwrap_or(Value::Null),
        );
    }
}

fn address_value(address: &EmailAddressInput) -> Value {
    json!({
        "email": address.email,
        "name": address.name,
    })
}

fn method_error(kind: &str, description: &str) -> Value {
    json!({
        "type": kind,
        "description": description,
    })
}

fn set_error(description: &str) -> Value {
    method_error("invalidProperties", description)
}

fn parse_submission_email_id(
    value: &Value,
    created_ids: &HashMap<String, String>,
) -> Result<Option<String>> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("submission create arguments must be an object"))?;
    if let Some(email_id) = object.get("emailId").and_then(Value::as_str) {
        return Ok(Some(resolve_creation_reference(email_id, created_ids)));
    }
    if let Some(reference) = object.get("#emailId").and_then(Value::as_str) {
        return Ok(created_ids.get(reference).cloned());
    }
    Ok(None)
}

fn resolve_creation_reference(value: &str, created_ids: &HashMap<String, String>) -> String {
    if let Some(reference) = value.strip_prefix('#') {
        created_ids
            .get(reference)
            .cloned()
            .unwrap_or_else(|| value.to_string())
    } else {
        value.to_string()
    }
}

fn parse_draft_mutation(value: Value) -> Result<DraftMutation> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("email arguments must be an object"))?;
    reject_unknown_email_properties(object)?;

    if let Some(mailbox_ids) = object.get("mailboxIds").and_then(Value::as_object) {
        if mailbox_ids.len() > 1 {
            bail!("only one mailboxId is supported");
        }
    }
    if let Some(keywords) = object.get("keywords").and_then(Value::as_object) {
        for keyword in keywords.keys() {
            if keyword != "$draft" && keyword != "$seen" && keyword != "$flagged" {
                bail!("unsupported keyword: {keyword}");
            }
        }
    }

    Ok(DraftMutation {
        from: parse_address_list(object.get("from"))?,
        to: parse_address_list(object.get("to"))?,
        cc: parse_address_list(object.get("cc"))?,
        bcc: parse_address_list(object.get("bcc"))?,
        subject: parse_optional_string(object.get("subject"))?,
        text_body: parse_optional_string(object.get("textBody"))?,
        html_body: parse_optional_nullable_string(object.get("htmlBody"))?,
    })
}

fn parse_mailbox_create(value: Value) -> Result<MailboxCreateInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("mailbox create arguments must be an object"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("mailbox name is required"))?
        .to_string();
    let sort_order = object
        .get("sortOrder")
        .and_then(Value::as_i64)
        .map(|value| value as i32);
    Ok(MailboxCreateInput { name, sort_order })
}

fn parse_mailbox_update(value: Value) -> Result<MailboxUpdateInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("mailbox update arguments must be an object"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_string());
    let sort_order = object
        .get("sortOrder")
        .and_then(Value::as_i64)
        .map(|value| value as i32);
    Ok(MailboxUpdateInput { name, sort_order })
}

fn parse_email_copy(value: Value, created_ids: &HashMap<String, String>) -> Result<(Uuid, Uuid)> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("Email/copy create arguments must be an object"))?;
    let email_id = object
        .get("emailId")
        .and_then(Value::as_str)
        .map(|value| resolve_creation_reference(value, created_ids))
        .ok_or_else(|| anyhow!("emailId is required"))?;
    let mailbox_ids = object
        .get("mailboxIds")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("mailboxIds is required"))?;
    let mailbox_id = mailbox_ids
        .iter()
        .find(|(_, value)| value.as_bool().unwrap_or(false))
        .map(|(id, _)| parse_uuid(id))
        .transpose()?
        .ok_or_else(|| anyhow!("one target mailboxId is required"))?;
    Ok((parse_uuid(&email_id)?, mailbox_id))
}

fn reject_unknown_email_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "from" | "to" | "cc" | "bcc" | "subject" | "textBody" | "htmlBody" | "mailboxIds"
            | "keywords" => {}
            _ => bail!("unsupported email property: {key}"),
        }
    }
    Ok(())
}

fn parse_address_list(value: Option<&Value>) -> Result<Option<Vec<EmailAddressInput>>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(Vec::new())),
        Some(value) => Ok(Some(serde_json::from_value(value.clone())?)),
    }
}

fn parse_optional_string(value: Option<&Value>) -> Result<Option<String>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(String::new())),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => bail!("string property expected"),
    }
}

fn parse_optional_nullable_string(value: Option<&Value>) -> Result<Option<Option<String>>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(value)) => Ok(Some(Some(value.clone()))),
        _ => bail!("string or null property expected"),
    }
}

fn select_from_address(
    from: Option<Vec<EmailAddressInput>>,
    account: &AuthenticatedAccount,
) -> Result<EmailAddressInput> {
    match from {
        None => Ok(EmailAddressInput {
            email: account.email.clone(),
            name: Some(account.display_name.clone()),
        }),
        Some(mut addresses) => {
            if addresses.len() != 1 {
                bail!("exactly one from address is required");
            }
            let address = addresses.remove(0);
            if address.email.trim().eq_ignore_ascii_case(&account.email) {
                Ok(EmailAddressInput {
                    email: account.email.clone(),
                    name: address.name,
                })
            } else {
                bail!("from email must match authenticated account");
            }
        }
    }
}

fn map_recipients(input: Vec<EmailAddressInput>) -> Result<Vec<SubmittedRecipientInput>> {
    input
        .into_iter()
        .map(|recipient| {
            let address = recipient.email.trim().to_lowercase();
            if address.is_empty() {
                bail!("recipient email is required");
            }
            Ok(SubmittedRecipientInput {
                address,
                display_name: recipient.name.and_then(|name| {
                    let trimmed = name.trim().to_string();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed)
                    }
                }),
            })
        })
        .collect()
}

fn map_existing_recipients(recipients: &[JmapEmailAddress]) -> Vec<SubmittedRecipientInput> {
    recipients
        .iter()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
        })
        .collect()
}

#[derive(Debug)]
struct ParsedRfc822Message {
    from: Option<EmailAddressInput>,
    to: Vec<EmailAddressInput>,
    cc: Vec<EmailAddressInput>,
    subject: String,
    message_id: Option<String>,
    body_text: String,
    attachments: Vec<AttachmentUploadInput>,
}

fn parse_rfc822_message(bytes: &[u8]) -> Result<ParsedRfc822Message> {
    let raw = String::from_utf8_lossy(bytes).replace("\r\n", "\n");
    let (header_text, body_text) = raw
        .split_once("\n\n")
        .map(|(headers, body)| (headers, body))
        .unwrap_or((raw.as_str(), ""));
    let headers = parse_headers(header_text);

    Ok(ParsedRfc822Message {
        from: headers
            .get("from")
            .and_then(|value| parse_single_address(value).ok()),
        to: headers
            .get("to")
            .map(|value| parse_address_header(value))
            .transpose()?
            .unwrap_or_default(),
        cc: headers
            .get("cc")
            .map(|value| parse_address_header(value))
            .transpose()?
            .unwrap_or_default(),
        subject: headers.get("subject").cloned().unwrap_or_default(),
        message_id: headers.get("message-id").cloned(),
        body_text: body_text.trim().to_string(),
        attachments: collect_mime_attachment_parts(bytes)?
            .into_iter()
            .enumerate()
            .map(|(index, attachment)| AttachmentUploadInput {
                file_name: attachment
                    .filename
                    .unwrap_or_else(|| format!("attachment-{}.bin", index + 1)),
                media_type: attachment
                    .declared_mime
                    .unwrap_or_else(|| "application/octet-stream".to_string()),
                blob_bytes: attachment.bytes,
            })
            .collect(),
    })
}

fn parse_headers(input: &str) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    let mut current_name: Option<String> = None;
    let mut current_value = String::new();

    for line in input.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if !current_value.is_empty() {
                current_value.push(' ');
            }
            current_value.push_str(line.trim());
            continue;
        }

        if let Some(name) = current_name.take() {
            headers.insert(name, current_value.trim().to_string());
            current_value.clear();
        }

        if let Some((name, value)) = line.split_once(':') {
            current_name = Some(name.trim().to_lowercase());
            current_value.push_str(value.trim());
        }
    }

    if let Some(name) = current_name {
        headers.insert(name, current_value.trim().to_string());
    }

    headers
}

fn parse_address_header(value: &str) -> Result<Vec<EmailAddressInput>> {
    value
        .split(',')
        .map(parse_single_address)
        .filter(|result| match result {
            Ok(address) => !address.email.is_empty(),
            Err(_) => true,
        })
        .collect()
}

    fn parse_single_address(value: &str) -> Result<EmailAddressInput> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("email address is empty");
    }

    if let Some((name, email)) = trimmed.rsplit_once('<') {
        let email = email.trim_end_matches('>').trim();
        return Ok(EmailAddressInput {
            email: email.to_lowercase(),
            name: Some(name.trim().trim_matches('"').to_string()).filter(|value| !value.is_empty()),
        });
    }

    Ok(EmailAddressInput {
        email: trimmed.trim_matches('"').to_lowercase(),
        name: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lpe_magika::{DetectionSource, Detector, MagikaDetection};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct FakeStore {
        session: Option<AuthenticatedAccount>,
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        uploads: Arc<Mutex<Vec<JmapUploadBlob>>>,
        saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
        submitted_drafts: Arc<Mutex<Vec<Uuid>>>,
    }

    #[derive(Clone)]
    struct FakeDetector {
        result: Result<MagikaDetection, String>,
    }

    #[test]
    fn parse_rfc822_message_collects_supported_attachment_parts() {
        let message = concat!(
            "From: Alice <alice@example.test>\r\n",
            "To: Bob <bob@example.test>\r\n",
            "Subject: Import\r\n",
            "Content-Type: multipart/mixed; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "Hello\r\n",
            "--b1\r\n",
            "Content-Type: application/vnd.oasis.opendocument.text\r\n",
            "Content-Disposition: attachment; filename=\"notes.odt\"\r\n",
            "\r\n",
            "ODT-DATA\r\n",
            "--b1--\r\n"
        );

        let parsed = parse_rfc822_message(message.as_bytes()).unwrap();

        assert_eq!(parsed.subject, "Import");
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].file_name, "notes.odt");
        assert_eq!(
            parsed.attachments[0].media_type,
            "application/vnd.oasis.opendocument.text"
        );
        assert_eq!(parsed.attachments[0].blob_bytes, b"ODT-DATA".to_vec());
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> Result<MagikaDetection> {
            self.result.clone().map_err(anyhow::Error::msg)
        }
    }

    fn validator_ok(
        mime_type: &str,
        label: &str,
        extension: &str,
        score: f32,
    ) -> Validator<FakeDetector> {
        Validator::new(
            FakeDetector {
                result: Ok(MagikaDetection {
                    label: label.to_string(),
                    mime_type: mime_type.to_string(),
                    description: label.to_string(),
                    group: "document".to_string(),
                    extensions: vec![extension.to_string()],
                    score: Some(score),
                }),
            },
            0.80,
        )
    }

    fn validator_error(message: &str) -> Validator<FakeDetector> {
        Validator::new(
            FakeDetector {
                result: Err(message.to_string()),
            },
            0.80,
        )
    }

    impl FakeStore {
        fn account() -> AuthenticatedAccount {
            AuthenticatedAccount {
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                email: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                expires_at: "2099-01-01T00:00:00Z".to_string(),
            }
        }

        fn draft_mailbox() -> JmapMailbox {
            JmapMailbox {
                id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
                role: "drafts".to_string(),
                name: "Drafts".to_string(),
                sort_order: 10,
                total_emails: 1,
                unread_emails: 0,
            }
        }

        fn draft_email() -> JmapEmail {
            JmapEmail {
                id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
                thread_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                mailbox_id: Self::draft_mailbox().id,
                mailbox_role: "drafts".to_string(),
                mailbox_name: "Drafts".to_string(),
                received_at: "2026-04-18T10:00:00Z".to_string(),
                sent_at: None,
                from_address: "alice@example.test".to_string(),
                from_display: Some("Alice".to_string()),
                to: vec![lpe_storage::JmapEmailAddress {
                    address: "bob@example.test".to_string(),
                    display_name: Some("Bob".to_string()),
                }],
                cc: Vec::new(),
                bcc: vec![lpe_storage::JmapEmailAddress {
                    address: "hidden@example.test".to_string(),
                    display_name: None,
                }],
                subject: "Draft subject".to_string(),
                preview: "Draft preview".to_string(),
                body_text: "Draft body".to_string(),
                body_html_sanitized: None,
                unread: false,
                flagged: false,
                has_attachments: false,
                size_octets: 42,
                internet_message_id: Some("<draft@example.test>".to_string()),
                delivery_status: "draft".to_string(),
            }
        }
    }

    impl JmapStore for FakeStore {
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

        fn fetch_jmap_mailboxes<'a>(
            &'a self,
            _account_id: Uuid,
        ) -> StoreFuture<'a, Vec<JmapMailbox>> {
            let mailboxes = self.mailboxes.clone();
            Box::pin(async move { Ok(mailboxes) })
        }

        fn fetch_jmap_mailbox_ids<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>> {
            let ids = self.mailboxes.iter().map(|mailbox| mailbox.id).collect();
            Box::pin(async move { Ok(ids) })
        }

        fn query_jmap_email_ids<'a>(
            &'a self,
            _account_id: Uuid,
            mailbox_id: Option<Uuid>,
            _search_text: Option<&'a str>,
            position: u64,
            limit: u64,
        ) -> StoreFuture<'a, JmapEmailQuery> {
            let emails = self.emails.clone();
            Box::pin(async move {
                let mut ids = emails
                    .into_iter()
                    .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
                    .map(|email| email.id)
                    .collect::<Vec<_>>();
                let total = ids.len() as u64;
                ids = ids
                    .into_iter()
                    .skip(position as usize)
                    .take(limit as usize)
                    .collect();
                Ok(JmapEmailQuery { ids, total })
            })
        }

        fn fetch_all_jmap_email_ids<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>> {
            let ids = self.emails.iter().map(|email| email.id).collect();
            Box::pin(async move { Ok(ids) })
        }

        fn fetch_all_jmap_thread_ids<'a>(
            &'a self,
            _account_id: Uuid,
        ) -> StoreFuture<'a, Vec<Uuid>> {
            let ids = self
                .emails
                .iter()
                .map(|email| email.thread_id)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            Box::pin(async move { Ok(ids) })
        }

        fn create_jmap_mailbox<'a>(
            &'a self,
            input: JmapMailboxCreateInput,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, JmapMailbox> {
            Box::pin(async move {
                Ok(JmapMailbox {
                    id: Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
                    role: "".to_string(),
                    name: input.name,
                    sort_order: input.sort_order.unwrap_or(99),
                    total_emails: 0,
                    unread_emails: 0,
                })
            })
        }

        fn update_jmap_mailbox<'a>(
            &'a self,
            input: JmapMailboxUpdateInput,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, JmapMailbox> {
            Box::pin(async move {
                Ok(JmapMailbox {
                    id: input.mailbox_id,
                    role: "".to_string(),
                    name: input.name.unwrap_or_else(|| "Updated".to_string()),
                    sort_order: input.sort_order.unwrap_or(10),
                    total_emails: 0,
                    unread_emails: 0,
                })
            })
        }

        fn destroy_jmap_mailbox<'a>(
            &'a self,
            _account_id: Uuid,
            _mailbox_id: Uuid,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }

        fn fetch_jmap_emails<'a>(
            &'a self,
            _account_id: Uuid,
            ids: &'a [Uuid],
        ) -> StoreFuture<'a, Vec<JmapEmail>> {
            let emails = self.emails.clone();
            let ids = ids.to_vec();
            Box::pin(async move {
                Ok(ids
                    .into_iter()
                    .filter_map(|id| emails.iter().find(|email| email.id == id).cloned())
                    .collect())
            })
        }

        fn fetch_jmap_draft<'a>(
            &'a self,
            _account_id: Uuid,
            id: Uuid,
        ) -> StoreFuture<'a, Option<JmapEmail>> {
            let draft = self.emails.iter().find(|email| email.id == id).cloned();
            Box::pin(async move { Ok(draft) })
        }

        fn fetch_jmap_email_submissions<'a>(
            &'a self,
            _account_id: Uuid,
            ids: &'a [Uuid],
        ) -> StoreFuture<'a, Vec<JmapEmailSubmission>> {
            let submissions = vec![JmapEmailSubmission {
                id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
                email_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                thread_id: FakeStore::draft_email().thread_id,
                identity_email: FakeStore::account().email,
                envelope_mail_from: "alice@example.test".to_string(),
                envelope_rcpt_to: vec!["bob@example.test".to_string()],
                send_at: "2026-04-18T10:01:00Z".to_string(),
                undo_status: "final".to_string(),
                delivery_status: "queued".to_string(),
            }];
            let ids = ids.to_vec();
            Box::pin(async move {
                if ids.is_empty() {
                    Ok(submissions)
                } else {
                    Ok(submissions
                        .into_iter()
                        .filter(|submission| ids.contains(&submission.id))
                        .collect())
                }
            })
        }

        fn fetch_jmap_quota<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, JmapQuota> {
            Box::pin(async move {
                Ok(JmapQuota {
                    id: "mail".to_string(),
                    name: "Mail".to_string(),
                    used: 10,
                    hard_limit: 100,
                })
            })
        }

        fn save_jmap_upload_blob<'a>(
            &'a self,
            account_id: Uuid,
            media_type: &'a str,
            blob_bytes: &'a [u8],
        ) -> StoreFuture<'a, JmapUploadBlob> {
            let blob = JmapUploadBlob {
                id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
                account_id,
                media_type: media_type.to_string(),
                octet_size: blob_bytes.len() as u64,
                blob_bytes: blob_bytes.to_vec(),
            };
            self.uploads.lock().unwrap().push(blob.clone());
            Box::pin(async move { Ok(blob) })
        }

        fn fetch_jmap_upload_blob<'a>(
            &'a self,
            _account_id: Uuid,
            blob_id: Uuid,
        ) -> StoreFuture<'a, Option<JmapUploadBlob>> {
            let blob = self
                .uploads
                .lock()
                .unwrap()
                .iter()
                .find(|blob| blob.id == blob_id)
                .cloned();
            Box::pin(async move { Ok(blob) })
        }

        fn save_draft_message<'a>(
            &'a self,
            input: SubmitMessageInput,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, SavedDraftMessage> {
            self.saved_drafts.lock().unwrap().push(input.clone());
            Box::pin(async move {
                Ok(SavedDraftMessage {
                    message_id: input.draft_message_id.unwrap_or_else(Uuid::new_v4),
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

        fn submit_draft_message<'a>(
            &'a self,
            _account_id: Uuid,
            draft_message_id: Uuid,
            _source: &'a str,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, SubmittedMessage> {
            self.submitted_drafts.lock().unwrap().push(draft_message_id);
            Box::pin(async move {
                Ok(SubmittedMessage {
                    message_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                    thread_id: FakeStore::draft_email().thread_id,
                    account_id: FakeStore::account().account_id,
                    sent_mailbox_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff")
                        .unwrap(),
                    outbound_queue_id: Uuid::parse_str("11111111-2222-3333-4444-555555555555")
                        .unwrap(),
                    delivery_status: "queued".to_string(),
                })
            })
        }

        fn copy_jmap_email<'a>(
            &'a self,
            _account_id: Uuid,
            _message_id: Uuid,
            target_mailbox_id: Uuid,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, JmapEmail> {
            let mut email = FakeStore::draft_email();
            email.id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
            email.mailbox_id = target_mailbox_id;
            email.mailbox_role = "".to_string();
            email.mailbox_name = "Archive".to_string();
            Box::pin(async move { Ok(email) })
        }

        fn import_jmap_email<'a>(
            &'a self,
            input: JmapImportedEmailInput,
            _audit: AuditEntryInput,
        ) -> StoreFuture<'a, JmapEmail> {
            Box::pin(async move {
                Ok(JmapEmail {
                    id: Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap(),
                    thread_id: Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap(),
                    mailbox_id: input.mailbox_id,
                    mailbox_role: "".to_string(),
                    mailbox_name: "Imported".to_string(),
                    received_at: "2026-04-18T10:05:00Z".to_string(),
                    sent_at: None,
                    from_address: input.from_address,
                    from_display: input.from_display,
                    to: input
                        .to
                        .into_iter()
                        .map(|recipient| JmapEmailAddress {
                            address: recipient.address,
                            display_name: recipient.display_name,
                        })
                        .collect(),
                    cc: input
                        .cc
                        .into_iter()
                        .map(|recipient| JmapEmailAddress {
                            address: recipient.address,
                            display_name: recipient.display_name,
                        })
                        .collect(),
                    bcc: Vec::new(),
                    subject: input.subject,
                    preview: "Imported".to_string(),
                    body_text: input.body_text,
                    body_html_sanitized: None,
                    unread: false,
                    flagged: false,
                    has_attachments: false,
                    size_octets: input.size_octets,
                    internet_message_id: input.internet_message_id,
                    delivery_status: "stored".to_string(),
                })
            })
        }
    }

    #[tokio::test]
    async fn session_uses_existing_account_authentication() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });

        let session = service
            .session_document(Some("Bearer token"))
            .await
            .unwrap();

        assert_eq!(session.username, "alice@example.test");
        assert_eq!(session.api_url, "/jmap/api");
        assert!(session.capabilities.contains_key(JMAP_MAIL_CAPABILITY));
    }

    #[tokio::test]
    async fn email_set_creates_draft_through_canonical_storage() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "create": {
                                "k1": {
                                    "from": [{"email": "alice@example.test", "name": "Alice"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "bcc": [{"email": "hidden@example.test"}],
                                    "subject": "Hello",
                                    "textBody": "Draft body"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let saved = store.saved_drafts.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].from_address, "alice@example.test");
        assert_eq!(saved[0].bcc.len(), 1);
        assert!(response.created_ids.contains_key("k1"));
    }

    #[tokio::test]
    async fn email_submission_set_submits_existing_draft_and_returns_queued_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/set".to_string(),
                        json!({
                            "create": {
                                "send1": {
                                    "emailId": FakeStore::draft_email().id.to_string()
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let submitted = store.submitted_drafts.lock().unwrap();
        assert_eq!(submitted.as_slice(), &[FakeStore::draft_email().id]);
        let payload = &response.method_responses[0].1;
        assert_eq!(
            payload["created"]["send1"]["id"],
            Value::String("11111111-2222-3333-4444-555555555555".to_string())
        );
        assert_eq!(
            payload["created"]["send1"]["undoStatus"],
            Value::String("final".to_string())
        );
    }

    #[tokio::test]
    async fn mailbox_and_email_changes_return_existing_ids_from_initial_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_CORE_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/changes".to_string(),
                            json!({"sinceState": "0"}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/changes".to_string(),
                            json!({"sinceState": "0"}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["created"][0],
            Value::String(FakeStore::draft_mailbox().id.to_string())
        );
        assert_eq!(
            response.method_responses[1].1["created"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn identity_thread_and_submission_reads_are_available() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("Identity/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "Thread/get".to_string(),
                            json!({"ids": [FakeStore::draft_email().thread_id.to_string()]}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "EmailSubmission/get".to_string(),
                            json!({"ids": ["11111111-2222-3333-4444-555555555555"]}),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["email"],
            Value::String("alice@example.test".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["list"][0]["emailIds"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
        assert_eq!(
            response.method_responses[2].1["list"][0]["deliveryStatus"],
            Value::String("queued".to_string())
        );
    }

    #[tokio::test]
    async fn search_snippets_return_preview_for_requested_messages() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "SearchSnippet/get".to_string(),
                        json!({"emailIds": [FakeStore::draft_email().id.to_string()]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["preview"],
            Value::String("Draft preview".to_string())
        );
    }

    #[tokio::test]
    async fn mailbox_set_copy_import_and_quota_are_available() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        store.uploads.lock().unwrap().push(JmapUploadBlob {
            id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
            account_id: FakeStore::account().account_id,
            media_type: "message/rfc822".to_string(),
            octet_size: 82,
            blob_bytes: b"From: Alice <alice@example.test>\r\nTo: Bob <bob@example.test>\r\nSubject: Imported\r\n\r\nHello".to_vec(),
        });
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/set".to_string(),
                            json!({"create": {"m1": {"name": "Archive"}}}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/copy".to_string(),
                            json!({"fromAccountId": FakeStore::account().account_id.to_string(), "create": {"e1": {"emailId": FakeStore::draft_email().id.to_string(), "mailboxIds": {"99999999-9999-9999-9999-999999999999": true}}}}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/import".to_string(),
                            json!({"emails": {"i1": {"blobId": "77777777-7777-7777-7777-777777777777", "mailboxIds": {"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": true}}}}),
                            "c3".to_string(),
                        ),
                        JmapMethodCall("Quota/get".to_string(), json!({}), "c4".to_string()),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["created"]["m1"]["id"],
            Value::String("99999999-9999-9999-9999-999999999999".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["created"]["e1"]["id"],
            Value::String("66666666-6666-6666-6666-666666666666".to_string())
        );
        assert_eq!(
            response.method_responses[2].1["created"]["i1"]["id"],
            Value::String("55555555-5555-5555-5555-555555555555".to_string())
        );
        assert_eq!(
            response.method_responses[3].1["list"][0]["hardLimit"],
            Value::Number(100.into())
        );
    }

    #[tokio::test]
    async fn upload_and_download_use_authenticated_account() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store.clone(),
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let upload = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap();
        assert_eq!(
            upload["blobId"],
            Value::String("77777777-7777-7777-7777-777777777777".to_string())
        );

        let blob = service
            .handle_download(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "77777777-7777-7777-7777-777777777777",
            )
            .await
            .unwrap();
        assert_eq!(blob.media_type, "message/rfc822");
        assert_eq!(blob.blob_bytes, b"Subject: Hello\r\n\r\nBody".to_vec());
    }

    #[tokio::test]
    async fn upload_accepts_validated_matching_blob() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let upload = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap();

        assert_eq!(upload["type"], Value::String("message/rfc822".to_string()));
    }

    #[tokio::test]
    async fn upload_rejects_declared_mime_mismatch() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("application/pdf", "pdf", "pdf", 0.99),
        );

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"%PDF-1.7",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("JMAP upload blocked"));
    }

    #[tokio::test]
    async fn upload_rejects_unknown_type() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            Validator::new(
                FakeDetector {
                    result: Ok(MagikaDetection {
                        label: "unknown_binary".to_string(),
                        mime_type: "application/octet-stream".to_string(),
                        description: "unknown".to_string(),
                        group: "unknown".to_string(),
                        extensions: Vec::new(),
                        score: Some(0.99),
                    }),
                },
                0.80,
            ),
        );

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "application/octet-stream",
                b"\x00\x01\x02",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("JMAP upload blocked"));
    }

    #[tokio::test]
    async fn upload_surfaces_magika_failure_mode() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service =
            JmapService::new_with_validator(store, validator_error("Magika command failed"));

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Magika command failed"));
    }
}
