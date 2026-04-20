use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct JmapApiRequest {
    #[serde(rename = "using", default)]
    pub using_capabilities: Vec<String>,
    #[serde(rename = "methodCalls", default)]
    pub method_calls: Vec<JmapMethodCall>,
}

#[derive(Debug, Deserialize)]
pub struct JmapMethodCall(pub String, pub Value, pub String);

#[derive(Debug, Serialize)]
pub struct JmapApiResponse {
    #[serde(rename = "methodResponses")]
    pub method_responses: Vec<JmapMethodResponse>,
    #[serde(rename = "createdIds", skip_serializing_if = "HashMap::is_empty")]
    pub created_ids: HashMap<String, String>,
    #[serde(rename = "sessionState")]
    pub session_state: String,
}

#[derive(Debug, Serialize)]
pub struct JmapMethodResponse(pub String, pub Value, pub String);

#[derive(Debug, Serialize)]
pub struct SessionDocument {
    pub capabilities: HashMap<String, Value>,
    pub accounts: HashMap<String, SessionAccount>,
    #[serde(rename = "primaryAccounts")]
    pub primary_accounts: HashMap<String, String>,
    pub username: String,
    #[serde(rename = "apiUrl")]
    pub api_url: String,
    #[serde(rename = "downloadUrl")]
    pub download_url: String,
    #[serde(rename = "uploadUrl")]
    pub upload_url: String,
    #[serde(rename = "eventSourceUrl")]
    pub event_source_url: Option<String>,
    pub state: String,
}

#[derive(Debug, Serialize)]
pub struct SessionAccount {
    pub name: String,
    #[serde(rename = "isPersonal")]
    pub is_personal: bool,
    #[serde(rename = "isReadOnly")]
    pub is_read_only: bool,
    #[serde(rename = "accountCapabilities")]
    pub account_capabilities: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
    pub properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxQueryArguments {
    pub account_id: Option<String>,
    pub position: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryChangesArguments<F = Value, S = Value> {
    pub account_id: Option<String>,
    pub since_query_state: String,
    pub max_changes: Option<u64>,
    pub filter: Option<F>,
    pub sort: Option<Vec<S>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxSetArguments {
    pub account_id: Option<String>,
    pub create: Option<HashMap<String, Value>>,
    pub update: Option<HashMap<String, Value>>,
    pub destroy: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangesArguments {
    pub account_id: Option<String>,
    pub since_state: String,
    pub max_changes: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
    pub properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailQueryArguments {
    pub account_id: Option<String>,
    pub position: Option<u64>,
    pub limit: Option<u64>,
    pub filter: Option<EmailQueryFilter>,
    pub sort: Option<Vec<EmailQuerySort>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailQueryFilter {
    pub in_mailbox: Option<String>,
    pub text: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailQuerySort {
    pub property: String,
    pub is_ascending: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailSetArguments {
    pub account_id: Option<String>,
    pub create: Option<HashMap<String, Value>>,
    pub update: Option<HashMap<String, Value>>,
    pub destroy: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailSubmissionSetArguments {
    pub account_id: Option<String>,
    pub create: Option<HashMap<String, Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailSubmissionGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
    pub properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
    pub properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
    pub properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueryArguments {
    pub account_id: Option<String>,
    pub position: Option<u64>,
    pub limit: Option<u64>,
    pub filter: Option<EmailQueryFilter>,
    pub sort: Option<Vec<EmailQuerySort>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchSnippetGetArguments {
    pub account_id: Option<String>,
    pub email_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailCopyArguments {
    pub from_account_id: String,
    pub account_id: Option<String>,
    pub create: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailImportArguments {
    pub account_id: Option<String>,
    pub emails: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressBookGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
    pub properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressBookQueryArguments {
    pub account_id: Option<String>,
    pub position: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactCardGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
    pub properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactCardQueryArguments {
    pub account_id: Option<String>,
    pub position: Option<u64>,
    pub limit: Option<u64>,
    pub filter: Option<ContactCardQueryFilter>,
    pub sort: Option<Vec<EntityQuerySort>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactCardQueryFilter {
    pub in_address_book: Option<String>,
    pub text: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactCardSetArguments {
    pub account_id: Option<String>,
    pub create: Option<HashMap<String, Value>>,
    pub update: Option<HashMap<String, Value>>,
    pub destroy: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalendarGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
    pub properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalendarQueryArguments {
    pub account_id: Option<String>,
    pub position: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalendarEventGetArguments {
    pub account_id: Option<String>,
    pub ids: Option<Vec<String>>,
    pub properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalendarEventQueryArguments {
    pub account_id: Option<String>,
    pub position: Option<u64>,
    pub limit: Option<u64>,
    pub filter: Option<CalendarEventQueryFilter>,
    pub sort: Option<Vec<EntityQuerySort>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalendarEventQueryFilter {
    pub in_calendar: Option<String>,
    pub text: Option<String>,
    pub after: Option<String>,
    pub before: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalendarEventSetArguments {
    pub account_id: Option<String>,
    pub create: Option<HashMap<String, Value>>,
    pub update: Option<HashMap<String, Value>>,
    pub destroy: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityQuerySort {
    pub property: String,
    pub is_ascending: Option<bool>,
}

#[derive(Debug)]
pub struct MailboxCreateInput {
    pub name: String,
    pub sort_order: Option<i32>,
}

#[derive(Debug)]
pub struct MailboxUpdateInput {
    pub name: Option<String>,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Default, Clone)]
pub struct DraftMutation {
    pub from: Option<Vec<EmailAddressInput>>,
    pub to: Option<Vec<EmailAddressInput>>,
    pub cc: Option<Vec<EmailAddressInput>>,
    pub bcc: Option<Vec<EmailAddressInput>>,
    pub subject: Option<String>,
    pub text_body: Option<String>,
    pub html_body: Option<Option<String>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EmailAddressInput {
    pub email: String,
    #[serde(default)]
    pub name: Option<String>,
}
