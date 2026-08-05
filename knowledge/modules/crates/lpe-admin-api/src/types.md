---
type: Rust Module
title: types
resource: crates/lpe-admin-api/src/types.rs#L1-L825
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-http-statuscode-json
  - external/lpe-storage-accountapppassword-accountauthfactor-adminauthfactor-authenticatedaccount-authenticatedadmin-collaborationcollection-collaborationgrant-contactnamefields-contactsourcefields-delegateaccessobject-delegatefreebusymessageobject-freebusyblock-mailflowentry-mailboxdelegationoverview-sievescriptdocument-sievescriptsummary-tasklistgrant
  - external/serde-de-self-visitor-deserialize-deserializer-serialize
  - external/serde-json-value
  - external/uuid-uuid
  - external/super-patchclientcontactrequest-patchfield
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [PatchField](../../../../classes/crates/lpe-admin-api/src/types/PatchField.md)
- [default](../../../../functions/crates/lpe-admin-api/src/types/PatchField/default/default.md)
- [is_present](../../../../functions/crates/lpe-admin-api/src/types/PatchField/is_present.md)
- [deserialize](../../../../functions/crates/lpe-admin-api/src/types/PatchField/deserialize-de/deserialize.md)
- [PatchFieldVisitor](../../../../classes/crates/lpe-admin-api/src/types/PatchFieldVisitor.md)
- [expecting](../../../../functions/crates/lpe-admin-api/src/types/PatchFieldVisitor/visitor-de/expecting.md)
- [visit_none](../../../../functions/crates/lpe-admin-api/src/types/PatchFieldVisitor/visitor-de/visit_none.md)
- [visit_unit](../../../../functions/crates/lpe-admin-api/src/types/PatchFieldVisitor/visitor-de/visit_unit.md)
- [visit_some](../../../../functions/crates/lpe-admin-api/src/types/PatchFieldVisitor/visitor-de/visit_some.md)
- [UpsertPublicFolderItemRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertPublicFolderItemRequest.md)
- [CreatePublicFolderTreeRequest](../../../../classes/crates/lpe-admin-api/src/types/CreatePublicFolderTreeRequest.md)
- [CreatePublicFolderRequest](../../../../classes/crates/lpe-admin-api/src/types/CreatePublicFolderRequest.md)
- [UpdatePublicFolderRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdatePublicFolderRequest.md)
- [PublicFolderPermissionRequest](../../../../classes/crates/lpe-admin-api/src/types/PublicFolderPermissionRequest.md)
- [PublicFolderReplicaRequest](../../../../classes/crates/lpe-admin-api/src/types/PublicFolderReplicaRequest.md)
- [PublicFolderPerUserStatePatchRequest](../../../../classes/crates/lpe-admin-api/src/types/PublicFolderPerUserStatePatchRequest.md)
- [PublicFolderPerUserStatePatchBatchRequest](../../../../classes/crates/lpe-admin-api/src/types/PublicFolderPerUserStatePatchBatchRequest.md)
- [BootstrapAdminRequest](../../../../classes/crates/lpe-admin-api/src/types/BootstrapAdminRequest.md)
- [BootstrapAdminResponse](../../../../classes/crates/lpe-admin-api/src/types/BootstrapAdminResponse.md)
- [LocalAiHealthResponse](../../../../classes/crates/lpe-admin-api/src/types/LocalAiHealthResponse.md)
- [AttachmentSupportResponse](../../../../classes/crates/lpe-admin-api/src/types/AttachmentSupportResponse.md)
- [ReadinessCheck](../../../../classes/crates/lpe-admin-api/src/types/ReadinessCheck.md)
- [ReadinessResponse](../../../../classes/crates/lpe-admin-api/src/types/ReadinessResponse.md)
- [LoginResponse](../../../../classes/crates/lpe-admin-api/src/types/LoginResponse.md)
- [OidcMetadataResponse](../../../../classes/crates/lpe-admin-api/src/types/OidcMetadataResponse.md)
- [OidcStartResponse](../../../../classes/crates/lpe-admin-api/src/types/OidcStartResponse.md)
- [ClientLoginResponse](../../../../classes/crates/lpe-admin-api/src/types/ClientLoginResponse.md)
- [ClientOidcMetadataResponse](../../../../classes/crates/lpe-admin-api/src/types/ClientOidcMetadataResponse.md)
- [ClientOidcStartResponse](../../../../classes/crates/lpe-admin-api/src/types/ClientOidcStartResponse.md)
- [ClientOauthAccessTokenResponse](../../../../classes/crates/lpe-admin-api/src/types/ClientOauthAccessTokenResponse.md)
- [LoginRequest](../../../../classes/crates/lpe-admin-api/src/types/LoginRequest.md)
- [AdminAuthFactorsResponse](../../../../classes/crates/lpe-admin-api/src/types/AdminAuthFactorsResponse.md)
- [EnrollTotpRequest](../../../../classes/crates/lpe-admin-api/src/types/EnrollTotpRequest.md)
- [EnrollTotpResponse](../../../../classes/crates/lpe-admin-api/src/types/EnrollTotpResponse.md)
- [VerifyTotpRequest](../../../../classes/crates/lpe-admin-api/src/types/VerifyTotpRequest.md)
- [AccountAuthFactorsResponse](../../../../classes/crates/lpe-admin-api/src/types/AccountAuthFactorsResponse.md)
- [AccountAppPasswordsResponse](../../../../classes/crates/lpe-admin-api/src/types/AccountAppPasswordsResponse.md)
- [CreateAccountAppPasswordRequest](../../../../classes/crates/lpe-admin-api/src/types/CreateAccountAppPasswordRequest.md)
- [CreateClientOauthAccessTokenRequest](../../../../classes/crates/lpe-admin-api/src/types/CreateClientOauthAccessTokenRequest.md)
- [CreateAccountAppPasswordResponse](../../../../classes/crates/lpe-admin-api/src/types/CreateAccountAppPasswordResponse.md)
- [CreateAccountRequest](../../../../classes/crates/lpe-admin-api/src/types/CreateAccountRequest.md)
- [UpdateAccountRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdateAccountRequest.md)
- [CreateMailboxRequest](../../../../classes/crates/lpe-admin-api/src/types/CreateMailboxRequest.md)
- [CreatePstTransferJobRequest](../../../../classes/crates/lpe-admin-api/src/types/CreatePstTransferJobRequest.md)
- [CreateDomainRequest](../../../../classes/crates/lpe-admin-api/src/types/CreateDomainRequest.md)
- [UpdateDomainRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdateDomainRequest.md)
- [default_jmap_push_journal_retention_days](../../../../functions/crates/lpe-admin-api/src/types/default_jmap_push_journal_retention_days.md)
- [CreateAliasRequest](../../../../classes/crates/lpe-admin-api/src/types/CreateAliasRequest.md)
- [UpdateServerSettingsRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdateServerSettingsRequest.md)
- [UpdateSecuritySettingsRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdateSecuritySettingsRequest.md)
- [UpdateLocalAiSettingsRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdateLocalAiSettingsRequest.md)
- [UpdateAntispamSettingsRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdateAntispamSettingsRequest.md)
- [CreateStoragePoolRequest](../../../../classes/crates/lpe-admin-api/src/types/CreateStoragePoolRequest.md)
- [UpdateStoragePoolRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdateStoragePoolRequest.md)
- [UpdateStoragePolicyRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdateStoragePolicyRequest.md)
- [CreateServerAdministratorRequest](../../../../classes/crates/lpe-admin-api/src/types/CreateServerAdministratorRequest.md)
- [CreateFilterRuleRequest](../../../../classes/crates/lpe-admin-api/src/types/CreateFilterRuleRequest.md)
- [EmailTraceSearchRequest](../../../../classes/crates/lpe-admin-api/src/types/EmailTraceSearchRequest.md)
- [SubmitMessageRequest](../../../../classes/crates/lpe-admin-api/src/types/SubmitMessageRequest.md)
- [UpdateMessageFlagRequest](../../../../classes/crates/lpe-admin-api/src/types/UpdateMessageFlagRequest.md)
- [RecoverableItemsQueryRequest](../../../../classes/crates/lpe-admin-api/src/types/RecoverableItemsQueryRequest.md)
- [RestoreRecoverableItemRequest](../../../../classes/crates/lpe-admin-api/src/types/RestoreRecoverableItemRequest.md)
- [SubmitRecipientRequest](../../../../classes/crates/lpe-admin-api/src/types/SubmitRecipientRequest.md)
- [UpsertClientContactRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertClientContactRequest.md)
- [PatchClientContactRequest](../../../../classes/crates/lpe-admin-api/src/types/PatchClientContactRequest.md)
- [RecipientSuggestionQuery](../../../../classes/crates/lpe-admin-api/src/types/RecipientSuggestionQuery.md)
- [patch_contact_raw_vcard_distinguishes_omitted_null_and_value](../../../../functions/crates/lpe-admin-api/src/types/patch_contact_raw_vcard_distinguishes_omitted_null_and_value.md)
- [UpsertClientEventRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertClientEventRequest.md)
- [UpsertCollaborationGrantRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertCollaborationGrantRequest.md)
- [CollaborationOverviewResponse](../../../../classes/crates/lpe-admin-api/src/types/CollaborationOverviewResponse.md)
- [UpsertMailboxDelegationGrantRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertMailboxDelegationGrantRequest.md)
- [UpsertSenderDelegationGrantRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertSenderDelegationGrantRequest.md)
- [default_true](../../../../functions/crates/lpe-admin-api/src/types/default_true.md)
- [MailFlowResponse](../../../../classes/crates/lpe-admin-api/src/types/MailFlowResponse.md)
- [SieveOverviewResponse](../../../../classes/crates/lpe-admin-api/src/types/SieveOverviewResponse.md)
- [UpsertSieveScriptRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertSieveScriptRequest.md)
- [RenameSieveScriptRequest](../../../../classes/crates/lpe-admin-api/src/types/RenameSieveScriptRequest.md)
- [SetActiveSieveScriptRequest](../../../../classes/crates/lpe-admin-api/src/types/SetActiveSieveScriptRequest.md)
- [MailboxDelegationResponse](../../../../classes/crates/lpe-admin-api/src/types/MailboxDelegationResponse.md)
- [FreeBusyQuery](../../../../classes/crates/lpe-admin-api/src/types/FreeBusyQuery.md)
- [FreeBusyResponse](../../../../classes/crates/lpe-admin-api/src/types/FreeBusyResponse.md)
- [UpsertClientTaskRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertClientTaskRequest.md)
- [UpsertClientNoteRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertClientNoteRequest.md)
- [UpsertJournalEntryRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertJournalEntryRequest.md)
- [ReminderQueryRequest](../../../../classes/crates/lpe-admin-api/src/types/ReminderQueryRequest.md)
- [UpsertSearchFolderRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertSearchFolderRequest.md)
- [UpsertTaskListGrantRequest](../../../../classes/crates/lpe-admin-api/src/types/UpsertTaskListGrantRequest.md)

# Imports

- `axum::{http::StatusCode, Json}`
- `lpe_storage::{
    AccountAppPassword, AccountAuthFactor, AdminAuthFactor, AuthenticatedAccount,
    AuthenticatedAdmin, CollaborationCollection, CollaborationGrant, ContactNameFields,
    ContactSourceFields, DelegateAccessObject, DelegateFreeBusyMessageObject, FreeBusyBlock,
    MailFlowEntry, MailboxDelegationOverview, SieveScriptDocument, SieveScriptSummary,
    TaskListGrant,
}`
- `serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize,
}`
- `serde_json::Value`
- `uuid::Uuid`
- `super::{PatchClientContactRequest, PatchField}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)