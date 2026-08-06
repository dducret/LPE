---
type: Rust Module
title: service
resource: crates/lpe-exchange/src/service.rs#L1-L1087
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/axum-body-to-bytes-body-bytes-extract-state-http-header-content-length-www-authenticate-headermap-headervalue-method-statuscode-uri-response-intoresponse-response-router
  - external/base64-engine-general-purpose-standard-as-base64-standard-engine-as
  - external/lpe-domain-mail-format-format-mailbox-address-quote-header-parameter-sanitize-header-value-displaynamepolicy
  - external/lpe-domain-normalization
  - external/lpe-magika-detector-expectedkind-ingresscontext-policydecision-systemdetector-validationrequest-validator
  - external/lpe-mail-auth-authenticate-account-accountprincipal
  - external/lpe-storage-calendar-attendee-labels-parse-calendar-participants-metadata-serialize-calendar-participants-metadata-accessiblecontact-accessibleevent-activesyncattachment-activesyncattachmentcontent-attachmentuploadinput-auditentryinput-calendarorganizermetadata-calendarparticipantmetadata-calendarparticipantsmetadata-clientreminder-clienttask-collaborationcollection-collaborationgrant-collaborationresourcekind-collaborationrights-contactnamefields-createpublicfolderinput-jmapemail-jmapemailaddress-jmapemailfollowupupdate-jmapimportedemailinput-jmapmailbox-jmapmailboxcreateinput-jmapmailboxupdateinput-mailboxrule-managedretentionfoldercreateinput-publicfolder-publicfolderitem-reminderquery-storage-submitmessageinput-submittedrecipientinput-updatepublicfolderinput-upsertclientcontactinput-upsertclienteventinput-upsertclienttaskinput-upsertpublicfolderiteminput
  - external/std-collections-hashmap-hashset
  - external/std-time-instant
  - external/uuid-uuid
  - external/crate-build-info-ews-types-ewsdeletetype-ewsdistinguishedfolderidname-ewsoofstate-ewstaskstatus-mapi-self-mapiendpoint-store-ewsdelegate-ewsdelegatepreferences-ewsdiscoverysearchconfig-ewsdiscoverysearchresult-ewsholdmailbox-ewsimgroup-ewsimgroupmember-ewsimlist-ewsimmemberinput-ewsmailappmanifest-ewsmailapptokenevent-ewsmessagetrackingreport-ewsmessagetrackingreportdetail-ewsnonindexablereport-ewsretentionpolicytag-ewssearchablemailbox-ewstransferjob-ewsunifiedmessagingcall-ewsuserconfiguration-ewsuserconfigurationkey-exchangeaddressbookdirectorykind-exchangeaddressbookentry-exchangeaddressbookentrydetails-exchangeaddressbookentrykind-exchangestore-upsertewsdelegateinput-upsertewsuserconfigurationinput
  - external/ews-availability
  - external/ews-calendar
  - external/ews-contacts
  - external/ews-diagnostics
  - external/pub-crate-use-ews-errors-error-response
  - external/ews-fields
  - external/ews-folders
  - external/ews-ids
  - external/ews-mail
  - external/ews-mailboxes
  - external/ews-mime
  - external/ews-oof
  - external/ews-public-folders
  - external/ews-request-ids
  - external/ews-responses
  - external/ews-sync-state
  - external/ews-tasks
  - external/ews-xml
  - external/http-routes
  - external/http-utils
  - external/rpc-proxy-auth
  - external/pub-crate-use-rpc-proxy-channels-mark-rpc-proxy-out-endpoint-bind-ack
  - external/pub-crate-use-rpc-proxy-requests-is-rpc-proxy-in-data-channel-request
  - external/rpc-proxy-requests-is-rpc-proxy-echo-request-is-rpc-proxy-endpoint-ping
  - external/rpc-proxy-rts
  - external/rpc-proxy-stream
  - external/pub-crate-use-rpc-proxy-stream-rpc-proxy-in-channel-response-for-buffer-rpc-proxy-in-channel-response-for-endpoint-query-rpc-proxy-in-channel-response-for-endpoint-query-with-store
  - external/transport-diagnostics
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [router](../../../../functions/crates/lpe-exchange/src/service/router.md)
- [ExchangeService](../../../../classes/crates/lpe-exchange/src/service/ExchangeService.md)
- [new](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/new.md)
- [new_with_validator](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/new_with_validator.md)
- [options_handler](../../../../functions/crates/lpe-exchange/src/service/options_handler.md)
- [post_handler](../../../../functions/crates/lpe-exchange/src/service/post_handler.md)
- [mapi_options_handler](../../../../functions/crates/lpe-exchange/src/service/mapi_options_handler.md)
- [mapi_emsmdb_post_handler](../../../../functions/crates/lpe-exchange/src/service/mapi_emsmdb_post_handler.md)
- [mapi_nspi_post_handler](../../../../functions/crates/lpe-exchange/src/service/mapi_nspi_post_handler.md)
- [mapi_post_handler](../../../../functions/crates/lpe-exchange/src/service/mapi_post_handler.md)
- [rpc_proxy_handler](../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_handler.md)
- [requested_mailbox_folder_ids](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/requested_mailbox_folder_ids.md)
- [create_folder](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/create_folder.md)
- [create_folder_path](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path.md)
- [copy_folder](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder.md)
- [empty_folder](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_folder.md)
- [move_folder](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/move_folder.md)
- [update_folder](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)
- [copy_mailbox_folder_tree](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_mailbox_folder_tree.md)
- [empty_mailbox_folder](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_mailbox_folder.md)
- [copy_public_folder_tree](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_public_folder_tree.md)
- [empty_public_folder](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_public_folder.md)
- [delete_folder](../../../../functions/crates/lpe-exchange/src/service/ExchangeService/delete_folder.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `axum::{
    body::{to_bytes, Body, Bytes},
    extract::State,
    http::{
        header::{CONTENT_LENGTH, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, Method, StatusCode, Uri,
    },
    response::{IntoResponse, Response},
    Router,
}`
- `base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _}`
- `lpe_domain::mail_format::{
    format_mailbox_address, quote_header_parameter, sanitize_header_value, DisplayNamePolicy,
}`
- `lpe_domain::normalization`
- `lpe_magika::{
    Detector, ExpectedKind, IngressContext, PolicyDecision, SystemDetector, ValidationRequest,
    Validator,
}`
- `lpe_mail_auth::{authenticate_account, AccountPrincipal}`
- `lpe_storage::{
    calendar_attendee_labels, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, AccessibleContact, AccessibleEvent,
    ActiveSyncAttachment, ActiveSyncAttachmentContent, AttachmentUploadInput, AuditEntryInput,
    CalendarOrganizerMetadata, CalendarParticipantMetadata, CalendarParticipantsMetadata,
    ClientReminder, ClientTask, CollaborationCollection, CollaborationGrant,
    CollaborationResourceKind, CollaborationRights, ContactNameFields, CreatePublicFolderInput,
    JmapEmail, JmapEmailAddress, JmapEmailFollowupUpdate, JmapImportedEmailInput, JmapMailbox,
    JmapMailboxCreateInput, JmapMailboxUpdateInput, MailboxRule, ManagedRetentionFolderCreateInput,
    PublicFolder, PublicFolderItem, ReminderQuery, Storage, SubmitMessageInput,
    SubmittedRecipientInput, UpdatePublicFolderInput, UpsertClientContactInput,
    UpsertClientEventInput, UpsertClientTaskInput, UpsertPublicFolderItemInput,
}`
- `std::collections::{HashMap, HashSet}`
- `std::time::Instant`
- `uuid::Uuid`
- `crate::{
    build_info,
    ews_types::{EwsDeleteType, EwsDistinguishedFolderIdName, EwsOofState, EwsTaskStatus},
    mapi::{self, MapiEndpoint},
    store::{
        EwsDelegate, EwsDelegatePreferences, EwsDiscoverySearchConfig, EwsDiscoverySearchResult,
        EwsHoldMailbox, EwsImGroup, EwsImGroupMember, EwsImList, EwsImMemberInput,
        EwsMailAppManifest, EwsMailAppTokenEvent, EwsMessageTrackingReport,
        EwsMessageTrackingReportDetail, EwsNonIndexableReport, EwsRetentionPolicyTag,
        EwsSearchableMailbox, EwsTransferJob, EwsUnifiedMessagingCall, EwsUserConfiguration,
        EwsUserConfigurationKey, ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry,
        ExchangeAddressBookEntryDetails, ExchangeAddressBookEntryKind, ExchangeStore,
        UpsertEwsDelegateInput, UpsertEwsUserConfigurationInput,
    },
}`
- `ews::availability::*`
- `ews::calendar::*`
- `ews::contacts::*`
- `ews::diagnostics::*`
- `pub(crate) use ews::errors::error_response`
- `ews::fields::*`
- `ews::folders::*`
- `ews::ids::*`
- `ews::mail::*`
- `ews::mailboxes::*`
- `ews::mime::*`
- `ews::oof::*`
- `ews::public_folders::*`
- `ews::request_ids::*`
- `ews::responses::*`
- `ews::sync_state::*`
- `ews::tasks::*`
- `ews::xml::*`
- `http_routes::*`
- `http_utils::*`
- `rpc_proxy_auth::*`
- `pub(crate) use rpc_proxy_channels::mark_rpc_proxy_out_endpoint_bind_ack`
- `pub(crate) use rpc_proxy_requests::is_rpc_proxy_in_data_channel_request`
- `rpc_proxy_requests::{is_rpc_proxy_echo_request, is_rpc_proxy_endpoint_ping}`
- `rpc_proxy_rts::*`
- `rpc_proxy_stream::*`
- `pub(crate) use rpc_proxy_stream::{
    rpc_proxy_in_channel_response_for_buffer, rpc_proxy_in_channel_response_for_endpoint_query,
    rpc_proxy_in_channel_response_for_endpoint_query_with_store,
}`
- `transport_diagnostics::*`

# Member of

- [lpe-exchange](../../../../packages/crates/lpe-exchange.md)