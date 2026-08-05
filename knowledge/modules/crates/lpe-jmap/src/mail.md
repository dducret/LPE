---
type: Rust Module
title: mail
resource: crates/lpe-jmap/src/mail.rs#L1-L1427
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-storage-auditentryinput-authenticatedaccount-mailboxaccountaccess-saveddraftmessage-submitmessageinput
  - external/serde-json-json-map-value
  - external/std-collections-hashmap-hashset
  - external/uuid-uuid
  - external/crate-convert-map-existing-recipients-map-recipients-select-from-addresses-drafts-parse-draft-mutation-parse-email-copy-error-method-error-set-error-parse-parse-uuid-parse-uuid-list-protocol-changesarguments-emailcopyarguments-emailgetarguments-emailimportarguments-emailqueryarguments-emailqueryfilter-emailquerysort-emailsetarguments-emailsubmissiongetarguments-emailsubmissionqueryarguments-emailsubmissionqueryfilter-emailsubmissionquerysort-emailsubmissionsetarguments-identitygetarguments-querychangesarguments-quotagetarguments-searchsnippetgetarguments-threadgetarguments-threadqueryarguments-state-changes-response-changes-response-from-durable-with-cursor-changes-response-with-cursor-decode-query-state-encode-query-state-reference-query-changes-response-query-changes-response-from-diff-query-diff-for-kind-query-position-state-cursor-validate-query-state-token-durableobjectchange-validation-validate-query-sort-jmapservice-default-get-limit-max-query-limit-session-state
  - external/pub-crate-use-values
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [handle_email_query](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query.md)
- [handle_email_query_changes](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)
- [handle_email_get](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_get.md)
- [handle_email_changes](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_changes.md)
- [handle_email_copy](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy.md)
- [handle_email_import](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import.md)
- [handle_email_set](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set.md)
- [handle_email_submission_set](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set.md)
- [handle_email_submission_get](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get.md)
- [handle_email_submission_query](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query.md)
- [handle_email_submission_query_changes](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes.md)
- [handle_email_submission_changes](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_changes.md)
- [handle_identity_get](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_get.md)
- [handle_identity_changes](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_changes.md)
- [handle_thread_query](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query.md)
- [handle_thread_query_changes](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query_changes.md)
- [handle_thread_get](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get.md)
- [handle_thread_changes](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_changes.md)
- [handle_quota_get](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_quota_get.md)
- [handle_search_snippet_get](../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_search_snippet_get.md)
- [resolve_full_email_query_ids](../../../../functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_email_query_ids.md)
- [resolve_full_thread_query_ids](../../../../functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_thread_query_ids.md)
- [create_draft](../../../../functions/crates/lpe-jmap/src/mail/JmapService/create_draft.md)
- [update_draft](../../../../functions/crates/lpe-jmap/src/mail/JmapService/update_draft.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_storage::{
    AuditEntryInput, AuthenticatedAccount, MailboxAccountAccess, SavedDraftMessage,
    SubmitMessageInput,
}`
- `serde_json::{json, Map, Value}`
- `std::collections::{HashMap, HashSet}`
- `uuid::Uuid`
- `crate::{
    convert::{map_existing_recipients, map_recipients, select_from_addresses},
    drafts::{parse_draft_mutation, parse_email_copy},
    error::{method_error, set_error},
    parse::{parse_uuid, parse_uuid_list},
    protocol::{
        ChangesArguments, EmailCopyArguments, EmailGetArguments, EmailImportArguments,
        EmailQueryArguments, EmailQueryFilter, EmailQuerySort, EmailSetArguments,
        EmailSubmissionGetArguments, EmailSubmissionQueryArguments, EmailSubmissionQueryFilter,
        EmailSubmissionQuerySort, EmailSubmissionSetArguments, IdentityGetArguments,
        QueryChangesArguments, QuotaGetArguments, SearchSnippetGetArguments, ThreadGetArguments,
        ThreadQueryArguments,
    },
    state::{
        changes_response, changes_response_from_durable_with_cursor, changes_response_with_cursor,
        decode_query_state, encode_query_state_reference, query_changes_response,
        query_changes_response_from_diff, query_diff_for_kind, query_position, state_cursor,
        validate_query_state_token, DurableObjectChange,
    },
    validation::validate_query_sort,
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT, SESSION_STATE,
}`
- `pub(crate) use values::*`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)