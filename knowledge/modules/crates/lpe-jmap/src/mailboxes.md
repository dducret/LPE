---
type: Rust Module
title: mailboxes
resource: crates/lpe-jmap/src/mailboxes.rs#L1-L797
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-mailboxnamepolicy-mailboxsegment
  - external/serde-json-json-map-value
  - external/std-collections-hashmap-hashset
  - external/uuid-uuid
  - external/lpe-storage-auditentryinput-authenticatedaccount-jmapmailbox-jmapmailboxcreateinput-jmapmailboxupdateinput-mailboxaccountaccess
  - external/crate-convert-insert-if-error-set-error-parse-parse-uuid-parse-uuid-list-protocol-changesarguments-mailboxcreateinput-mailboxgetarguments-mailboxqueryarguments-mailboxsetarguments-mailboxupdateinput-querychangesarguments-state-changes-response-from-durable-with-cursor-changes-response-with-cursor-decode-query-state-encode-query-state-encode-query-state-reference-query-changes-response-from-diff-query-diff-for-kind-query-position-state-cursor-validate-query-state-token-durableobjectchange-jmapservice-default-get-limit-max-query-limit
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [handle_mailbox_get](../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get.md)
- [handle_mailbox_query](../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query.md)
- [handle_mailbox_query_changes](../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes.md)
- [handle_mailbox_changes](../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_changes.md)
- [handle_mailbox_set](../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set.md)
- [mailbox_properties](../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_properties.md)
- [mailbox_to_value](../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_to_value.md)
- [mailbox_account_may_submit](../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [mailbox_account_may_write](../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write.md)
- [mailbox_account_may_draft](../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_draft.md)
- [mailbox_is_user_managed](../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_is_user_managed.md)
- [ensure_mailbox_write](../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_write.md)
- [ensure_mailbox_draft_write](../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write.md)
- [parse_mailbox_create](../../../../functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_create.md)
- [parse_mailbox_update](../../../../functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_update.md)
- [parse_parent_id_field](../../../../functions/crates/lpe-jmap/src/mailboxes/parse_parent_id_field.md)
- [filter_mailboxes](../../../../functions/crates/lpe-jmap/src/mailboxes/filter_mailboxes.md)
- [validate_mailbox_set_names](../../../../functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names.md)
- [mailbox_parent_chain_contains](../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_parent_chain_contains.md)
- [mailbox_name_field](../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_name_field.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::{MailboxNamePolicy, MailboxSegment}`
- `serde_json::{json, Map, Value}`
- `std::collections::{HashMap, HashSet}`
- `uuid::Uuid`
- `lpe_storage::{
    AuditEntryInput, AuthenticatedAccount, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, MailboxAccountAccess,
}`
- `crate::{
    convert::insert_if,
    error::set_error,
    parse::{parse_uuid, parse_uuid_list},
    protocol::{
        ChangesArguments, MailboxCreateInput, MailboxGetArguments, MailboxQueryArguments,
        MailboxSetArguments, MailboxUpdateInput, QueryChangesArguments,
    },
    state::{
        changes_response_from_durable_with_cursor, changes_response_with_cursor,
        decode_query_state, encode_query_state, encode_query_state_reference,
        query_changes_response_from_diff, query_diff_for_kind, query_position, state_cursor,
        validate_query_state_token, DurableObjectChange,
    },
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)