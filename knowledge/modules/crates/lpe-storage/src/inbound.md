---
type: Rust Module
title: inbound
resource: crates/lpe-storage/src/inbound.rs#L1-L856
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/lpe-core-sieve-evaluate-script-executionoutcome-as-sieveexecutionoutcome-messagecontext-as-sievemessagecontext-vacationaction
  - external/lpe-domain-inbounddeliveryrequest-inbounddeliveryresponse-mailboxdisplayname-mailboxnamepolicy
  - external/sha2-digest-sha256
  - external/sqlx-postgres-row
  - external/std-collections-btreemap-btreeset
  - external/uuid-uuid
  - external/crate-mail-parse-header-recipients-parse-headers-map-parse-rfc822-message
  - external/crate-shared-allocate-uid-validity
  - external/crate-submission-attachmentuploadinput-auditentryinput-storage-submittedrecipientinput
  - external/super
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [SieveFollowUp](../../../../classes/crates/lpe-storage/src/inbound/SieveFollowUp.md)
- [verify_local_recipient](../../../../functions/crates/lpe-storage/src/inbound/Storage/verify_local_recipient.md)
- [deliver_inbound_message](../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
- [evaluate_inbound_sieve](../../../../functions/crates/lpe-storage/src/inbound/Storage/evaluate_inbound_sieve.md)
- [dispatch_sieve_followups](../../../../functions/crates/lpe-storage/src/inbound/Storage/dispatch_sieve_followups.md)
- [should_send_sieve_vacation](../../../../functions/crates/lpe-storage/src/inbound/Storage/should_send_sieve_vacation.md)
- [store_inbound_message_in_tx](../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [ensure_named_mailbox](../../../../functions/crates/lpe-storage/src/inbound/Storage/ensure_named_mailbox.md)
- [lock_inbound_trace_delivery](../../../../functions/crates/lpe-storage/src/inbound/lock_inbound_trace_delivery.md)
- [existing_inbound_delivery_response_in_tx](../../../../functions/crates/lpe-storage/src/inbound/existing_inbound_delivery_response_in_tx.md)
- [duplicate_inbound_delivery_response](../../../../functions/crates/lpe-storage/src/inbound/duplicate_inbound_delivery_response.md)
- [inbound_delivery_response](../../../../functions/crates/lpe-storage/src/inbound/inbound_delivery_response.md)
- [inbound_trace_advisory_lock_keys](../../../../functions/crates/lpe-storage/src/inbound/inbound_trace_advisory_lock_keys.md)
- [estimate_generated_message_size](../../../../functions/crates/lpe-storage/src/inbound/estimate_generated_message_size.md)
- [hash_sieve_vacation_key](../../../../functions/crates/lpe-storage/src/inbound/hash_sieve_vacation_key.md)
- [inbound_trace_id_helpers_normalize_whitespace](../../../../functions/crates/lpe-storage/src/inbound/inbound_trace_id_helpers_normalize_whitespace.md)
- [duplicate_inbound_response_returns_committed_receipt](../../../../functions/crates/lpe-storage/src/inbound/duplicate_inbound_response_returns_committed_receipt.md)
- [inbound_response_rejects_when_no_recipient_was_accepted](../../../../functions/crates/lpe-storage/src/inbound/inbound_response_rejects_when_no_recipient_was_accepted.md)
- [inbound_response_accepts_when_at_least_one_recipient_was_accepted](../../../../functions/crates/lpe-storage/src/inbound/inbound_response_accepts_when_at_least_one_recipient_was_accepted.md)

# Imports

- `anyhow::{bail, Result}`
- `lpe_core::sieve::{
    evaluate_script, ExecutionOutcome as SieveExecutionOutcome,
    MessageContext as SieveMessageContext, VacationAction,
}`
- `lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, MailboxDisplayName, MailboxNamePolicy,
}`
- `sha2::{Digest, Sha256}`
- `sqlx::{Postgres, Row}`
- `std::collections::{BTreeMap, BTreeSet}`
- `uuid::Uuid`
- `crate::mail::{parse_header_recipients, parse_headers_map, parse_rfc822_message}`
- `crate::shared::allocate_uid_validity`
- `crate::{submission, AttachmentUploadInput, AuditEntryInput, Storage, SubmittedRecipientInput}`
- `super::*`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)