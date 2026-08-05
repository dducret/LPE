---
type: Rust Method
title: deliver_inbound_message
resource: crates/lpe-storage/src/inbound.rs#L56-L253
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/util/normalize_subject
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message
  - functions/crates/lpe-storage/src/mail/parse_headers_map
  - functions/crates/lpe-storage/src/mail/parse_header_recipients
  - functions/crates/lpe-storage/src/submission/types/push_recipients
  - functions/crates/lpe-storage/src/submission/types/participants_normalized
  - functions/crates/lpe-storage/src/util/preview_text
  - functions/crates/lpe-storage/src/inbound/lock_inbound_trace_delivery
  - functions/crates/lpe-storage/src/inbound/existing_inbound_delivery_response_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/inbound/Storage/evaluate_inbound_sieve
  - functions/crates/lpe-storage/src/inbound/Storage/ensure_named_mailbox
  - functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/conversation_actions/Storage/apply_conversation_actions_to_jmap_email
  - functions/crates/lpe-storage/src/inbound/Storage/dispatch_sieve_followups
  - functions/crates/lpe-storage/src/inbound/inbound_delivery_response
---

# Signature

`pub async fn deliver_inbound_message( &self, mut request: InboundDeliveryRequest, ) -> Result<InboundDeliveryResponse>`

# Calls

- [normalize_subject](../../../../../../functions/crates/lpe-storage/src/util/normalize_subject.md)
- [parse_rfc822_message](../../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message.md)
- [parse_headers_map](../../../../../../functions/crates/lpe-storage/src/mail/parse_headers_map.md)
- [parse_header_recipients](../../../../../../functions/crates/lpe-storage/src/mail/parse_header_recipients.md)
- [push_recipients](../../../../../../functions/crates/lpe-storage/src/submission/types/push_recipients.md)
- [participants_normalized](../../../../../../functions/crates/lpe-storage/src/submission/types/participants_normalized.md)
- [preview_text](../../../../../../functions/crates/lpe-storage/src/util/preview_text.md)
- [lock_inbound_trace_delivery](../../../../../../functions/crates/lpe-storage/src/inbound/lock_inbound_trace_delivery.md)
- [existing_inbound_delivery_response_in_tx](../../../../../../functions/crates/lpe-storage/src/inbound/existing_inbound_delivery_response_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [evaluate_inbound_sieve](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/evaluate_inbound_sieve.md)
- [ensure_named_mailbox](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/ensure_named_mailbox.md)
- [ensure_mailbox](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox.md)
- [store_inbound_message_in_tx](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [apply_conversation_actions_to_jmap_email](../../../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/apply_conversation_actions_to_jmap_email.md)
- [dispatch_sieve_followups](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/dispatch_sieve_followups.md)
- [inbound_delivery_response](../../../../../../functions/crates/lpe-storage/src/inbound/inbound_delivery_response.md)