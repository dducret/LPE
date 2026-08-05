---
type: Rust Module
title: dsn
resource: LPE-CT/src/smtp/dsn.rs#L1-L174
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/lpe-domain-transportdeliverystatus-transportdsnreport-transportretryadvice-transportroutedecision-transporttechnicalstatus
  - external/super-default-queue-for-status-retry-after-seconds-outboundexecution-queuedmessage-default-outbound-retry-after-seconds
  member_of:
  - packages/LPE-CT
---

# Contains

- [deferred_smtp_reply](../../../../functions/LPE-CT/src/smtp/dsn/deferred_smtp_reply.md)
- [deferred_smtp_reason](../../../../functions/LPE-CT/src/smtp/dsn/deferred_smtp_reason.md)
- [rejected_smtp_reply](../../../../functions/LPE-CT/src/smtp/dsn/rejected_smtp_reply.md)
- [sanitize_smtp_reply_detail](../../../../functions/LPE-CT/src/smtp/dsn/sanitize_smtp_reply_detail.md)
- [direct_mx_failure](../../../../functions/LPE-CT/src/smtp/dsn/direct_mx_failure.md)
- [is_permanent_direct_mx_error](../../../../functions/LPE-CT/src/smtp/dsn/is_permanent_direct_mx_error.md)
- [is_permanent_relay_error](../../../../functions/LPE-CT/src/smtp/dsn/is_permanent_relay_error.md)
- [parse_enhanced_status](../../../../functions/LPE-CT/src/smtp/dsn/parse_enhanced_status.md)

# Imports

- `lpe_domain::{
    TransportDeliveryStatus, TransportDsnReport, TransportRetryAdvice, TransportRouteDecision,
    TransportTechnicalStatus,
}`
- `super::{
    default_queue_for_status, retry_after_seconds, OutboundExecution, QueuedMessage,
    DEFAULT_OUTBOUND_RETRY_AFTER_SECONDS,
}`

# Member of

- [lpe-ct](../../../../packages/LPE-CT.md)