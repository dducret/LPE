---
type: Rust Module
title: transport_policy
resource: LPE-CT/src/transport_policy.rs#L1-L741
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-domain-recipientverificationrequest-recipientverificationresponse-signedintegrationheaders-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/lpe-magika-collect-mime-attachment-parts-detector-ingresscontext-policydecision-validationrequest-validator
  - external/std-env
  - external/std-collections-btreemap-sync-mutex-oncelock-time-systemtime-unix-epoch
  - external/uuid-uuid
  - external/crate-integration-shared-secret-storage
  - external/super-evaluate-address-policy-evaluate-attachment-policy-evaluate-attachment-policy-with-config-verify-recipient-with-core-addresspolicyverdict-addressrole-attachmentpolicyconfig-attachmentpolicyverdict-recipientverificationconfig-recipientverificationverdict-recipient-verification-path
  - external/crate-env-test-lock
  - external/axum-routing-post-json-router
  - external/lpe-magika-detectionsource-detector-ingresscontext-magikadetection-validator
  - external/serde-json-value
  - external/std-sync-arc-mutex
  - external/tokio-net-tcplistener
  member_of:
  - packages/LPE-CT
---

# Contains

- [AddressRole](../../../classes/LPE-CT/src/transport_policy/AddressRole.md)
- [AddressPolicyVerdict](../../../classes/LPE-CT/src/transport_policy/AddressPolicyVerdict.md)
- [RecipientVerificationVerdict](../../../classes/LPE-CT/src/transport_policy/RecipientVerificationVerdict.md)
- [AttachmentPolicyVerdict](../../../classes/LPE-CT/src/transport_policy/AttachmentPolicyVerdict.md)
- [CachedRecipientVerdict](../../../classes/LPE-CT/src/transport_policy/CachedRecipientVerdict.md)
- [AddressPolicyConfig](../../../classes/LPE-CT/src/transport_policy/AddressPolicyConfig.md)
- [AttachmentPolicyConfig](../../../classes/LPE-CT/src/transport_policy/AttachmentPolicyConfig.md)
- [RecipientVerificationConfig](../../../classes/LPE-CT/src/transport_policy/RecipientVerificationConfig.md)
- [evaluate_address_policy_with_config](../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config.md)
- [evaluate_address_policy](../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy.md)
- [verify_recipient_with_core](../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)
- [evaluate_attachment_policy_with_config](../../../functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config.md)
- [evaluate_attachment_policy](../../../functions/LPE-CT/src/transport_policy/evaluate_attachment_policy.md)
- [address_policy_config_from_env](../../../functions/LPE-CT/src/transport_policy/address_policy_config_from_env.md)
- [attachment_policy_config_from_env](../../../functions/LPE-CT/src/transport_policy/attachment_policy_config_from_env.md)
- [parse_csv_env](../../../functions/LPE-CT/src/transport_policy/parse_csv_env.md)
- [normalize_extension_token](../../../functions/LPE-CT/src/transport_policy/normalize_extension_token.md)
- [normalize_address](../../../functions/LPE-CT/src/transport_policy/normalize_address.md)
- [match_address_rule](../../../functions/LPE-CT/src/transport_policy/match_address_rule.md)
- [match_exact_rule](../../../functions/LPE-CT/src/transport_policy/match_exact_rule.md)
- [cached_recipient_verdict](../../../functions/LPE-CT/src/transport_policy/cached_recipient_verdict.md)
- [store_recipient_verdict](../../../functions/LPE-CT/src/transport_policy/store_recipient_verdict.md)
- [recipient_verdict_label](../../../functions/LPE-CT/src/transport_policy/recipient_verdict_label.md)
- [recipient_verdict_detail](../../../functions/LPE-CT/src/transport_policy/recipient_verdict_detail.md)
- [recipient_verdict_from_record](../../../functions/LPE-CT/src/transport_policy/recipient_verdict_from_record.md)
- [unix_now](../../../functions/LPE-CT/src/transport_policy/unix_now.md)
- [FakeDetector](../../../classes/LPE-CT/src/transport_policy/FakeDetector.md)
- [detect](../../../functions/LPE-CT/src/transport_policy/FakeDetector/detector/detect.md)
- [address_policy_supports_exact_and_domain_rules](../../../functions/LPE-CT/src/transport_policy/address_policy_supports_exact_and_domain_rules.md)
- [attachment_policy_checks_extension_and_detected_type](../../../functions/LPE-CT/src/transport_policy/attachment_policy_checks_extension_and_detected_type.md)
- [attachment_policy_normalizes_leading_dot_extensions](../../../functions/LPE-CT/src/transport_policy/attachment_policy_normalizes_leading_dot_extensions.md)
- [recipient_verification_uses_internal_api](../../../functions/LPE-CT/src/transport_policy/recipient_verification_uses_internal_api.md)

# Imports

- `anyhow::Result`
- `lpe_domain::{
    RecipientVerificationRequest, RecipientVerificationResponse, SignedIntegrationHeaders,
    INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER,
    INTEGRATION_TIMESTAMP_HEADER,
}`
- `lpe_magika::{
    collect_mime_attachment_parts, Detector, IngressContext, PolicyDecision, ValidationRequest,
    Validator,
}`
- `std::env`
- `std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
}`
- `uuid::Uuid`
- `crate::{integration_shared_secret, storage}`
- `super::{
        evaluate_address_policy, evaluate_attachment_policy,
        evaluate_attachment_policy_with_config, verify_recipient_with_core, AddressPolicyVerdict,
        AddressRole, AttachmentPolicyConfig, AttachmentPolicyVerdict, RecipientVerificationConfig,
        RecipientVerificationVerdict, RECIPIENT_VERIFICATION_PATH,
    }`
- `crate::env_test_lock`
- `axum::{routing::post, Json, Router}`
- `lpe_magika::{DetectionSource, Detector, IngressContext, MagikaDetection, Validator}`
- `serde_json::Value`
- `std::sync::{Arc, Mutex}`
- `tokio::net::TcpListener`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)