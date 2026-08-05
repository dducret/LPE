---
type: Rust Function
title: evaluate_attachment_policy_with_config
resource: LPE-CT/src/transport_policy.rs#L288-L391
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts
  - functions/LPE-CT/src/transport_policy/normalize_extension_token
  - functions/LPE-CT/src/transport_policy/match_exact_rule
  called_by:
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/submission/handle_submission_session
  - functions/LPE-CT/src/transport_policy/evaluate_attachment_policy
---

# Signature

`pub(crate) fn evaluate_attachment_policy_with_config<D: Detector>( config: &AttachmentPolicyConfig, validator: &Validator<D>, ingress_context: IngressContext, raw_message: &[u8], ) -> Result<AttachmentPolicyVerdict>`

# Calls

- [collect_mime_attachment_parts](../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)
- [normalize_extension_token](../../../../functions/LPE-CT/src/transport_policy/normalize_extension_token.md)
- [match_exact_rule](../../../../functions/LPE-CT/src/transport_policy/match_exact_rule.md)

# Called by

- [receive_message_with_validator](../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)
- [evaluate_attachment_policy](../../../../functions/LPE-CT/src/transport_policy/evaluate_attachment_policy.md)