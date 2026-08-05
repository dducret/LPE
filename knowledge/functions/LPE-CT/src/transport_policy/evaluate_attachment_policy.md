---
type: Rust Function
title: evaluate_attachment_policy
resource: LPE-CT/src/transport_policy.rs#L394-L401
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/transport_policy/attachment_policy_config_from_env
  - functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config
---

# Signature

`pub(crate) fn evaluate_attachment_policy<D: Detector>( validator: &Validator<D>, ingress_context: IngressContext, raw_message: &[u8], ) -> Result<AttachmentPolicyVerdict>`

# Calls

- [attachment_policy_config_from_env](../../../../functions/LPE-CT/src/transport_policy/attachment_policy_config_from_env.md)
- [evaluate_attachment_policy_with_config](../../../../functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config.md)