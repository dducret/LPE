---
type: Rust Function
title: apply_authentication_scores
resource: LPE-CT/src/smtp/auth.rs#L56-L142
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
  - functions/LPE-CT/src/smtp/tests/auth_score_application_penalizes_failures_and_alignment_gaps
---

# Signature

`pub(in crate::smtp) fn apply_authentication_scores( assessment: &AuthenticationAssessment, spam_score: &mut f32, security_score: &mut f32, decision_trace: &mut Vec<DecisionTraceEntry>, )`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)
- [auth_score_application_penalizes_failures_and_alignment_gaps](../../../../../functions/LPE-CT/src/smtp/tests/auth_score_application_penalizes_failures_and_alignment_gaps.md)