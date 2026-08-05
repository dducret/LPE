---
type: Rust Function
title: finalize_policy_decision
resource: LPE-CT/src/smtp/inbound_policy.rs#L284-L398
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
  - functions/LPE-CT/src/smtp/tests/decide_auth_policy
---

# Signature

`pub(in crate::smtp) fn finalize_policy_decision( config: &RuntimeConfig, auth_assessment: Option<&AuthenticationAssessment>, spam_score: f32, security_score: f32, reputation_score: i32, decision_trace: &mut Vec<DecisionTraceEntry>, mut defer_reasons: Vec<String>, mut reject_reasons: Vec<String>, mut quarantine_reasons: Vec<String>, ) -> (FilterAction, Option<String>)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)
- [decide_auth_policy](../../../../../functions/LPE-CT/src/smtp/tests/decide_auth_policy.md)