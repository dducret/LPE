---
type: Rust Function
title: evaluate_greylisting
resource: LPE-CT/src/smtp/anti_abuse.rs#L63-L151
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/stable_key_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
  - functions/LPE-CT/src/smtp/tests/greylisting_defers_first_triplet_then_allows_after_release_window
---

# Signature

`pub(in crate::smtp) async fn evaluate_greylisting( spool_dir: &Path, config: &RuntimeConfig, ip: IpAddr, mail_from: &str, rcpt_to: &[String], ) -> Result<Option<String>>`

# Calls

- [stable_key_id](../../../../../functions/LPE-CT/src/smtp/stable_key_id.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)
- [greylisting_defers_first_triplet_then_allows_after_release_window](../../../../../functions/LPE-CT/src/smtp/tests/greylisting_defers_first_triplet_then_allows_after_release_window.md)