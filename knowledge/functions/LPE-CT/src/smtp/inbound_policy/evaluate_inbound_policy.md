---
type: Rust Function
title: evaluate_inbound_policy
resource: LPE-CT/src/smtp/inbound_policy.rs#L3-L282
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/policy/inbound_domain_policy
  - functions/LPE-CT/src/smtp/reputation/load_reputation_score
  - functions/LPE-CT/src/smtp/should_quarantine
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/anti_abuse/evaluate_greylisting
  - functions/LPE-CT/src/smtp/anti_abuse/query_dnsbl
  - functions/LPE-CT/src/smtp/auth/authenticate_message
  - functions/LPE-CT/src/smtp/auth/apply_authentication_scores
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/crates/lpe-magika/src/mime/extract_visible_text
  - functions/LPE-CT/src/smtp/bayes/score_bayespam
  - functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy
  - functions/LPE-CT/src/smtp/inbound_policy/finalize_policy_decision
  called_by:
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
---

# Signature

`pub(in crate::smtp) async fn evaluate_inbound_policy( spool_dir: &Path, config: &RuntimeConfig, peer_ip: Option<IpAddr>, helo: &str, mail_from: &str, rcpt_to: &[String], message_bytes: &[u8], ) -> Result<FilterVerdict>`

# Calls

- [inbound_domain_policy](../../../../../functions/LPE-CT/src/smtp/policy/inbound_domain_policy.md)
- [load_reputation_score](../../../../../functions/LPE-CT/src/smtp/reputation/load_reputation_score.md)
- [should_quarantine](../../../../../functions/LPE-CT/src/smtp/should_quarantine.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [evaluate_greylisting](../../../../../functions/LPE-CT/src/smtp/anti_abuse/evaluate_greylisting.md)
- [query_dnsbl](../../../../../functions/LPE-CT/src/smtp/anti_abuse/query_dnsbl.md)
- [authenticate_message](../../../../../functions/LPE-CT/src/smtp/auth/authenticate_message.md)
- [apply_authentication_scores](../../../../../functions/LPE-CT/src/smtp/auth/apply_authentication_scores.md)
- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [extract_visible_text](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_text.md)
- [score_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/score_bayespam.md)
- [evaluate_antivirus_policy](../../../../../functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy.md)
- [finalize_policy_decision](../../../../../functions/LPE-CT/src/smtp/inbound_policy/finalize_policy_decision.md)

# Called by

- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)