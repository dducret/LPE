---
type: Rust Function
title: spawn_dummy_smtp_with_profile
resource: LPE-CT/src/smtp/tests.rs#L3397-L3405
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/tests/handle_dummy_smtp
  called_by:
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_relays_message
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay
  - functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp
---

# Signature

`async fn spawn_dummy_smtp_with_profile(profile: DummySmtpProfile) -> String`

# Calls

- [handle_dummy_smtp](../../../../../functions/LPE-CT/src/smtp/tests/handle_dummy_smtp.md)

# Called by

- [outbound_handoff_relays_message](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_relays_message.md)
- [outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay.md)
- [outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay](../../../../../functions/LPE-CT/src/smtp/tests/outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay.md)
- [outbound_handoff_bounces_on_permanent_rcpt_failure](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_bounces_on_permanent_rcpt_failure.md)
- [outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody.md)
- [spawn_dummy_smtp](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp.md)