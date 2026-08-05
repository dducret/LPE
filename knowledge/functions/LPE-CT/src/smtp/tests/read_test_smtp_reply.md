---
type: Rust Function
title: read_test_smtp_reply
resource: LPE-CT/src/smtp/tests.rs#L1596-L1611
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply
---

# Signature

`async fn read_test_smtp_reply<R>(reader: &mut BufReader<R>) -> String where R: tokio::io::AsyncRead + Unpin,`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [smtp_starttls_upgrades_to_tls_after_ready_reply](../../../../../functions/LPE-CT/src/smtp/tests/smtp_starttls_upgrades_to_tls_after_ready_reply.md)