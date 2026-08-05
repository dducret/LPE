---
type: Rust Function
title: load_certificates
resource: LPE-CT/src/smtp/tls.rs#L138-L151
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/test_rca_outlook_trace_summary/FakePath/open
---

# Signature

`fn load_certificates(path: &str) -> Result<Vec<CertificateDer<'static>>>`

# Calls

- [open](../../../../../functions/tools/test_rca_outlook_trace_summary/FakePath/open.md)