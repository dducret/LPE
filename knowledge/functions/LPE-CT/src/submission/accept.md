---
type: Rust Function
title: accept
resource: LPE-CT/src/submission.rs#L896-L909
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`async fn accept( State(capture): State<Capture>, headers: HeaderMap, ) -> Json<SmtpSubmissionResponse>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)