---
type: Rust Function
title: smtp_xoauth_is_rejected_before_core_auth_request
resource: LPE-CT/src/submission.rs#L871-L889
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/LPE-CT/src/submission/authenticate_smtp_client
---

# Signature

`async fn smtp_xoauth_is_rejected_before_core_auth_request()`

# Calls

- [build](../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [empty](../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [authenticate_smtp_client](../../../../functions/LPE-CT/src/submission/authenticate_smtp_client.md)