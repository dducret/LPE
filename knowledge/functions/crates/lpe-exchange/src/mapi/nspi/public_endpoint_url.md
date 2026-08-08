---
type: Rust Function
title: public_endpoint_url
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1554-L1569
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/endpoint_url_response
---

# Signature

`pub(in crate::mapi) fn public_endpoint_url(headers: &HeaderMap, path: &str) -> String`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [endpoint_url_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/endpoint_url_response.md)