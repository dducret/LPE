---
type: Rust Function
title: mapi_headers_with_content_type
resource: crates/lpe-exchange/src/tests/mod.rs#L12286-L12307
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/insert_mapi_content_length
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_accepts_outlook_octet_stream_bind_probe
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_accepts_rca_octet_stream_emsmdb_connect
---

# Signature

`fn mapi_headers_with_content_type(request_type: &str, content_type: &'static str) -> HeaderMap`

# Calls

- [insert_mapi_content_length](../../../../../functions/crates/lpe-exchange/src/tests/insert_mapi_content_length.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [mapi_over_http_accepts_outlook_octet_stream_bind_probe](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_accepts_outlook_octet_stream_bind_probe.md)
- [mapi_over_http_accepts_rca_octet_stream_emsmdb_connect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_accepts_rca_octet_stream_emsmdb_connect.md)