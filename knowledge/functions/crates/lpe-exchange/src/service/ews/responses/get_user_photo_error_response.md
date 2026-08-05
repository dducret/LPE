---
type: Rust Function
title: get_user_photo_error_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L303-L315
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_user_photo
---

# Signature

`pub(in crate::service) fn get_user_photo_error_response(code: &str, message: &str) -> String`

# Called by

- [get_user_photo](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_user_photo.md)