---
type: Rust Method
title: poll_flush
resource: LPE-CT/src/smtp/tls.rs#L63-L65
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>>`