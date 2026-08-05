---
type: Rust Method
title: poll_shutdown
resource: LPE-CT/src/smtp/tls.rs#L67-L72
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn poll_shutdown( mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>, ) -> Poll<std::io::Result<()>>`