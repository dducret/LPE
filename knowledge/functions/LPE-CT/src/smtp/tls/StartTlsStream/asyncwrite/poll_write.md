---
type: Rust Method
title: poll_write
resource: LPE-CT/src/smtp/tls.rs#L55-L61
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn poll_write( mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>, data: &[u8], ) -> Poll<std::io::Result<usize>>`