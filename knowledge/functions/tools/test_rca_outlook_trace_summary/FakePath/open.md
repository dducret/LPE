---
type: Python Method
title: open
resource: tools/test_rca_outlook_trace_summary.py#L1731-L1732
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/imaps_proxy/load_certificates
  - functions/LPE-CT/src/imaps_proxy/load_private_key
  - functions/LPE-CT/src/smtp/audit/append_transport_audit
  - functions/LPE-CT/src/smtp/audit/append_postfix_style_mail_log
  - functions/LPE-CT/src/smtp/tls/load_certificates
  - functions/LPE-CT/src/smtp/tls/load_private_key
  - functions/LPE-CT/src/submission/load_certificates
  - functions/LPE-CT/src/submission/load_private_key
  - functions/crates/lpe-attachments/src/extraction/extract_pdf_text
  - functions/crates/lpe-core/src/outlook_trace/open_trace_file_with_mode
  - functions/crates/lpe-core/src/outlook_trace/next_trace_sequence
  - functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst
  - functions/tools/check_oversized_sources/count_lines
  - functions/tools/rca_outlook/http/request
  - functions/tools/rca_outlook_trace_summary/summarize_rr
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/web/client/src/useClientWorkspace/useClientWorkspace
---

# Signature

`def open(self, *args, **kwargs):`

# Called by

- [load_certificates](../../../../functions/LPE-CT/src/imaps_proxy/load_certificates.md)
- [load_private_key](../../../../functions/LPE-CT/src/imaps_proxy/load_private_key.md)
- [append_transport_audit](../../../../functions/LPE-CT/src/smtp/audit/append_transport_audit.md)
- [append_postfix_style_mail_log](../../../../functions/LPE-CT/src/smtp/audit/append_postfix_style_mail_log.md)
- [load_certificates](../../../../functions/LPE-CT/src/smtp/tls/load_certificates.md)
- [load_private_key](../../../../functions/LPE-CT/src/smtp/tls/load_private_key.md)
- [load_certificates](../../../../functions/LPE-CT/src/submission/load_certificates.md)
- [load_private_key](../../../../functions/LPE-CT/src/submission/load_private_key.md)
- [extract_pdf_text](../../../../functions/crates/lpe-attachments/src/extraction/extract_pdf_text.md)
- [open_trace_file_with_mode](../../../../functions/crates/lpe-core/src/outlook_trace/open_trace_file_with_mode.md)
- [next_trace_sequence](../../../../functions/crates/lpe-core/src/outlook_trace/next_trace_sequence.md)
- [import_mailbox_from_pst](../../../../functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst.md)
- [count_lines](../../../../functions/tools/check_oversized_sources/count_lines.md)
- [request](../../../../functions/tools/rca_outlook/http/request.md)
- [summarize_rr](../../../../functions/tools/rca_outlook_trace_summary/summarize_rr.md)
- [summarize_log](../../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [useClientWorkspace](../../../../functions/web/client/src/useClientWorkspace/useClientWorkspace.md)