---
type: Rust Function
title: summarize_projection
resource: crates/lpe-ai/src/provider.rs#L45-L68
generated:
  by: okf-rs/0.3.0
---

# Signature

`pub fn summarize_projection( provider: &dyn LocalModelProvider, principal_account_id: Uuid, model_id: &str, projection: DocumentProjection, chunks: Vec<DocumentChunk>, ) -> Result<DocumentAnnotation>`