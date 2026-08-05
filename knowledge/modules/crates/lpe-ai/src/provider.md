---
type: Rust Module
title: provider
resource: crates/lpe-ai/src/provider.rs#L1-L68
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-domain-documentannotation-documentchunk-documentprojection
  - external/serde-deserialize-serialize
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-ai
---

# Contains

- [ModelCapability](../../../../classes/crates/lpe-ai/src/provider/ModelCapability.md)
- [LocalModelDescriptor](../../../../classes/crates/lpe-ai/src/provider/LocalModelDescriptor.md)
- [InferenceRequest](../../../../classes/crates/lpe-ai/src/provider/InferenceRequest.md)
- [InferenceResponse](../../../../classes/crates/lpe-ai/src/provider/InferenceResponse.md)
- [LocalModelProvider](../../../../interfaces/crates/lpe-ai/src/provider/LocalModelProvider.md)
- [summarize_projection](../../../../functions/crates/lpe-ai/src/provider/summarize_projection.md)

# Imports

- `anyhow::Result`
- `lpe_domain::{DocumentAnnotation, DocumentChunk, DocumentProjection}`
- `serde::{Deserialize, Serialize}`
- `uuid::Uuid`

# Member of

- [lpe-ai](../../../../packages/crates/lpe-ai.md)