use anyhow::Result;
use lpe_domain::{DocumentAnnotation, DocumentChunk, DocumentProjection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ModelCapability {
    Summarize,
    Classify,
    Extract,
    Embed,
    Chat,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalModelDescriptor {
    pub id: String,
    pub family: String,
    pub capabilities: Vec<ModelCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InferenceRequest {
    pub request_id: Uuid,
    pub principal_account_id: Uuid,
    pub model_id: String,
    pub instructions: String,
    pub projection: DocumentProjection,
    pub chunks: Vec<DocumentChunk>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InferenceResponse {
    pub request_id: Uuid,
    pub model_id: String,
    pub output_text: String,
    pub provenance_chunk_ids: Vec<Uuid>,
}

pub trait LocalModelProvider: Send + Sync {
    fn describe_models(&self) -> Vec<LocalModelDescriptor>;
    fn infer(&self, request: InferenceRequest) -> Result<InferenceResponse>;
}

pub fn summarize_projection(
    provider: &dyn LocalModelProvider,
    principal_account_id: Uuid,
    model_id: &str,
    projection: DocumentProjection,
    chunks: Vec<DocumentChunk>,
) -> Result<DocumentAnnotation> {
    let response = provider.infer(InferenceRequest {
        request_id: Uuid::new_v4(),
        principal_account_id,
        model_id: model_id.to_string(),
        instructions: "Summarize the document while preserving provenance.".to_string(),
        projection: projection.clone(),
        chunks,
    })?;

    Ok(DocumentAnnotation {
        id: Uuid::new_v4(),
        document_id: projection.id,
        annotation_type: "summary".to_string(),
        payload_json: format!(r#"{{"summary":"{}"}}"#, response.output_text),
        model_name: Some(response.model_id),
    })
}
