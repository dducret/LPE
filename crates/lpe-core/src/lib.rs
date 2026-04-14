use anyhow::Result;
use lpe_ai::{summarize_projection, LocalModelProvider};
use lpe_attachments::AttachmentFormat;
use lpe_domain::{
    AccessScope, Account, DocumentChunk, DocumentKind, DocumentProjection,
};
use uuid::Uuid;

#[derive(Debug, Default)]
pub struct CoreService;

impl CoreService {
    pub fn bootstrap_admin_account(&self) -> Result<Account> {
        Ok(Account::new("admin@example.test", "LPE Administrator"))
    }

    pub fn bootstrap_mail_projection(&self, owner_account_id: Uuid) -> DocumentProjection {
        DocumentProjection {
            id: Uuid::new_v4(),
            source_object_id: Uuid::new_v4(),
            kind: DocumentKind::MailMessage,
            title: "Welcome to LPE".to_string(),
            preview: "LPE prepares normalized documents for search and local AI.".to_string(),
            body_text: "LPE stores normalized message projections to support PostgreSQL full-text search and future local LLM workflows.".to_string(),
            language: Some("en".to_string()),
            participants: vec!["admin@example.test".to_string()],
            content_hash: "bootstrap-mail-projection".to_string(),
            scope: AccessScope {
                tenant_id: "default".to_string(),
                owner_account_id: lpe_domain::AccountId(owner_account_id),
                acl_fingerprint: "owner-only".to_string(),
            },
        }
    }

    pub fn bootstrap_projection_chunks(&self, document_id: Uuid) -> Vec<DocumentChunk> {
        vec![DocumentChunk {
            id: Uuid::new_v4(),
            document_id,
            ordinal: 0,
            chunk_text: "LPE prepares normalized message projections for search and future local AI.".to_string(),
            token_estimate: 16,
        }]
    }

    pub fn summarize_bootstrap_projection(
        &self,
        provider: &dyn LocalModelProvider,
        principal_account_id: Uuid,
    ) -> Result<String> {
        let projection = self.bootstrap_mail_projection(principal_account_id);
        let chunks = self.bootstrap_projection_chunks(projection.id);
        let annotation = summarize_projection(
            provider,
            principal_account_id,
            "stub-local",
            projection,
            chunks,
        )?;

        Ok(annotation.payload_json)
    }

    pub fn supported_attachment_formats(&self) -> Vec<AttachmentFormat> {
        vec![
            AttachmentFormat::Pdf,
            AttachmentFormat::Docx,
            AttachmentFormat::Odt,
        ]
    }
}
