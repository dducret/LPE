use anyhow::{bail, Result};
use lpe_magika::ExpectedKind;
use lpe_storage::JmapEmail;
use uuid::Uuid;

use crate::parse::parse_uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JmapBlobId {
    Upload(Uuid),
    Opaque(String),
}

pub(crate) fn parse_upload_blob_id(value: &str) -> Result<Uuid> {
    match JmapBlobId::parse(value)? {
        JmapBlobId::Upload(id) => Ok(id),
        JmapBlobId::Opaque(_) => bail!("blob not found"),
    }
}

pub(crate) fn expected_attachment_kind(media_type: &str, file_name: &str) -> ExpectedKind {
    let media_type = media_type.trim().to_ascii_lowercase();
    let file_name = file_name.trim().to_ascii_lowercase();
    if matches!(
        media_type.as_str(),
        "application/pdf"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.oasis.opendocument.text"
    ) || file_name.ends_with(".pdf")
        || file_name.ends_with(".docx")
        || file_name.ends_with(".odt")
    {
        ExpectedKind::SupportedAttachmentText
    } else {
        ExpectedKind::Any
    }
}

pub(crate) fn blob_id_for_message(email: &JmapEmail) -> String {
    JmapBlobId::for_message(email).into_response_id()
}

impl JmapBlobId {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            bail!("blobId is required");
        }
        if let Ok(id) = Uuid::parse_str(trimmed) {
            return Ok(Self::Upload(id));
        }
        if let Some(upload_id) = trimmed.strip_prefix("upload:") {
            return Ok(Self::Upload(parse_uuid(upload_id)?));
        }
        Ok(Self::Opaque(trimmed.to_string()))
    }

    pub(crate) fn for_message(email: &JmapEmail) -> Self {
        match email.mime_blob_ref.as_deref() {
            Some(value) if !value.trim().is_empty() => {
                Self::parse(value).unwrap_or_else(|_| Self::Opaque(value.trim().to_string()))
            }
            _ => Self::Opaque(format!("message:{}", email.id)),
        }
    }

    pub(crate) fn into_response_id(self) -> String {
        match self {
            Self::Upload(id) => format!("upload:{id}"),
            Self::Opaque(value) => value,
        }
    }
}
