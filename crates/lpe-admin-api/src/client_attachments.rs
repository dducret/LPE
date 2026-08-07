use anyhow::bail;
use axum::{
    body::Body,
    extract::{Multipart, Path as AxumPath, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::Response,
    Json,
};
use lpe_magika::{
    Detector, ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator,
};
use lpe_storage::{
    ActiveSyncAttachment, AttachmentUploadInput, AuditEntryInput, AuthenticatedAccount,
    MailboxAccountAccess, Storage,
};
use serde::{Deserialize, Serialize};
use std::path::Path as FilePath;
use uuid::Uuid;

use crate::{http::internal_error, require_account};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageAttachmentQuery {
    account_id: Uuid,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClientAttachmentUploadResponse {
    id: Uuid,
    message_id: Uuid,
    name: String,
    media_type: String,
    size_octets: u64,
}

pub(crate) async fn upload_draft_attachment(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(message_id): AxumPath<Uuid>,
    Query(query): Query<MessageAttachmentQuery>,
    mut multipart: Multipart,
) -> Result<Json<ClientAttachmentUploadResponse>, (StatusCode, String)> {
    let account = require_account(&storage, &headers).await?;
    let mailbox_access =
        require_attachment_mailbox_access(&storage, &account, query.account_id, true).await?;
    if !storage
        .message_is_visible_draft(mailbox_access.account_id, message_id)
        .await
        .map_err(internal_error)?
    {
        return Err((StatusCode::NOT_FOUND, "draft message not found".to_string()));
    }

    let attachment = read_multipart_attachment(&mut multipart).await?;
    let attachment = validate_client_attachment_with_validator(&Validator::from_env(), attachment)
        .map_err(crate::bad_request_error)?;
    let attachment_for_audit = attachment.file_name.clone();
    let Some((_email, stored_attachment)) = storage
        .add_message_attachment(
            mailbox_access.account_id,
            message_id,
            attachment,
            AuditEntryInput {
                actor: account.email,
                action: "client-upload-draft-attachment".to_string(),
                subject: attachment_for_audit,
            },
        )
        .await
        .map_err(internal_error)?
    else {
        return Err((StatusCode::NOT_FOUND, "draft message not found".to_string()));
    };

    Ok(Json(client_attachment_upload_response(stored_attachment)))
}

pub(crate) async fn download_message_attachment(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((message_id, attachment_id)): AxumPath<(Uuid, Uuid)>,
    Query(query): Query<MessageAttachmentQuery>,
) -> Result<Response, (StatusCode, String)> {
    let account = require_account(&storage, &headers).await?;
    let mailbox_access =
        require_attachment_mailbox_access(&storage, &account, query.account_id, false).await?;
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let Some(content) = storage
        .fetch_activesync_attachment_content(mailbox_access.account_id, &file_reference)
        .await
        .map_err(internal_error)?
    else {
        return Err((StatusCode::NOT_FOUND, "attachment not found".to_string()));
    };

    Ok(attachment_content_response(
        &content.file_name,
        &content.media_type,
        content.blob_bytes,
    ))
}

async fn require_attachment_mailbox_access(
    storage: &Storage,
    account: &AuthenticatedAccount,
    target_account_id: Uuid,
    write_required: bool,
) -> Result<MailboxAccountAccess, (StatusCode, String)> {
    let accessible = storage
        .fetch_accessible_mailbox_accounts(account.account_id)
        .await
        .map_err(internal_error)?;
    authorize_attachment_mailbox_access(accessible, target_account_id, write_required)
}

fn authorize_attachment_mailbox_access(
    accessible: Vec<MailboxAccountAccess>,
    target_account_id: Uuid,
    write_required: bool,
) -> Result<MailboxAccountAccess, (StatusCode, String)> {
    let mailbox_access = accessible
        .into_iter()
        .find(|entry| entry.account_id == target_account_id)
        .ok_or((
            StatusCode::FORBIDDEN,
            "authenticated account cannot access this mailbox".to_string(),
        ))?;
    if !mailbox_access.is_owned && !mailbox_access.may_read {
        return Err((
            StatusCode::FORBIDDEN,
            "authenticated account cannot read this mailbox".to_string(),
        ));
    }
    if write_required && !mailbox_access.is_owned && !mailbox_access.may_write {
        return Err((
            StatusCode::FORBIDDEN,
            "authenticated account cannot write drafts in this mailbox".to_string(),
        ));
    }
    Ok(mailbox_access)
}

async fn read_multipart_attachment(
    multipart: &mut Multipart,
) -> Result<AttachmentUploadInput, (StatusCode, String)> {
    let mut attachment = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(crate::bad_request_error)?
    {
        if field.name() != Some("file") {
            continue;
        }
        if attachment.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "exactly one attachment file is required".to_string(),
            ));
        }
        let file_name = normalized_attachment_file_name(field.file_name()).ok_or((
            StatusCode::BAD_REQUEST,
            "attachment file name is required".to_string(),
        ))?;
        let media_type = field
            .content_type()
            .map(ToString::to_string)
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let blob_bytes = field
            .bytes()
            .await
            .map_err(crate::bad_request_error)?
            .to_vec();
        attachment = Some(AttachmentUploadInput {
            file_name,
            media_type,
            disposition: Some("attachment".to_string()),
            content_id: None,
            blob_bytes,
        });
    }
    attachment.ok_or((
        StatusCode::BAD_REQUEST,
        "exactly one attachment file is required".to_string(),
    ))
}

fn validate_client_attachment_with_validator<D: Detector>(
    validator: &Validator<D>,
    attachment: AttachmentUploadInput,
) -> anyhow::Result<AttachmentUploadInput> {
    let outcome = validator.validate_bytes(
        ValidationRequest {
            ingress_context: IngressContext::JmapUpload,
            declared_mime: Some(attachment.media_type.clone()),
            filename: Some(attachment.file_name.clone()),
            expected_kind: ExpectedKind::Any,
        },
        &attachment.blob_bytes,
    )?;
    if outcome.policy_decision != PolicyDecision::Accept {
        bail!(
            "attachment upload blocked by Magika validation: {}",
            outcome.reason
        );
    }
    Ok(attachment)
}

fn normalized_attachment_file_name(file_name: Option<&str>) -> Option<String> {
    let file_name = file_name?;
    let file_name = FilePath::new(file_name).file_name()?.to_str()?.trim();
    (!file_name.is_empty() && !file_name.contains(['\r', '\n'])).then(|| file_name.to_string())
}

fn client_attachment_upload_response(
    attachment: ActiveSyncAttachment,
) -> ClientAttachmentUploadResponse {
    ClientAttachmentUploadResponse {
        id: attachment.id,
        message_id: attachment.message_id,
        name: attachment.file_name,
        media_type: attachment.media_type,
        size_octets: attachment.size_octets,
    }
}

fn attachment_content_response(file_name: &str, media_type: &str, blob_bytes: Vec<u8>) -> Response {
    let safe_file_name: String = normalized_attachment_file_name(Some(file_name))
        .unwrap_or_else(|| "attachment".to_string())
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    let disposition = format!("inline; filename=\"{safe_file_name}\"");
    let content_type = HeaderValue::from_str(media_type)
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, disposition)
        .body(Body::from(blob_bytes))
        .expect("validated attachment response headers are valid")
}

#[cfg(test)]
mod tests {
    use super::{
        attachment_content_response, authorize_attachment_mailbox_access,
        normalized_attachment_file_name, validate_client_attachment_with_validator,
    };
    use axum::http::StatusCode;
    use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
    use lpe_storage::{AttachmentUploadInput, MailboxAccountAccess};
    use uuid::Uuid;

    #[derive(Clone)]
    struct FakeDetector {
        detection: MagikaDetection,
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
            Ok(self.detection.clone())
        }
    }

    fn attachment() -> AttachmentUploadInput {
        AttachmentUploadInput {
            file_name: "report.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            disposition: Some("attachment".to_string()),
            content_id: None,
            blob_bytes: b"%PDF-1.7".to_vec(),
        }
    }

    #[test]
    fn client_attachment_upload_requires_magika_acceptance() {
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "pdf".to_string(),
                    mime_type: "application/pdf".to_string(),
                    description: "PDF".to_string(),
                    group: "document".to_string(),
                    extensions: vec!["pdf".to_string()],
                    score: Some(0.99),
                },
            },
            0.9,
        );

        assert!(validate_client_attachment_with_validator(&validator, attachment()).is_ok());
    }

    #[test]
    fn client_attachment_upload_rejects_magika_mismatch() {
        let validator = Validator::new(
            FakeDetector {
                detection: MagikaDetection {
                    label: "png".to_string(),
                    mime_type: "image/png".to_string(),
                    description: "PNG".to_string(),
                    group: "image".to_string(),
                    extensions: vec!["png".to_string()],
                    score: Some(0.99),
                },
            },
            0.9,
        );

        assert!(validate_client_attachment_with_validator(&validator, attachment()).is_err());
    }

    #[test]
    fn attachment_download_uses_safe_inline_headers() {
        let response = attachment_content_response("report.pdf", "application/pdf", Vec::new());
        assert_eq!(response.headers()["content-type"], "application/pdf");
        assert_eq!(
            response.headers()["content-disposition"],
            "inline; filename=\"report.pdf\""
        );
        assert_eq!(
            normalized_attachment_file_name(Some("../report.pdf")),
            Some("report.pdf".to_string())
        );
        assert_eq!(
            normalized_attachment_file_name(Some("unsafe\r\n.pdf")),
            None
        );
        assert_eq!(
            attachment_content_response("résumé.pdf", "application/pdf", Vec::new()).headers()
                ["content-disposition"],
            "inline; filename=\"r_sum_.pdf\""
        );
    }

    #[test]
    fn delegated_attachment_upload_requires_canonical_write_access() {
        let delegated_account_id = Uuid::new_v4();
        let read_only = MailboxAccountAccess {
            tenant_id: Uuid::new_v4(),
            account_id: delegated_account_id,
            email: "owner@example.test".to_string(),
            display_name: "Owner".to_string(),
            is_owned: false,
            may_read: true,
            may_write: false,
            may_send_as: false,
            may_send_on_behalf: false,
        };

        let error =
            authorize_attachment_mailbox_access(vec![read_only], delegated_account_id, true)
                .expect_err("a read-only delegate cannot add a draft attachment");
        assert_eq!(error.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn delegated_attachment_download_allows_canonical_read_access() {
        let delegated_account_id = Uuid::new_v4();
        let read_only = MailboxAccountAccess {
            tenant_id: Uuid::new_v4(),
            account_id: delegated_account_id,
            email: "owner@example.test".to_string(),
            display_name: "Owner".to_string(),
            is_owned: false,
            may_read: true,
            may_write: false,
            may_send_as: false,
            may_send_on_behalf: false,
        };

        assert_eq!(
            authorize_attachment_mailbox_access(vec![read_only], delegated_account_id, false)
                .expect("a read delegate can retrieve an accessible attachment")
                .account_id,
            delegated_account_id
        );
    }

    #[test]
    fn attachment_access_rejects_a_mailbox_absent_from_canonical_grants() {
        let error = authorize_attachment_mailbox_access(Vec::new(), Uuid::new_v4(), false)
            .expect_err("an ungranted mailbox cannot expose attachments");
        assert_eq!(error.0, StatusCode::FORBIDDEN);
    }
}
