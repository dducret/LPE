use crate::{
    bad_request_error, ha_allows_active_work, ha_current_role, integration_shared_secret,
    internal_error, observability,
    types::ApiResult,
};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, SmtpSubmissionAuthRequest,
    SmtpSubmissionAuthResponse, SmtpSubmissionRequest, SmtpSubmissionResponse,
};
use lpe_magika::{
    collect_mime_attachment_parts, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_mail_auth::{authenticate_plain_credentials, AccountPrincipal};
use lpe_storage::{
    AuditEntryInput, Storage, SubmissionAccountIdentity, SubmitMessageInput,
    SubmittedRecipientInput,
};
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq)]
enum SmtpSubmissionError {
    Invalid(String),
    Forbidden(String),
    Temporary(String),
}

impl SmtpSubmissionError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden(message.into())
    }

    fn temporary(message: impl Into<String>) -> Self {
        Self::Temporary(message.into())
    }

    fn into_http_error(self) -> (StatusCode, String) {
        match self {
            Self::Invalid(message) => (StatusCode::BAD_REQUEST, message),
            Self::Forbidden(message) => (StatusCode::FORBIDDEN, message),
            Self::Temporary(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
        }
    }
}

pub(crate) async fn deliver_inbound_message(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<InboundDeliveryRequest>,
) -> ApiResult<InboundDeliveryResponse> {
    require_integration(&headers)?;
    if !ha_allows_active_work().map_err(internal_error)? {
        let role = ha_current_role()
            .map_err(internal_error)?
            .unwrap_or_else(|| "standby".to_string());
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("node role {role} does not accept LPE-CT inbound deliveries"),
        ));
    }
    let trace_id = request.trace_id.clone();
    let internet_message_id = request.internet_message_id.clone();
    let recipient_count = request.rcpt_to.len();
    let response = storage
        .deliver_inbound_message(request)
        .await
        .map_err(bad_request_error)?;
    observability::record_inbound_delivery(if response.accepted {
        "relayed"
    } else {
        "failed"
    });
    info!(
        trace_id = %trace_id,
        accepted = response.accepted,
        delivered_mailboxes = response.delivered_mailboxes.len(),
        recipient_count,
        internet_message_id = internet_message_id.as_deref().unwrap_or(""),
        "inbound delivery processed"
    );
    Ok(Json(response))
}

pub(crate) async fn authenticate_smtp_submission(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SmtpSubmissionAuthRequest>,
) -> ApiResult<SmtpSubmissionAuthResponse> {
    require_integration(&headers)?;
    let principal = authenticate_plain_credentials(
        &storage,
        None,
        &request.login,
        &request.password,
        "smtp",
    )
    .await
    .map_err(|error| (StatusCode::UNAUTHORIZED, error.to_string()))?;
    Ok(Json(SmtpSubmissionAuthResponse {
        accepted: true,
        account_id: Some(principal.account_id),
        account_email: Some(principal.email),
        account_display_name: Some(principal.display_name),
    }))
}

pub(crate) async fn accept_smtp_submission(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SmtpSubmissionRequest>,
) -> ApiResult<SmtpSubmissionResponse> {
    require_integration(&headers)?;
    if !ha_allows_active_work().map_err(internal_error)? {
        let role = ha_current_role()
            .map_err(internal_error)?
            .unwrap_or_else(|| "standby".to_string());
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("node role {role} does not accept LPE-CT smtp submissions"),
        ));
    }

    let principal = load_authenticated_submission_principal(&storage, &request)
        .await
        .map_err(SmtpSubmissionError::into_http_error)?;
    let submit_input = build_smtp_submission_input(&storage, &principal, &request)
        .await
        .map_err(SmtpSubmissionError::into_http_error)?;
    let submitted = storage
        .submit_message(
            submit_input,
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "smtp-submission".to_string(),
                subject: "client smtp submission".to_string(),
            },
        )
        .await
        .map_err(classify_submission_storage_error)?;
    observability::record_mail_submission("smtp");
    info!(
        trace_id = %request.trace_id,
        account_id = %principal.account_id,
        submitted = true,
        peer = %request.peer,
        helo = %request.helo,
        recipient_count = request.rcpt_to.len(),
        "smtp submission accepted from lpe-ct"
    );

    Ok(Json(SmtpSubmissionResponse {
        accepted: true,
        trace_id: request.trace_id,
        detail: Some(format!(
            "accepted message {} queued as {}",
            submitted.message_id, submitted.outbound_queue_id
        )),
    }))
}

async fn build_smtp_submission_input(
    storage: &Storage,
    principal: &AccountPrincipal,
    request: &SmtpSubmissionRequest,
) -> Result<SubmitMessageInput, SmtpSubmissionError> {
    let mut parsed = lpe_storage::mail::parse_rfc822_message(&request.raw_message)
        .map_err(|error| SmtpSubmissionError::invalid(error.to_string()))?;
    let from = parse_required_submission_from(&request.raw_message)?;
    parsed.from = Some(lpe_storage::mail::ParsedMailAddress {
        email: from.address.clone(),
        display_name: from.display_name.clone(),
    });
    validate_smtp_submission_attachments(&request.raw_message)
        .map_err(|error| SmtpSubmissionError::invalid(error.to_string()))?;
    let envelope_from = request
        .mail_from
        .trim()
        .trim_matches(['<', '>'])
        .to_lowercase();
    if envelope_from.is_empty() {
        return Err(SmtpSubmissionError::invalid(
            "smtp submission requires MAIL FROM",
        ));
    }
    if request.rcpt_to.is_empty() {
        return Err(SmtpSubmissionError::invalid(
            "smtp submission requires at least one RCPT TO recipient",
        ));
    }

    let owner = if from.address == principal.email {
        SubmissionAccountIdentity {
            account_id: principal.account_id,
            email: principal.email.clone(),
            display_name: principal.display_name.clone(),
        }
    } else {
        storage
            .find_submission_account_by_email_in_same_tenant(principal.account_id, &from.address)
            .await
            .map_err(|error| SmtpSubmissionError::temporary(error.to_string()))?
            .ok_or_else(|| {
                SmtpSubmissionError::forbidden(
                    "delegated From address is not a mailbox in the same tenant",
                )
            })?
    };
    if envelope_from != principal.email && envelope_from != owner.email {
        return Err(SmtpSubmissionError::forbidden(
            "smtp submission MAIL FROM must match the authenticated account or delegated mailbox",
        ));
    }

    let visible_to = parsed
        .to
        .iter()
        .cloned()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.email,
            display_name: recipient.display_name,
        })
        .collect::<Vec<_>>();
    let visible_cc = parsed
        .cc
        .iter()
        .cloned()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.email,
            display_name: recipient.display_name,
        })
        .collect::<Vec<_>>();
    let bcc = merge_smtp_bcc_recipients(
        &request.raw_message,
        &request.rcpt_to,
        &visible_to,
        &visible_cc,
    );
    let sender = parse_smtp_submission_sender(
        &request.raw_message,
        &from.address,
        &principal.email,
        &owner.email,
    )
    .map_err(|error| SmtpSubmissionError::forbidden(error.to_string()))?;

    Ok(build_smtp_submission_input_for_owner(
        principal, &owner, request, parsed, visible_to, visible_cc, bcc, sender,
    ))
}

async fn load_authenticated_submission_principal(
    storage: &Storage,
    request: &SmtpSubmissionRequest,
) -> Result<AccountPrincipal, SmtpSubmissionError> {
    let identity = storage
        .fetch_account_identity(request.account_id)
        .await
        .map_err(|error| SmtpSubmissionError::forbidden(error.to_string()))?;
    let requested_email = request.account_email.trim().to_lowercase();
    if !requested_email.is_empty() && requested_email != identity.email {
        return Err(SmtpSubmissionError::forbidden(
            "smtp submission principal does not match authenticated account",
        ));
    }

    Ok(AccountPrincipal {
        tenant_id: String::new(),
        account_id: identity.account_id,
        email: identity.email,
        display_name: identity.display_name,
    })
}

fn parse_required_submission_from(
    raw_message: &[u8],
) -> Result<SubmittedRecipientInput, SmtpSubmissionError> {
    let from = lpe_storage::mail::parse_header_recipients(raw_message, "from");
    match from.as_slice() {
        [] => Err(SmtpSubmissionError::invalid(
            "smtp submission requires exactly one From mailbox",
        )),
        [address] => Ok(address.clone()),
        _ => Err(SmtpSubmissionError::invalid(
            "smtp submission requires exactly one From mailbox",
        )),
    }
}

pub(crate) fn build_smtp_submission_input_for_owner(
    principal: &AccountPrincipal,
    owner: &SubmissionAccountIdentity,
    request: &SmtpSubmissionRequest,
    parsed: lpe_storage::mail::ParsedRfc822Message,
    to: Vec<SubmittedRecipientInput>,
    cc: Vec<SubmittedRecipientInput>,
    bcc: Vec<SubmittedRecipientInput>,
    sender: Option<SubmittedRecipientInput>,
) -> SubmitMessageInput {
    let from_display = parsed
        .from
        .as_ref()
        .and_then(|address| address.display_name.clone())
        .or_else(|| Some(owner.display_name.clone()));

    SubmitMessageInput {
        draft_message_id: None,
        account_id: owner.account_id,
        submitted_by_account_id: principal.account_id,
        source: "smtp-submission".to_string(),
        from_display,
        from_address: owner.email.clone(),
        sender_display: sender
            .as_ref()
            .and_then(|address| address.display_name.clone())
            .or_else(|| sender.as_ref().map(|_| principal.display_name.clone())),
        sender_address: sender.map(|address| address.address),
        to,
        cc,
        bcc,
        subject: parsed.subject,
        body_text: parsed.body_text,
        body_html_sanitized: parsed.body_html_sanitized,
        internet_message_id: parsed.message_id,
        mime_blob_ref: Some(format!("smtp-submission-mime:{}", request.trace_id)),
        size_octets: request.raw_message.len() as i64,
        unread: Some(false),
        flagged: Some(false),
        attachments: parsed.attachments,
    }
}

pub(crate) fn parse_smtp_submission_sender(
    raw_message: &[u8],
    from_address: &str,
    principal_email: &str,
    owner_email: &str,
) -> anyhow::Result<Option<SubmittedRecipientInput>> {
    let sender_addresses = lpe_storage::mail::parse_header_recipients(raw_message, "sender");
    if sender_addresses.len() > 1 {
        anyhow::bail!("smtp submission supports at most one Sender mailbox");
    }
    let sender = sender_addresses.into_iter().next();
    let Some(sender) = sender else {
        return Ok(None);
    };
    let normalized_sender = sender.address.trim().to_lowercase();
    if normalized_sender.is_empty()
        || normalized_sender == from_address
        || normalized_sender == owner_email
    {
        return Ok(None);
    }
    if normalized_sender != principal_email {
        anyhow::bail!("authenticated account cannot submit a different Sender address");
    }
    Ok(Some(SubmittedRecipientInput {
        address: normalized_sender,
        display_name: sender.display_name,
    }))
}

pub(crate) fn merge_smtp_bcc_recipients(
    raw_message: &[u8],
    envelope_recipients: &[String],
    to: &[SubmittedRecipientInput],
    cc: &[SubmittedRecipientInput],
) -> Vec<SubmittedRecipientInput> {
    let mut visible = to
        .iter()
        .chain(cc.iter())
        .map(|recipient| recipient.address.trim().to_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    let mut merged = Vec::new();
    let mut seen = std::collections::BTreeSet::new();

    for recipient in lpe_storage::mail::parse_header_recipients(raw_message, "bcc") {
        let normalized = recipient.address.trim().to_lowercase();
        if !normalized.is_empty() && seen.insert(normalized.clone()) {
            visible.insert(normalized);
            merged.push(recipient);
        }
    }

    for recipient in envelope_recipients {
        let normalized = recipient.trim().trim_matches(['<', '>']).to_lowercase();
        if !normalized.is_empty()
            && !visible.contains(&normalized)
            && seen.insert(normalized.clone())
        {
            merged.push(SubmittedRecipientInput {
                address: normalized,
                display_name: None,
            });
        }
    }

    merged
}

fn validate_smtp_submission_attachments(raw_message: &[u8]) -> anyhow::Result<()> {
    let validator = Validator::from_env();
    for attachment in collect_mime_attachment_parts(raw_message)? {
        let outcome = validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::SmtpClientSubmission,
                declared_mime: attachment.declared_mime.clone(),
                filename: attachment.filename.clone(),
                expected_kind: ExpectedKind::Any,
            },
            &attachment.bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            anyhow::bail!(
                "smtp submission blocked by Magika validation for {:?}: {}",
                attachment.filename,
                outcome.reason
            );
        }
    }
    Ok(())
}

fn classify_submission_storage_error(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("send as is not granted")
        || lowered.contains("send on behalf is not granted")
        || lowered.contains("from email must match delegated mailbox")
        || lowered.contains("sender email must match authenticated account")
        || lowered.contains("account not found")
    {
        return (StatusCode::FORBIDDEN, message);
    }

    if lowered.contains("from_address is required")
        || lowered.contains("at least one recipient")
        || lowered.contains("subject")
        || lowered.contains("mail from")
    {
        return (StatusCode::BAD_REQUEST, message);
    }

    internal_error(message)
}

fn require_integration(headers: &HeaderMap) -> std::result::Result<(), (StatusCode, String)> {
    let provided = headers
        .get("x-lpe-integration-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            observability::record_security_event("integration_auth_failure");
            (
                StatusCode::UNAUTHORIZED,
                "missing integration key".to_string(),
            )
        })?;
    let expected = integration_shared_secret().map_err(internal_error)?;
    if provided == expected {
        Ok(())
    } else {
        observability::record_security_event("integration_auth_failure");
        Err((
            StatusCode::UNAUTHORIZED,
            "invalid integration key".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_required_submission_from, parse_smtp_submission_sender, SmtpSubmissionError,
    };

    #[test]
    fn smtp_submission_requires_exactly_one_from_mailbox() {
        let missing = parse_required_submission_from(b"Subject: Hi\r\n\r\nBody\r\n").unwrap_err();
        assert_eq!(
            missing,
            SmtpSubmissionError::Invalid(
                "smtp submission requires exactly one From mailbox".to_string()
            )
        );

        let multiple = parse_required_submission_from(
            concat!(
                "From: Alice <alice@example.test>, Shared <shared@example.test>\r\n",
                "Subject: Hi\r\n",
                "\r\n",
                "Body\r\n"
            )
            .as_bytes(),
        )
        .unwrap_err();
        assert_eq!(
            multiple,
            SmtpSubmissionError::Invalid(
                "smtp submission requires exactly one From mailbox".to_string()
            )
        );
    }

    #[test]
    fn smtp_submission_sender_rejects_multiple_sender_mailboxes() {
        let error = parse_smtp_submission_sender(
            concat!(
                "From: Shared <shared@example.test>\r\n",
                "Sender: Delegate <delegate@example.test>, Other <other@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Hi\r\n",
                "\r\n",
                "Body\r\n"
            )
            .as_bytes(),
            "shared@example.test",
            "delegate@example.test",
            "shared@example.test",
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("smtp submission supports at most one Sender mailbox")
        );
    }

    #[test]
    fn smtp_submission_sender_rejects_unrelated_sender_identity() {
        let error = parse_smtp_submission_sender(
            concat!(
                "From: Shared <shared@example.test>\r\n",
                "Sender: Other <other@example.test>\r\n",
                "To: Bob <bob@example.test>\r\n",
                "Subject: Hi\r\n",
                "\r\n",
                "Body\r\n"
            )
            .as_bytes(),
            "shared@example.test",
            "delegate@example.test",
            "shared@example.test",
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("authenticated account cannot submit a different Sender address")
        );
    }
}
