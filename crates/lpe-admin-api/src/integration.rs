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
use uuid::Uuid;

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
    observability::record_inbound_delivery(response.status.as_str());
    info!(
        trace_id = %trace_id,
        status = response.status.as_str(),
        accepted_recipients = response.accepted_recipients.len(),
        rejected_recipients = response.rejected_recipients.len(),
        stored_messages = response.stored_message_ids.len(),
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
        &request.username,
        &request.password,
        "smtp",
    )
    .await
    .map_err(|error| (StatusCode::UNAUTHORIZED, error.to_string()))?;
    Ok(Json(SmtpSubmissionAuthResponse {
        tenant_id: principal.tenant_id,
        account_id: principal.account_id,
        email: principal.email,
        display_name: principal.display_name,
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

    let principal = AccountPrincipal {
        tenant_id: String::new(),
        account_id: request.account_id,
        email: request.account_email.trim().to_lowercase(),
        display_name: request.account_display_name.clone(),
    };
    let submit_input = build_smtp_submission_input(&storage, &principal, &request)
        .await
        .map_err(bad_request_error)?;
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
        .map_err(bad_request_error)?;
    observability::record_mail_submission("smtp");
    info!(
        trace_id = %request.trace_id,
        account_id = %principal.account_id,
        message_id = %submitted.message_id,
        outbound_queue_id = %submitted.outbound_queue_id,
        peer = %request.peer,
        helo = %request.helo,
        recipient_count = request.rcpt_to.len(),
        "smtp submission accepted from lpe-ct"
    );

    Ok(Json(SmtpSubmissionResponse {
        trace_id: request.trace_id,
        message_id: submitted.message_id,
        outbound_queue_id: submitted.outbound_queue_id,
        delivery_status: submitted.delivery_status,
    }))
}

async fn build_smtp_submission_input(
    storage: &Storage,
    principal: &AccountPrincipal,
    request: &SmtpSubmissionRequest,
) -> anyhow::Result<SubmitMessageInput> {
    let parsed = lpe_storage::mail::parse_rfc822_message(&request.raw_message)?;
    validate_smtp_submission_attachments(&request.raw_message)?;
    let envelope_from = request
        .mail_from
        .trim()
        .trim_matches(['<', '>'])
        .to_lowercase();
    if envelope_from.is_empty() {
        anyhow::bail!("smtp submission requires MAIL FROM");
    }
    if request.rcpt_to.is_empty() {
        anyhow::bail!("smtp submission requires at least one RCPT TO recipient");
    }

    let from = parsed
        .from
        .as_ref()
        .map(|address| address.email.trim().to_lowercase())
        .unwrap_or_else(|| principal.email.clone());
    let owner = if from == principal.email {
        SubmissionAccountIdentity {
            account_id: principal.account_id,
            email: principal.email.clone(),
            display_name: principal.display_name.clone(),
        }
    } else {
        storage
            .find_submission_account_by_email_in_same_tenant(principal.account_id, &from)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("delegated From address is not a mailbox in the same tenant")
            })?
    };
    if envelope_from != principal.email && envelope_from != owner.email {
        anyhow::bail!(
            "smtp submission MAIL FROM must match the authenticated account or delegated mailbox"
        );
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
    let sender =
        parse_smtp_submission_sender(&request.raw_message, &from, &principal.email, &owner.email)?;

    Ok(build_smtp_submission_input_for_owner(
        principal, &owner, request, parsed, visible_to, visible_cc, bcc, sender,
    ))
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
        mime_blob_ref: Some(format!("smtp-submission-mime:{}", Uuid::new_v4())),
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
    let sender = lpe_storage::mail::parse_header_recipients(raw_message, "sender")
        .into_iter()
        .next();
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
