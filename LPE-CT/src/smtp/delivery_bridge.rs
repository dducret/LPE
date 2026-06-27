use super::*;

const INBOUND_DELIVERY_PATH: &str = "/internal/lpe-ct/inbound-deliveries";

pub(in crate::smtp) async fn deliver_inbound_message(
    config: &RuntimeConfig,
    message: &QueuedMessage,
) -> Result<InboundDeliveryResponse> {
    if config.core_delivery_base_url.trim().is_empty() {
        anyhow::bail!(
            "core final delivery base URL is not configured; set LPE_CT_CORE_DELIVERY_BASE_URL"
        );
    }
    let endpoint = format!(
        "{}{}",
        config.core_delivery_base_url.trim_end_matches('/'),
        INBOUND_DELIVERY_PATH
    );
    let subject = parse_rfc822_header_value(&message.data, "subject").unwrap_or_default();
    let internet_message_id = parse_rfc822_header_value(&message.data, "message-id");
    let body_text = extract_visible_text(&message.data)?;
    let request = InboundDeliveryRequest {
        trace_id: message.id.clone(),
        peer: message.peer.clone(),
        helo: message.helo.clone(),
        mail_from: message.mail_from.clone(),
        rcpt_to: message.rcpt_to.clone(),
        subject,
        body_text,
        internet_message_id,
        raw_message: message.data.clone(),
    };

    let client = reqwest::Client::builder().build()?;
    let integration_secret = integration_shared_secret()?;
    let signed = SignedIntegrationHeaders::sign(
        &integration_secret,
        "POST",
        INBOUND_DELIVERY_PATH,
        &request,
    )
    .map_err(|error| anyhow!(error.to_string()))?;
    let response = client
        .post(endpoint)
        .header(INTEGRATION_KEY_HEADER, signed.integration_key)
        .header(INTEGRATION_TIMESTAMP_HEADER, signed.timestamp)
        .header(INTEGRATION_NONCE_HEADER, signed.nonce)
        .header(INTEGRATION_SIGNATURE_HEADER, signed.signature)
        .header("x-trace-id", request.trace_id.clone())
        .json(&request)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("core delivery endpoint returned {status}: {body}"));
    }

    let delivery: InboundDeliveryResponse = response.json().await?;
    if !delivery.accepted {
        observability::record_inbound_delivery("failed");
        return Err(anyhow!(
            "core delivery rejected inbound delivery: {}",
            delivery.detail.unwrap_or_else(|| "no detail".to_string())
        ));
    }
    observability::record_inbound_delivery("relayed");
    info!(
        trace_id = %request.trace_id,
        accepted = delivery.accepted,
        delivered_mailboxes = delivery.delivered_mailboxes.len(),
        internet_message_id = request.internet_message_id.as_deref().unwrap_or(""),
        "inbound message delivered to lpe core"
    );
    Ok(delivery)
}
