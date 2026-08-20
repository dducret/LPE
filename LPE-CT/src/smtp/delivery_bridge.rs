use super::*;

const INBOUND_DELIVERY_PATH: &str = "/internal/lpe-ct/inbound-deliveries";
const CORE_TRACE_HEADER: &[u8] = b"x-lpe-ct-trace-id";

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
    // [MS-OXCSPAM] section 2.2.1.3 and [MS-OXPHISH] section 2.2.1.1 define
    // Exchange-local message stamps. LPE deliberately projects neither: the
    // sole core mailbox fact from LPE-CT is its bridge-owned trace identifier.
    let raw_message = strip_spoofed_core_trace_header(&message.data);
    let subject = parse_rfc822_header_value(&raw_message, "subject").unwrap_or_default();
    let internet_message_id = parse_rfc822_header_value(&raw_message, "message-id");
    let body_text = extract_visible_text(&raw_message)?;
    let request = InboundDeliveryRequest {
        trace_id: message.id.clone(),
        peer: message.peer.clone(),
        helo: message.helo.clone(),
        mail_from: message.mail_from.clone(),
        rcpt_to: message.rcpt_to.clone(),
        subject,
        body_text,
        internet_message_id,
        raw_message,
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

fn strip_spoofed_core_trace_header(raw: &[u8]) -> Vec<u8> {
    let Some(header_end) = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .or_else(|| raw.windows(2).position(|window| window == b"\n\n"))
    else {
        return raw.to_vec();
    };

    let mut sanitized = Vec::with_capacity(raw.len());
    let mut skipping = false;
    for line in raw[..header_end].split_inclusive(|byte| *byte == b'\n') {
        let line_without_eol = line.strip_suffix(b"\n").unwrap_or(line);
        let line_without_eol = line_without_eol
            .strip_suffix(b"\r")
            .unwrap_or(line_without_eol);
        let continuation = matches!(line_without_eol.first(), Some(b' ' | b'\t'));
        if !continuation {
            skipping = line_without_eol
                .iter()
                .position(|byte| *byte == b':')
                .is_some_and(|colon| {
                    line_without_eol[..colon]
                        .trim_ascii()
                        .eq_ignore_ascii_case(CORE_TRACE_HEADER)
                });
        }
        if !skipping {
            sanitized.extend_from_slice(line);
        }
    }
    sanitized.extend_from_slice(&raw[header_end..]);
    sanitized
}

#[cfg(test)]
mod tests {
    use super::strip_spoofed_core_trace_header;

    #[test]
    fn bridge_removes_spoofed_trace_headers_but_preserves_message_bytes() {
        // [MS-OXCSPAM] section 2.2.1.3; [MS-OXPHISH] section 2.2.1.1:
        // the signed bridge, not Internet message headers, is the source of
        // LPE's bounded trace projection.
        let raw = b"From: sender@example.test\r\nX-LPE-CT-Trace-Id: stale-user-value\r\n\tcontinued\r\nX-Unrelated: retained\r\n\r\nbody\x00bytes";

        let sanitized = strip_spoofed_core_trace_header(raw);

        assert_eq!(
            sanitized,
            b"From: sender@example.test\r\nX-Unrelated: retained\r\n\r\nbody\x00bytes"
        );
    }

    #[test]
    fn bridge_leaves_malformed_message_bytes_untouched() {
        let raw = b"X-LPE-CT-Trace-Id: stale-user-value\r\nno-header-separator";

        assert_eq!(strip_spoofed_core_trace_header(raw), raw);
    }
}
