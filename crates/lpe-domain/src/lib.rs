pub mod account;
pub mod bridge_auth;
pub mod document;
mod encoding;
pub mod submission;
pub mod transport;

pub use crate::account::{Account, AccountId};
pub use crate::bridge_auth::{
    current_unix_timestamp, BridgeAuthError, SignedIntegrationHeaders, DEFAULT_MAX_SKEW_SECONDS,
    INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER,
    INTEGRATION_TIMESTAMP_HEADER,
};
pub use crate::document::{
    AccessScope, DocumentAnnotation, DocumentChunk, DocumentKind, DocumentProjection,
};
pub use crate::submission::{
    InboundDeliveryRequest, InboundDeliveryResponse, RecipientVerificationRequest,
    RecipientVerificationResponse, SmtpSubmissionAuthRequest, SmtpSubmissionAuthResponse,
    SmtpSubmissionRequest, SmtpSubmissionResponse,
};
pub use crate::transport::{
    OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, TransportDeliveryStatus,
    TransportDsnReport, TransportRecipient, TransportRetryAdvice, TransportRouteDecision,
    TransportTechnicalStatus, TransportThrottleStatus,
};

#[cfg(test)]
mod tests {
    use super::{
        OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, SmtpSubmissionRequest,
        TransportDeliveryStatus, TransportDsnReport, TransportRecipient, TransportRetryAdvice,
        TransportRouteDecision, TransportTechnicalStatus, TransportThrottleStatus,
    };
    use uuid::Uuid;

    #[test]
    fn transport_delivery_status_serializes_as_lowercase() {
        let value = serde_json::to_string(&TransportDeliveryStatus::Deferred).unwrap();
        assert_eq!(value, "\"deferred\"");
    }

    #[test]
    fn outbound_envelope_recipients_include_bcc() {
        let request = OutboundMessageHandoffRequest {
            queue_id: Uuid::nil(),
            message_id: Uuid::nil(),
            account_id: Uuid::nil(),
            from_address: "sender@example.test".to_string(),
            from_display: None,
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            to: vec![TransportRecipient {
                address: "to@example.test".to_string(),
                display_name: None,
            }],
            cc: vec![TransportRecipient {
                address: "cc@example.test".to_string(),
                display_name: None,
            }],
            bcc: vec![TransportRecipient {
                address: "bcc@example.test".to_string(),
                display_name: None,
            }],
            subject: "subject".to_string(),
            body_text: "body".to_string(),
            body_html_sanitized: None,
            internet_message_id: None,
            attempt_count: 0,
            last_attempt_error: None,
        };

        assert_eq!(
            request.envelope_recipients(),
            vec![
                "to@example.test".to_string(),
                "cc@example.test".to_string(),
                "bcc@example.test".to_string()
            ]
        );
    }

    #[test]
    fn outbound_handoff_response_carries_structured_transport_details() {
        let response = OutboundMessageHandoffResponse {
            queue_id: Uuid::nil(),
            status: TransportDeliveryStatus::Deferred,
            trace_id: "trace-1".to_string(),
            detail: Some("rate limit reached".to_string()),
            remote_message_ref: Some("remote-42".to_string()),
            retry: Some(TransportRetryAdvice {
                retry_after_seconds: 120,
                policy: "throttle".to_string(),
                reason: Some("tenant quota".to_string()),
            }),
            dsn: Some(TransportDsnReport {
                action: "delayed".to_string(),
                status: "4.7.1".to_string(),
                diagnostic_code: Some("smtp; 451 4.7.1 throttled".to_string()),
                remote_mta: Some("mx1.example.test".to_string()),
            }),
            technical: Some(TransportTechnicalStatus {
                phase: "rcpt-to".to_string(),
                smtp_code: Some(451),
                enhanced_code: Some("4.7.1".to_string()),
                remote_host: Some("mx1.example.test".to_string()),
                detail: Some("recipient domain throttled".to_string()),
            }),
            route: Some(TransportRouteDecision {
                rule_id: Some("domain-example".to_string()),
                relay_target: Some("smtp://mx1.example.test:25".to_string()),
                queue: "deferred".to_string(),
            }),
            throttle: Some(TransportThrottleStatus {
                scope: "recipient-domain".to_string(),
                key: "example.test".to_string(),
                limit: 20,
                window_seconds: 60,
                retry_after_seconds: 120,
            }),
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["status"], "deferred");
        assert_eq!(json["retry"]["retry_after_seconds"], 120);
        assert_eq!(json["dsn"]["status"], "4.7.1");
        assert_eq!(json["route"]["queue"], "deferred");
        assert_eq!(json["throttle"]["scope"], "recipient-domain");
    }

    #[test]
    fn smtp_submission_request_serializes_raw_message_as_base64() {
        let request = SmtpSubmissionRequest {
            trace_id: "trace-1".to_string(),
            helo: "client.example.test".to_string(),
            peer: "203.0.113.10:53544".to_string(),
            account_id: Uuid::nil(),
            account_email: "alice@example.test".to_string(),
            account_display_name: "Alice".to_string(),
            mail_from: "alice@example.test".to_string(),
            rcpt_to: vec!["bob@example.test".to_string()],
            raw_message: b"Subject: hi\r\n\r\nbody".to_vec(),
        };

        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["raw_message"], "U3ViamVjdDogaGkNCg0KYm9keQ==");
    }
}
