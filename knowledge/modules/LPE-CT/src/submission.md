---
type: Rust Module
title: submission
resource: LPE-CT/src/submission.rs#L1-L996
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-context-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-domain-signedintegrationheaders-smtpsubmissionauthrequest-smtpsubmissionauthresponse-smtpsubmissionrequest-smtpsubmissionresponse-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/lpe-magika-ingresscontext-validator
  - external/reqwest-statuscode
  - external/std-env-fs-file-io-bufreader-net-socketaddr-sync-arc-mutex
  - external/tokio-io-asyncbufread-asyncbufreadext-asyncwrite-asyncwriteext-bufreader-as-tokiobufreader-net-tcplistener-tcpstream
  - external/tokio-rustls-rustls-pki-types-certificateder-pki-types-privatekeyder-serverconfig-tlsacceptor
  - external/tracing-info-warn
  - external/uuid-uuid
  - external/crate-integration-shared-secret-observability-outlook-test-message-smtp-max-smtp-message-size-bytes-parse-smtp-path-smtp-path-error-reply-smtppathkind-transport-policy
  - external/super-authenticate-smtp-client-classify-auth-failure-status-classify-submission-failure-status-decode-auth-login-token-decode-auth-plain-sanitize-smtp-text-smtp-auth-failure-reply-smtp-submission-failure-reply-submit-message-smtpauthfailurekind-submissionfailurekind
  - external/crate-env-test-lock
  - external/axum-extract-state-http-headermap-routing-post-json-router
  - external/lpe-domain-smtpsubmissionrequest-smtpsubmissionresponse
  - external/std-sync-arc-mutex
  - external/tokio-net-tcplistener
  member_of:
  - packages/LPE-CT
---

# Contains

- [SubmissionPrincipal](../../../classes/LPE-CT/src/submission/SubmissionPrincipal.md)
- [SmtpAuthFailureKind](../../../classes/LPE-CT/src/submission/SmtpAuthFailureKind.md)
- [SubmissionFailureKind](../../../classes/LPE-CT/src/submission/SubmissionFailureKind.md)
- [SubmissionTransaction](../../../classes/LPE-CT/src/submission/SubmissionTransaction.md)
- [reset_message](../../../functions/LPE-CT/src/submission/SubmissionTransaction/reset_message.md)
- [run_submission_listener](../../../functions/LPE-CT/src/submission/run_submission_listener.md)
- [handle_submission_session](../../../functions/LPE-CT/src/submission/handle_submission_session.md)
- [authenticate_smtp_client](../../../functions/LPE-CT/src/submission/authenticate_smtp_client.md)
- [submit_message](../../../functions/LPE-CT/src/submission/submit_message.md)
- [parse_auth_plain](../../../functions/LPE-CT/src/submission/parse_auth_plain.md)
- [parse_auth_login](../../../functions/LPE-CT/src/submission/parse_auth_login.md)
- [decode_auth_plain](../../../functions/LPE-CT/src/submission/decode_auth_plain.md)
- [decode_auth_login_token](../../../functions/LPE-CT/src/submission/decode_auth_login_token.md)
- [read_client_line](../../../functions/LPE-CT/src/submission/read_client_line.md)
- [read_data](../../../functions/LPE-CT/src/submission/read_data.md)
- [write_line](../../../functions/LPE-CT/src/submission/write_line.md)
- [max_message_size_bytes](../../../functions/LPE-CT/src/submission/max_message_size_bytes.md)
- [load_tls_acceptor](../../../functions/LPE-CT/src/submission/load_tls_acceptor.md)
- [load_certificates](../../../functions/LPE-CT/src/submission/load_certificates.md)
- [load_private_key](../../../functions/LPE-CT/src/submission/load_private_key.md)
- [required_env](../../../functions/LPE-CT/src/submission/required_env.md)
- [sanitize_smtp_text](../../../functions/LPE-CT/src/submission/sanitize_smtp_text.md)
- [internal_submission_error](../../../functions/LPE-CT/src/submission/internal_submission_error.md)
- [classify_auth_failure_status](../../../functions/LPE-CT/src/submission/classify_auth_failure_status.md)
- [smtp_auth_failure_reply](../../../functions/LPE-CT/src/submission/smtp_auth_failure_reply.md)
- [smtp_submission_failure_reply](../../../functions/LPE-CT/src/submission/smtp_submission_failure_reply.md)
- [classify_submission_failure_status](../../../functions/LPE-CT/src/submission/classify_submission_failure_status.md)
- [Capture](../../../classes/LPE-CT/src/submission/Capture.md)
- [auth_plain_decodes_username_and_password](../../../functions/LPE-CT/src/submission/auth_plain_decodes_username_and_password.md)
- [auth_login_token_decodes_base64_value](../../../functions/LPE-CT/src/submission/auth_login_token_decodes_base64_value.md)
- [smtp_error_text_is_sanitized_for_wire_replies](../../../functions/LPE-CT/src/submission/smtp_error_text_is_sanitized_for_wire_replies.md)
- [temporary_submission_failures_map_to_451](../../../functions/LPE-CT/src/submission/temporary_submission_failures_map_to_451.md)
- [permanent_submission_failures_map_to_550_for_authorization_errors](../../../functions/LPE-CT/src/submission/permanent_submission_failures_map_to_550_for_authorization_errors.md)
- [malformed_submission_failures_map_to_554](../../../functions/LPE-CT/src/submission/malformed_submission_failures_map_to_554.md)
- [auth_failures_distinguish_temporary_and_invalid_credentials](../../../functions/LPE-CT/src/submission/auth_failures_distinguish_temporary_and_invalid_credentials.md)
- [smtp_xoauth_is_rejected_before_core_auth_request](../../../functions/LPE-CT/src/submission/smtp_xoauth_is_rejected_before_core_auth_request.md)
- [submit_message_posts_trace_header_and_returns_success](../../../functions/LPE-CT/src/submission/submit_message_posts_trace_header_and_returns_success.md)
- [accept](../../../functions/LPE-CT/src/submission/accept.md)
- [submit_message_rejects_non_accepted_success_body_before_smtp_final_reply](../../../functions/LPE-CT/src/submission/submit_message_rejects_non_accepted_success_body_before_smtp_final_reply.md)
- [reject](../../../functions/LPE-CT/src/submission/reject.md)

# Imports

- `anyhow::{anyhow, bail, Context, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_domain::{
    SignedIntegrationHeaders, SmtpSubmissionAuthRequest, SmtpSubmissionAuthResponse,
    SmtpSubmissionRequest, SmtpSubmissionResponse, INTEGRATION_KEY_HEADER,
    INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
}`
- `lpe_magika::{IngressContext, Validator}`
- `reqwest::StatusCode`
- `std::{
    env,
    fs::File,
    io::BufReader,
    net::SocketAddr,
    sync::{Arc, Mutex},
}`
- `tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader as TokioBufReader},
    net::{TcpListener, TcpStream},
}`
- `tokio_rustls::{
    rustls::{pki_types::CertificateDer, pki_types::PrivateKeyDer, ServerConfig},
    TlsAcceptor,
}`
- `tracing::{info, warn}`
- `uuid::Uuid`
- `crate::{
    integration_shared_secret, observability, outlook_test_message,
    smtp::{max_smtp_message_size_bytes, parse_smtp_path, smtp_path_error_reply, SmtpPathKind},
    transport_policy,
}`
- `super::{
        authenticate_smtp_client, classify_auth_failure_status, classify_submission_failure_status,
        decode_auth_login_token, decode_auth_plain, sanitize_smtp_text, smtp_auth_failure_reply,
        smtp_submission_failure_reply, submit_message, SmtpAuthFailureKind, SubmissionFailureKind,
    }`
- `crate::env_test_lock`
- `axum::{extract::State, http::HeaderMap, routing::post, Json, Router}`
- `lpe_domain::{SmtpSubmissionRequest, SmtpSubmissionResponse}`
- `std::sync::{Arc, Mutex}`
- `tokio::net::TcpListener`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)