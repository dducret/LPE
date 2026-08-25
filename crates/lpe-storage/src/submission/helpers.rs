use anyhow::Result;
use sqlx::Postgres;
use uuid::Uuid;

use super::{ResolvedSubmissionAuthorization, SubmitMessageInput, SubmittedRecipientInput};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SubmissionSourceBehavior {
    ReplaceWithInput,
    UsePersisted,
}

pub(super) fn exact_editor_submission_input(
    mut input: SubmitMessageInput,
    mut persisted: SubmitMessageInput,
    authorization: &ResolvedSubmissionAuthorization,
) -> SubmitMessageInput {
    if !input.replace_attachments {
        persisted.attachments.append(&mut input.attachments);
        input.attachments = persisted.attachments;
    }
    input.from_address = authorization.from_address.clone();
    input.from_display = authorization.from_display.clone();
    input.sender_address = authorization.sender_address.clone();
    input.sender_display = authorization.sender_display.clone();
    input.replace_attachments = false;
    input
}

pub(super) async fn insert_visible_recipient(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    message_id: Uuid,
    role: &str,
    ordinal: usize,
    recipient: &SubmittedRecipientInput,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO message_recipients (
            id, tenant_id, message_id, role, address, display_name, ordinal
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id)
    .bind(message_id)
    .bind(role)
    .bind(&recipient.address)
    .bind(recipient.display_name.as_deref())
    .bind(ordinal as i32)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
