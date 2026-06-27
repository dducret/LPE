use anyhow::Result;
use uuid::Uuid;

use crate::{AuditEntryInput, JmapEmail, JmapEmailFollowupUpdate, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MessageFlagUpdate {
    pub unread: Option<bool>,
    pub flagged: Option<bool>,
}

impl MessageFlagUpdate {
    pub fn into_followup_update(self) -> JmapEmailFollowupUpdate {
        JmapEmailFollowupUpdate {
            unread: self.unread,
            flagged: self.flagged,
            followup_flag_status: self.flagged.map(|flagged| {
                if flagged {
                    "flagged".to_string()
                } else {
                    "none".to_string()
                }
            }),
            ..Default::default()
        }
    }
}

pub async fn update_message_flags(
    storage: &Storage,
    account_id: Uuid,
    message_id: Uuid,
    update: MessageFlagUpdate,
    audit: AuditEntryInput,
) -> Result<JmapEmail> {
    storage
        .update_jmap_email_followup_flags(
            account_id,
            message_id,
            update.into_followup_update(),
            audit,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::MessageFlagUpdate;

    #[test]
    fn message_flag_update_projects_followup_flag_status() {
        let flagged = MessageFlagUpdate {
            unread: Some(false),
            flagged: Some(true),
        }
        .into_followup_update();
        assert_eq!(flagged.unread, Some(false));
        assert_eq!(flagged.flagged, Some(true));
        assert_eq!(flagged.followup_flag_status.as_deref(), Some("flagged"));

        let unflagged = MessageFlagUpdate {
            unread: None,
            flagged: Some(false),
        }
        .into_followup_update();
        assert_eq!(unflagged.followup_flag_status.as_deref(), Some("none"));

        let read_only = MessageFlagUpdate {
            unread: Some(true),
            flagged: None,
        }
        .into_followup_update();
        assert_eq!(read_only.unread, Some(true));
        assert_eq!(read_only.followup_flag_status, None);
    }
}
