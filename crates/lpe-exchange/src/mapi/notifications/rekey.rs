use super::{MapiNotificationEvent, MapiNotificationKind};
use crate::mapi::wire::MapiNotificationEventMask;

impl MapiNotificationEvent {
    /// A Calendar ICS upload can preserve the canonical Event while replacing
    /// its client-visible MAPI identity. The retired and replacement MIDs are
    /// distinct message objects for notification purposes.
    pub(crate) fn calendar_identity_rekey_events(
        &self,
        old_message_id: u64,
        new_message_id: u64,
    ) -> Option<[Self; 2]> {
        if self.kind != MapiNotificationKind::Content
            || self.object_kind != Some("calendar_event")
            || self.message_id != Some(new_message_id)
            || old_message_id == new_message_id
        {
            return None;
        }

        let mut deleted = self.clone();
        deleted.parent_folder_id = None;
        deleted.message_id = Some(old_message_id);
        deleted.old_folder_id = None;
        deleted.old_parent_folder_id = None;
        deleted.old_message_id = None;
        deleted.event_mask = MapiNotificationEventMask::ObjectDeleted.as_u16();
        deleted.change_kind = Some("deleted".to_string());

        let mut created = self.clone();
        created.old_folder_id = None;
        created.old_parent_folder_id = None;
        created.old_message_id = None;
        created.event_mask = MapiNotificationEventMask::ObjectCreated.as_u16();
        created.change_kind = Some("created".to_string());

        Some([deleted, created])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_rekey_reports_retirement_before_replacement() {
        let folder_id = 0x0000_0000_0009_0001;
        let old_message_id = 0x0000_0000_0111_0001;
        let new_message_id = 0x0000_0000_0112_0001;
        let event = MapiNotificationEvent::content(folder_id, Some(new_message_id))
            .with_parent_folder_id(Some(0x0000_0000_0001_0001))
            .with_object_kind("calendar_event");

        let [deleted, created] = event
            .calendar_identity_rekey_events(old_message_id, new_message_id)
            .unwrap();

        assert_eq!(deleted.message_id, Some(old_message_id));
        assert_eq!(
            deleted.event_mask,
            MapiNotificationEventMask::ObjectDeleted.as_u16()
        );
        assert_eq!(deleted.parent_folder_id, None);
        assert_eq!(created.message_id, Some(new_message_id));
        assert_eq!(
            created.event_mask,
            MapiNotificationEventMask::ObjectCreated.as_u16()
        );
        assert_eq!(created.parent_folder_id, Some(0x0000_0000_0001_0001));
    }

    #[test]
    fn unchanged_calendar_identity_is_not_a_rekey() {
        let message_id = 0x0000_0000_0111_0001;
        let event = MapiNotificationEvent::content(0x0000_0000_0009_0001, Some(message_id))
            .with_object_kind("calendar_event");

        assert!(event
            .calendar_identity_rekey_events(message_id, message_id)
            .is_none());
    }
}
