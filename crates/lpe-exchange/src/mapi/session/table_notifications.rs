use super::*;
use crate::mapi::wire::MapiNotificationEventMask;

// [MS-OXCNOTIF] section 3.1.4.3 automatically subscribes a table after a
// view-creating table ROP. [MS-OXCFOLD] sections 2.2.1.13.1 and 2.2.1.14.1
// exclude tables opened with NoNotifications (0x10).

impl MapiSession {
    pub(in crate::mapi) fn remember_table_notification_eligibility(
        &mut self,
        handle: u32,
        notifications_enabled: bool,
    ) {
        self.table_notification_active_handles.remove(&handle);
        if notifications_enabled {
            self.table_notification_eligible_handles.insert(handle);
        } else {
            self.table_notification_eligible_handles.remove(&handle);
        }
    }

    pub(in crate::mapi) fn activate_table_notifications_for_request(
        &mut self,
        handle_slots: &[u32],
        request: &RopRequest,
    ) {
        let Some(rop_id) = RopId::from_u8(request.rop_id) else {
            return;
        };
        if !matches!(
            rop_id,
            RopId::CollapseRow
                | RopId::ExpandRow
                | RopId::FindRow
                | RopId::QueryColumnsAll
                | RopId::QueryPosition
                | RopId::QueryRows
                | RopId::SeekRow
                | RopId::SeekRowBookmark
                | RopId::SeekRowFractional
        ) {
            return;
        }
        let Some(handle) = input_handle(handle_slots, request) else {
            return;
        };
        if self.table_notification_eligible_handles.contains(&handle)
            && matches!(
                self.handles.get(&handle),
                Some(MapiObject::HierarchyTable { .. } | MapiObject::ContentsTable { .. })
            )
        {
            self.table_notification_active_handles.insert(handle);
        }
    }

    pub(in crate::mapi) fn deactivate_table_notifications(&mut self, handle: Option<u32>) {
        if let Some(handle) = handle {
            self.table_notification_active_handles.remove(&handle);
        }
    }

    pub(in crate::mapi) fn forget_table_notification_handle(&mut self, handle: u32) {
        self.table_notification_eligible_handles.remove(&handle);
        self.table_notification_active_handles.remove(&handle);
    }

    pub(in crate::mapi) fn record_notification(&mut self, event: MapiNotificationEvent) {
        if self.has_notification_target(&event) {
            self.pending_notifications.push_back(event);
        }
    }

    pub(in crate::mapi) fn pending_notification_count(&self) -> usize {
        self.pending_notifications.len()
    }

    pub(in crate::mapi) fn take_pending_notification_deliveries(
        &mut self,
    ) -> Vec<(u32, MapiNotificationEvent)> {
        let events: Vec<_> = self.pending_notifications.drain(..).collect();
        let mut deliveries = Vec::new();
        let mut delivered_table_changes = HashSet::new();
        for event in events {
            let table_event = table_changed_event(&event);
            let folder_event = folder_counts_modified_event(&event);
            let hierarchy_table_event = folder_event
                .as_ref()
                .and_then(folder_counts_hierarchy_table_event);
            let mut event_deliveries = Vec::new();
            for (handle, object) in &self.handles {
                match object {
                    MapiObject::NotificationSubscription { registration } => {
                        if registration_matches_event(registration, &event) {
                            event_deliveries.push((*handle, event.clone(), false));
                        }
                        if let Some(folder_event) = &folder_event {
                            if registration_matches_event(registration, folder_event) {
                                event_deliveries.push((*handle, folder_event.clone(), false));
                            }
                        }
                        if event.event_mask != MapiNotificationEventMask::TableModified.as_u16()
                            && registration_matches_event(registration, &table_event)
                        {
                            event_deliveries.push((*handle, table_event.clone(), true));
                        }
                    }
                    _ if self.table_notification_active_handles.contains(handle) => {
                        if table_matches_event(object, &event) {
                            event_deliveries.push((*handle, table_event.clone(), true));
                        } else if let Some(hierarchy_table_event) = &hierarchy_table_event {
                            if table_matches_event(object, hierarchy_table_event) {
                                event_deliveries.push((
                                    *handle,
                                    hierarchy_table_event.clone(),
                                    true,
                                ));
                            }
                        }
                    }
                    _ => {}
                }
            }
            event_deliveries.sort_unstable_by_key(|(handle, delivery, table_change)| {
                (
                    *handle,
                    *table_change,
                    match delivery.kind {
                        MapiNotificationKind::Content => 0,
                        MapiNotificationKind::Hierarchy => 1,
                    },
                )
            });
            for (handle, delivery, table_change) in event_deliveries {
                if table_change
                    && !delivered_table_changes.insert((handle, delivery.kind, delivery.folder_id))
                {
                    continue;
                }
                deliveries.push((handle, delivery));
            }
        }
        deliveries
    }

    pub(in crate::mapi) fn matching_notifications(
        &self,
        events: Vec<MapiNotificationEvent>,
    ) -> Vec<MapiNotificationEvent> {
        events
            .into_iter()
            .filter(|event| self.has_notification_target(event))
            .collect()
    }

    fn has_notification_target(&self, event: &MapiNotificationEvent) -> bool {
        let table_event = table_changed_event(event);
        let folder_event = folder_counts_modified_event(event);
        let hierarchy_table_event = folder_event
            .as_ref()
            .and_then(folder_counts_hierarchy_table_event);
        self.handles.iter().any(|(handle, object)| match object {
            MapiObject::NotificationSubscription { registration } => {
                registration_matches_event(registration, event)
                    || registration_matches_event(registration, &table_event)
                    || folder_event
                        .as_ref()
                        .map(|folder_event| registration_matches_event(registration, folder_event))
                        .unwrap_or(false)
            }
            _ => {
                self.table_notification_active_handles.contains(handle)
                    && (table_matches_event(object, event)
                        || hierarchy_table_event
                            .as_ref()
                            .map(|hierarchy_event| table_matches_event(object, hierarchy_event))
                            .unwrap_or(false))
            }
        })
    }
}

fn table_changed_event(event: &MapiNotificationEvent) -> MapiNotificationEvent {
    let mut table_event = event.clone();
    table_event.event_mask = MapiNotificationEventMask::TableModified.as_u16();
    table_event
}

fn folder_counts_modified_event(event: &MapiNotificationEvent) -> Option<MapiNotificationEvent> {
    if event.kind != MapiNotificationKind::Content
        || (event.total_messages.is_none() && event.unread_messages.is_none())
    {
        return None;
    }
    let mut folder_event = event.clone();
    folder_event.kind = MapiNotificationKind::Hierarchy;
    folder_event.event_mask = MapiNotificationEventMask::ObjectModified.as_u16();
    folder_event.message_id = Some(event.folder_id);
    folder_event.old_folder_id = None;
    folder_event.canonical_message_id = None;
    folder_event.object_kind = Some("mailbox");
    folder_event.message_subject = None;
    Some(folder_event)
}

fn folder_counts_hierarchy_table_event(
    folder_event: &MapiNotificationEvent,
) -> Option<MapiNotificationEvent> {
    // [MS-OXCNOTIF] section 3.1.4.3: changing a folder's content counts also
    // changes that folder's row in the automatically subscribed parent table.
    let mut table_event = folder_event.clone();
    table_event.folder_id = folder_event.parent_folder_id?;
    table_event.event_mask = MapiNotificationEventMask::TableModified.as_u16();
    Some(table_event)
}

fn table_matches_event(object: &MapiObject, event: &MapiNotificationEvent) -> bool {
    match (object, event.kind) {
        (MapiObject::ContentsTable { folder_id, .. }, MapiNotificationKind::Content)
        | (MapiObject::HierarchyTable { folder_id, .. }, MapiNotificationKind::Hierarchy) => {
            *folder_id == event.folder_id
        }
        _ => false,
    }
}
