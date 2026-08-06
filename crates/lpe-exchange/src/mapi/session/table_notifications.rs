use super::*;
use crate::mapi::wire::MapiNotificationEventMask;

// [MS-OXCNOTIF] section 3.1.4.3 automatically subscribes a table after a
// view-creating table ROP. [MS-OXCFOLD] sections 2.2.1.13.1 and 2.2.1.14.1
// exclude tables opened with NoNotifications (0x10).

impl MapiSession {
    pub(in crate::mapi) fn remember_table_notification_eligibility(
        &mut self,
        handle: u32,
        logon_id: u8,
        notifications_enabled: bool,
    ) {
        self.table_notification_active_handles.remove(&handle);
        if notifications_enabled {
            self.table_notification_eligible_handles
                .insert(handle, logon_id);
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
        if self
            .table_notification_eligible_handles
            .contains_key(&handle)
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

    pub(in crate::mapi) fn pending_collaboration_hierarchy_notification_requires_contents(
        &self,
    ) -> bool {
        self.pending_notifications.iter().any(|event| {
            if !matches!(event.object_kind, Some("contact" | "calendar_event")) {
                return false;
            }
            let hierarchy_table_event = folder_counts_modified_event(event)
                .as_ref()
                .and_then(|folder_event| folder_counts_hierarchy_table_event(event, folder_event));
            hierarchy_table_event.is_some_and(|hierarchy_table_event| {
                self.handles.iter().any(|(handle, object)| {
                    self.table_notification_active_handles.contains(handle)
                        && table_matches_event(object, &hierarchy_table_event)
                })
            })
        })
    }

    pub(in crate::mapi) fn has_notification_targets(&self) -> bool {
        self.handles
            .values()
            .any(|object| matches!(object, MapiObject::NotificationSubscription { .. }))
            || self
                .table_notification_active_handles
                .iter()
                .any(|handle| self.handles.contains_key(handle))
    }

    pub(in crate::mapi) fn take_pending_notification_delivery_batch(
        &mut self,
    ) -> (
        Vec<(u32, u8, MapiNotificationEvent)>,
        VecDeque<MapiNotificationEvent>,
    ) {
        let events: Vec<_> = self.pending_notifications.drain(..).collect();
        let mut deliveries = Vec::new();
        let mut delivered_events = VecDeque::new();
        let mut delivered_table_changes = HashSet::new();
        for event in events {
            let table_event = table_changed_event(&event);
            let folder_event = folder_counts_modified_event(&event);
            let hierarchy_table_event = folder_event
                .as_ref()
                .and_then(|folder_event| folder_counts_hierarchy_table_event(&event, folder_event));
            let mut event_deliveries = Vec::new();
            for (handle, object) in &self.handles {
                match object {
                    MapiObject::NotificationSubscription { registration } => {
                        if event.is_complete_for_wire()
                            && registration_matches_event(registration, &event)
                        {
                            event_deliveries.push((
                                *handle,
                                registration.logon_id,
                                event.clone(),
                                false,
                            ));
                        }
                        if let Some(folder_event) = &folder_event {
                            if registration_matches_event(registration, folder_event) {
                                event_deliveries.push((
                                    *handle,
                                    registration.logon_id,
                                    folder_event.clone(),
                                    false,
                                ));
                            }
                        }
                        if event.event_mask != MapiNotificationEventMask::TableModified.as_u16()
                            && registration_matches_event(registration, &table_event)
                        {
                            event_deliveries.push((
                                *handle,
                                registration.logon_id,
                                table_event.clone(),
                                true,
                            ));
                        }
                    }
                    _ if self.table_notification_active_handles.contains(handle) => {
                        let Some(logon_id) = self
                            .table_notification_eligible_handles
                            .get(handle)
                            .copied()
                        else {
                            continue;
                        };
                        if table_matches_event(object, &event) {
                            event_deliveries.push((*handle, logon_id, table_event.clone(), true));
                        } else if let Some(hierarchy_table_event) = &hierarchy_table_event {
                            if table_matches_event(object, hierarchy_table_event) {
                                event_deliveries.push((
                                    *handle,
                                    logon_id,
                                    hierarchy_table_event.clone(),
                                    true,
                                ));
                            }
                        }
                    }
                    _ => {}
                }
            }
            // Exchange delivers an explicit subscription (for example NewMail)
            // before the automatic table invalidation for the same change.
            event_deliveries.sort_unstable_by_key(|(handle, _logon_id, delivery, table_change)| {
                (
                    *table_change,
                    *handle,
                    match delivery.kind {
                        MapiNotificationKind::Content => 0,
                        MapiNotificationKind::Hierarchy => 1,
                    },
                )
            });
            let delivery_count_before_event = deliveries.len();
            for (handle, logon_id, delivery, table_change) in event_deliveries {
                // Basic invalidations can coalesce by table. An informative
                // child-content hierarchy row must retain its changed folder
                // identity so two folders changed before one Execute are both
                // delivered.
                let changed_hierarchy_row_id = (delivery.kind == MapiNotificationKind::Hierarchy
                    && delivery.parent_folder_id == Some(delivery.folder_id))
                .then_some(delivery.message_id)
                .flatten();
                if table_change
                    && !delivered_table_changes.insert((
                        handle,
                        delivery.kind,
                        delivery.folder_id,
                        changed_hierarchy_row_id,
                    ))
                {
                    continue;
                }
                deliveries.push((handle, logon_id, delivery));
            }
            if deliveries.len() > delivery_count_before_event {
                delivered_events.push_back(event);
            }
        }
        (deliveries, delivered_events)
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
            .and_then(|folder_event| folder_counts_hierarchy_table_event(event, folder_event));
        self.handles.iter().any(|(handle, object)| match object {
            MapiObject::NotificationSubscription { registration } => {
                (event.is_complete_for_wire() && registration_matches_event(registration, event))
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
    // [MS-OXCNOTIF] section 3.1.4.3: a child content change updates the
    // corresponding row in an automatically subscribed parent hierarchy
    // table. A TableModified event carries no content counts, so their absence
    // cannot suppress that parent-table refresh.
    if event.kind != MapiNotificationKind::Content || event.parent_folder_id.is_none() {
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
    source_event: &MapiNotificationEvent,
    folder_event: &MapiNotificationEvent,
) -> Option<MapiNotificationEvent> {
    // [MS-OXCNOTIF] section 3.1.4.3: changing a folder's content counts also
    // changes that folder's row in the automatically subscribed parent table.
    let mut table_event = folder_event.clone();
    table_event.folder_id = folder_event.parent_folder_id?;
    table_event.event_mask = MapiNotificationEventMask::TableModified.as_u16();
    if source_event.event_mask & 0x0FFF == MapiNotificationEventMask::NewMail.as_u16() {
        // Exchange 2016 test1_202608031300.saz raw/753 sets the message and
        // search bits on the active hierarchy row it emits for NewMail.
        table_event.event_mask |= 0xC000;
    }
    Some(table_event)
}

fn table_matches_event(object: &MapiObject, event: &MapiNotificationEvent) -> bool {
    match (object, event.kind) {
        (MapiObject::ContentsTable { folder_id, .. }, MapiNotificationKind::Content) => {
            *folder_id == event.folder_id
        }
        (
            MapiObject::HierarchyTable {
                folder_id,
                depth,
                depth_folder_ids,
                ..
            },
            MapiNotificationKind::Hierarchy,
        ) => {
            *folder_id == event.folder_id
                // [MS-OXCFOLD] section 2.2.1.13.1: a Depth table includes
                // descendants, so their changed hierarchy rows are in view.
                || (*depth
                    && (depth_folder_ids.contains(&event.folder_id)
                        || event
                            .message_id
                            .is_some_and(|folder_id| depth_folder_ids.contains(&folder_id))))
        }
        _ => false,
    }
}
