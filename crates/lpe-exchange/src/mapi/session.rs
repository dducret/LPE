use super::dispatch::*;
use super::notifications::*;
use super::outlook_startup::*;
use super::properties::*;
use super::rop::*;
use super::store_adapter::*;
use super::sync::*;
use super::transport::*;
use super::*;
use crate::mapi::wire::RopId;
use crate::mapi_store::MapiAssociatedConfigMessage;
use lpe_storage::{
    AttachmentUploadInput, JmapEmail, MapiContactImportedIdentity, MapiEventAttachmentChanges,
    MapiEventImportedIdentity, SearchFolderDefinition,
};

const MAX_POST_HIERARCHY_ROP_IDS: usize = 64;
const MAX_POST_HIERARCHY_REQUEST_CONTRACTS: usize = 8;
const MAX_OUTLOOK_VIEW_FAILURE_TRACE_EVENTS: usize = 32;
const MAX_OUTLOOK_STREAM_BATCH_EVENTS: usize = 8;
const RELEASED_HANDLE_RESPONSE_SENTINEL: u32 = 0;

fn session_debug_context_or_none(context: &str) -> &str {
    if context.is_empty() {
        "none"
    } else {
        context
    }
}

mod lifecycle;
mod named_properties;
mod table_notifications;
mod types;
#[cfg(test)]
pub(crate) use lifecycle::begin_active_session_request_for_test;
pub(in crate::mapi) use lifecycle::*;
pub(crate) use lifecycle::{create_rpc_emsmdb_context, execute_rpc_emsmdb_rops};
pub(in crate::mapi) use named_properties::*;
pub(in crate::mapi) use types::*;

impl MapiSession {
    pub(in crate::mapi) fn record_logon_identity(&mut self, identity: MapiLogonIdentityDebug) {
        self.logon_identity = Some(identity);
    }

    pub(in crate::mapi) fn record_transport_request(
        &mut self,
        request_type: &str,
        request_id: &str,
    ) {
        if self.request_count == 0 {
            self.first_request_type = request_type.to_string();
            self.first_request_id = request_id.to_string();
        }
        self.last_request_type = request_type.to_string();
        self.last_request_id = request_id.to_string();
        self.request_count = self.request_count.saturating_add(1);
        if request_type == "Execute" {
            self.execute_request_count = self.execute_request_count.saturating_add(1);
        }
    }

    pub(in crate::mapi) fn record_completed_sync_checkpoint(
        &mut self,
        folder_id: u64,
        folder_role: &str,
        folder_container_class: &str,
        checkpoint_kind: &str,
        sync_type: u8,
        status: &str,
    ) {
        if self
            .post_hierarchy_actions
            .completed_sync_checkpoint_summaries
            .len()
            >= 64
        {
            return;
        }
        let summary = format!(
            "folder=0x{folder_id:016x};role={folder_role};container={folder_container_class};kind={checkpoint_kind};sync=0x{sync_type:02x};status={status}"
        );
        if !self
            .post_hierarchy_actions
            .completed_sync_checkpoint_summaries
            .contains(&summary)
        {
            if !self
                .post_hierarchy_actions
                .completed_sync_checkpoint_folder_ids
                .contains(&folder_id)
            {
                self.post_hierarchy_actions
                    .completed_sync_checkpoint_folder_ids
                    .push(folder_id);
            }
            self.post_hierarchy_actions
                .completed_sync_checkpoint_summaries
                .push(summary);
        }
    }

    pub(in crate::mapi) fn record_opened_folder(&mut self, folder_id: u64) {
        if self.post_hierarchy_actions.opened_folder_ids.len() >= 64
            || self
                .post_hierarchy_actions
                .opened_folder_ids
                .contains(&folder_id)
        {
            return;
        }
        self.post_hierarchy_actions
            .opened_folder_ids
            .push(folder_id);
    }

    pub(in crate::mapi) fn record_message_handle_generation(
        &mut self,
        handle: u32,
        folder_id: u64,
        message_id: u64,
    ) {
        let generation = self.message_save_generation(folder_id, message_id);
        self.message_handle_generations.insert(handle, generation);
    }

    pub(in crate::mapi) fn message_save_generation(&self, folder_id: u64, message_id: u64) -> u64 {
        self.message_save_generations
            .get(&(folder_id, message_id))
            .copied()
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn message_handle_generation(&self, handle: u32) -> Option<u64> {
        self.message_handle_generations.get(&handle).copied()
    }

    pub(in crate::mapi) fn record_message_saved(
        &mut self,
        handle: u32,
        folder_id: u64,
        message_id: u64,
    ) {
        let generation = self
            .message_save_generation(folder_id, message_id)
            .saturating_add(1);
        self.message_save_generations
            .insert((folder_id, message_id), generation);
        self.message_handle_generations.insert(handle, generation);
    }

    pub(in crate::mapi) fn record_inbox_open_folder_probe(&mut self) {
        self.post_hierarchy_actions.inbox_open_folder_probe_count = self
            .post_hierarchy_actions
            .inbox_open_folder_probe_count
            .saturating_add(1);
    }

    pub(in crate::mapi) fn record_inbox_folder_type_getprops_probe(&mut self) {
        self.post_hierarchy_actions
            .inbox_folder_type_getprops_probe_count = self
            .post_hierarchy_actions
            .inbox_folder_type_getprops_probe_count
            .saturating_add(1);
    }

    pub(in crate::mapi) fn record_inbox_normal_contents_table(&mut self) {
        self.post_hierarchy_actions
            .inbox_normal_contents_table_observed = true;
    }

    pub(in crate::mapi) fn record_inbox_normal_contents_table_setcolumns(
        &mut self,
        handle: Option<u32>,
        context: String,
    ) {
        self.post_hierarchy_actions
            .inbox_normal_contents_table_setcolumns_observed = true;
        self.post_hierarchy_actions
            .last_inbox_normal_contents_table_setcolumns_handle = handle;
        self.post_hierarchy_actions
            .last_inbox_normal_contents_table_setcolumns_context = context;
    }

    pub(in crate::mapi) fn record_inbox_normal_contents_table_query_rows(
        &mut self,
        handle: Option<u32>,
        context: String,
    ) {
        self.post_hierarchy_actions
            .inbox_normal_contents_table_query_rows_observed = true;
        self.post_hierarchy_actions
            .last_inbox_normal_contents_table_query_rows_handle = handle;
        self.post_hierarchy_actions
            .last_inbox_normal_contents_table_query_rows_context = context;
    }

    pub(in crate::mapi) fn record_inbox_normal_contents_table_find_row(
        &mut self,
        handle: Option<u32>,
        context: String,
    ) {
        self.post_hierarchy_actions
            .inbox_normal_contents_table_find_row_observed = true;
        self.post_hierarchy_actions
            .last_inbox_normal_contents_table_find_row_handle = handle;
        self.post_hierarchy_actions
            .last_inbox_normal_contents_table_find_row_context = context.clone();
        self.post_hierarchy_actions
            .last_visible_inbox_message_row_context = context;
    }

    pub(in crate::mapi) fn record_inbox_normal_contents_table_query_position(
        &mut self,
        handle: Option<u32>,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_inbox_normal_contents_table_query_position_handle = handle;
        self.post_hierarchy_actions
            .last_inbox_normal_contents_table_query_position_context = context;
    }

    pub(in crate::mapi) fn record_calendar_normal_contents_table_query_position(
        &mut self,
        handle: Option<u32>,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_calendar_normal_contents_table_query_position_handle = handle;
        self.post_hierarchy_actions
            .last_calendar_normal_contents_table_query_position_context = context;
    }

    pub(in crate::mapi) fn record_calendar_normal_contents_table_query_rows(
        &mut self,
        handle: Option<u32>,
        context: String,
    ) {
        self.post_hierarchy_actions
            .calendar_normal_contents_table_query_rows_observed = true;
        self.post_hierarchy_actions
            .last_calendar_normal_contents_table_query_rows_handle = handle;
        self.post_hierarchy_actions
            .last_calendar_normal_contents_table_query_rows_context = context;
    }

    pub(in crate::mapi) fn record_default_view_normal_contents_table_query_rows(
        &mut self,
        handle: Option<u32>,
        context: String,
    ) {
        self.post_hierarchy_actions
            .default_view_normal_contents_table_query_rows_observed = true;
        self.post_hierarchy_actions
            .last_default_view_normal_contents_table_query_rows_handle = handle;
        self.post_hierarchy_actions
            .last_default_view_normal_contents_table_query_rows_context = context;
        if !self
            .post_hierarchy_actions
            .last_default_view_folder_open_context
            .is_empty()
        {
            self.post_hierarchy_actions
                .last_default_view_folder_open_followed_by_query_rows = true;
        }
    }

    pub(in crate::mapi) fn record_default_view_folder_opened(
        &mut self,
        handle: u32,
        folder_id: u64,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_default_view_folder_open_handle = Some(handle);
        self.post_hierarchy_actions
            .last_default_view_folder_open_folder_id = Some(folder_id);
        self.post_hierarchy_actions
            .last_default_view_folder_open_context = context.clone();
        self.post_hierarchy_actions
            .last_default_view_folder_open_followed_by_query_rows = false;
        self.record_outlook_view_failure_trace_event(format!("default_view_folder_open:{context}"));
    }

    pub(in crate::mapi) fn record_post_calendar_query_position_named_property_probe(
        &mut self,
        context: String,
    ) {
        if self
            .post_hierarchy_actions
            .last_calendar_normal_contents_table_query_position_context
            .is_empty()
            || self
                .post_hierarchy_actions
                .calendar_normal_contents_table_query_rows_observed
        {
            return;
        }
        self.post_hierarchy_actions
            .post_calendar_query_position_named_property_probe_count += 1;
        self.post_hierarchy_actions
            .last_post_calendar_query_position_named_property_context = context.clone();
        self.record_outlook_view_failure_trace_event(format!(
            "post_calendar_query_position_named_properties:{context}"
        ));
        crate::mapi::record_mapi_outlook_view_post_calendar_query_position_named_property_probe();
    }

    pub(in crate::mapi) fn record_inbox_associated_contents_table(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_contents_table_observed = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_broad_findrow(&mut self, matched: bool) {
        if matched {
            self.post_hierarchy_actions
                .inbox_associated_broad_ipm_configuration_findrow_matched = true;
        }
    }

    pub(in crate::mapi) fn record_inbox_associated_exact_findrow(&mut self, matched: bool) {
        if matched {
            self.post_hierarchy_actions
                .inbox_associated_exact_ipm_configuration_findrow_matched = true;
        }
    }

    pub(in crate::mapi) fn record_inbox_associated_findrow_returned_content(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_findrow_returned_content = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_query_rows_returned_non_empty(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_query_rows_returned_non_empty = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_non_empty_query_context(
        &mut self,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_inbox_associated_non_empty_query_context = context;
    }

    pub(in crate::mapi) fn record_inbox_associated_query_rows_reached_end(
        &mut self,
        context: String,
    ) {
        self.post_hierarchy_actions
            .inbox_associated_query_rows_reached_end = true;
        self.post_hierarchy_actions
            .last_inbox_associated_end_query_context = context;
    }

    pub(in crate::mapi) fn record_receive_folder_verification_passed(&mut self) {
        self.post_hierarchy_actions
            .receive_folder_verification_passed = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_config_open(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_config_open_observed = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_config_stream_open(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_config_stream_open_observed = true;
    }

    pub(in crate::mapi) fn record_inbox_associated_config_stream_handle(&mut self, handle: u32) {
        self.inbox_associated_config_stream_handles.insert(handle);
    }

    pub(in crate::mapi) fn record_inbox_rule_organizer_stream_handle(&mut self, handle: u32) {
        self.inbox_rule_organizer_stream_handles.insert(handle);
    }

    pub(in crate::mapi) fn is_inbox_associated_config_stream_handle(&self, handle: u32) -> bool {
        self.inbox_associated_config_stream_handles
            .contains(&handle)
    }

    pub(in crate::mapi) fn is_inbox_rule_organizer_stream_handle(&self, handle: u32) -> bool {
        self.inbox_rule_organizer_stream_handles.contains(&handle)
    }

    pub(in crate::mapi) fn record_inbox_associated_config_stream_read(&mut self) {
        self.post_hierarchy_actions
            .inbox_associated_config_stream_read_observed = true;
    }

    pub(in crate::mapi) fn record_inbox_rule_organizer_stream_read(&mut self, context: String) {
        self.post_hierarchy_actions
            .inbox_rule_organizer_stream_read_observed = true;
        self.post_hierarchy_actions
            .last_inbox_rule_organizer_stream_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_open_folder_context(&mut self, context: String) {
        self.post_hierarchy_actions.last_inbox_open_folder_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_contents_table_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_contents_table_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_associated_query_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_associated_query_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_associated_find_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_associated_find_context = context;
    }

    pub(in crate::mapi) fn record_last_common_views_inbox_shortcut_context(
        &mut self,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_common_views_inbox_shortcut_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_notification_registration_context(
        &mut self,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_inbox_notification_registration_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_hierarchy_table_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_hierarchy_table_context = context;
    }

    pub(in crate::mapi) fn record_last_inbox_hierarchy_query_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_hierarchy_query_context = context;
    }

    pub(in crate::mapi) fn record_last_hierarchy_table_query_position_context(
        &mut self,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_hierarchy_table_query_position_context = context.clone();
        if self
            .post_hierarchy_actions
            .last_inbox_related_release_context
            .contains("visible_inbox_release_without_query_rows=true")
            && self
                .post_hierarchy_actions
                .inbox_normal_contents_table_setcolumns_observed
            && !self
                .post_hierarchy_actions
                .inbox_normal_contents_table_query_rows_observed
            && !self
                .post_hierarchy_actions
                .inbox_normal_contents_table_find_row_observed
        {
            if self
                .post_hierarchy_actions
                .first_post_visible_release_hierarchy_query_position_context
                .is_empty()
            {
                self.post_hierarchy_actions
                    .first_post_visible_release_hierarchy_query_position_context = context.clone();
            }
            self.post_hierarchy_actions
                .post_visible_release_hierarchy_query_position_count += 1;
        }
        if self
            .post_hierarchy_actions
            .last_inbox_related_release_context
            .contains("visible_inbox_release_without_query_rows=true")
            && self
                .post_hierarchy_actions
                .inbox_normal_contents_table_setcolumns_observed
            && !self
                .post_hierarchy_actions
                .inbox_normal_contents_table_query_rows_observed
            && self
                .post_hierarchy_actions
                .inbox_normal_contents_table_find_row_observed
        {
            if self
                .post_hierarchy_actions
                .first_post_visible_findrow_release_hierarchy_query_position_context
                .is_empty()
            {
                self.post_hierarchy_actions
                    .first_post_visible_findrow_release_hierarchy_query_position_context =
                    context.clone();
            }
            self.post_hierarchy_actions
                .post_visible_findrow_release_hierarchy_query_position_count += 1;
        }
        self.record_outlook_view_failure_trace_event(format!("hierarchy_query_position:{context}"));
    }

    pub(in crate::mapi) fn record_last_inbox_related_release_context(&mut self, context: String) {
        self.post_hierarchy_actions
            .last_inbox_related_release_context = context;
    }

    pub(in crate::mapi) fn record_last_post_hierarchy_create_save_object_context(
        &mut self,
        context: String,
    ) {
        if self.hierarchy_sync_completed() {
            self.post_hierarchy_actions
                .last_post_hierarchy_create_save_object_context = context.clone();
            self.record_outlook_view_failure_trace_event(format!(
                "post_hierarchy_create_save_object:{context}"
            ));
        }
    }

    pub(in crate::mapi) fn record_post_hierarchy_submit_attempt_context(
        &mut self,
        context: String,
    ) {
        if self.hierarchy_sync_completed() {
            self.post_hierarchy_actions
                .post_hierarchy_submit_attempt_count = self
                .post_hierarchy_actions
                .post_hierarchy_submit_attempt_count
                .saturating_add(1);
            let context = format!(
                "attempt_count={};{context}",
                self.post_hierarchy_actions
                    .post_hierarchy_submit_attempt_count
            );
            self.post_hierarchy_actions
                .last_post_hierarchy_submit_attempt_context = context.clone();
            self.record_outlook_view_failure_trace_event(format!(
                "post_hierarchy_submit_attempt:{context}"
            ));
        }
    }

    pub(in crate::mapi) fn record_last_inbox_folder_type_getprops_context(
        &mut self,
        context: String,
    ) {
        self.post_hierarchy_actions
            .last_inbox_folder_type_getprops_context = context;
    }

    pub(in crate::mapi) fn record_last_successful_execute_context(
        &mut self,
        context: String,
        has_non_release_rop: bool,
    ) {
        self.post_hierarchy_actions.last_successful_execute_context = context.clone();
        if has_non_release_rop {
            self.post_hierarchy_actions
                .last_successful_non_release_execute_context = context;
        }
    }

    pub(in crate::mapi) fn record_last_table_context(&mut self, context: String) {
        self.post_hierarchy_actions.last_table_context = context;
    }

    pub(in crate::mapi) fn record_last_table_query_rows_context(&mut self, context: String) {
        self.post_hierarchy_actions.last_table_query_rows_context = context.clone();
        self.post_hierarchy_actions.last_table_context = context;
    }

    pub(in crate::mapi) fn record_last_table_release_context(&mut self, context: String) {
        self.post_hierarchy_actions.last_table_release_context = context.clone();
        self.post_hierarchy_actions.last_table_context = context;
    }

    pub(in crate::mapi) fn abandoned_after_inbox_fai_query_rows(&self) -> bool {
        let actions = &self.post_hierarchy_actions;
        actions.inbox_associated_contents_table_observed
            && !actions.inbox_normal_contents_table_observed
            && !actions.inbox_normal_contents_table_query_rows_observed
            && !actions.inbox_associated_findrow_returned_content
            && !actions.last_inbox_associated_query_context.is_empty()
            && actions
                .last_table_release_context
                .contains("folder=0x0000000000050001;role=inbox;associated=true")
    }

    pub(in crate::mapi) fn record_first_inbox_loop_transition_context(&mut self, context: String) {
        if self
            .post_hierarchy_actions
            .first_inbox_loop_transition_context
            .is_empty()
        {
            self.post_hierarchy_actions
                .first_inbox_loop_transition_context = context;
        }
    }

    pub(in crate::mapi) fn mark_inbox_loop_transition_logged(&mut self) {
        self.post_hierarchy_actions.inbox_loop_transition_logged = true;
    }

    pub(in crate::mapi) fn mark_post_inbox_fai_handoff_logged(&mut self) {
        self.post_hierarchy_actions.post_inbox_fai_handoff_logged = true;
    }

    pub(in crate::mapi) fn mark_post_common_views_handoff_logged(&mut self) {
        self.post_hierarchy_actions.post_common_views_handoff_logged = true;
    }

    pub(in crate::mapi) fn mark_post_common_views_notification_handoff_logged(&mut self) {
        self.post_hierarchy_actions
            .post_common_views_notification_handoff_logged = true;
    }

    pub(in crate::mapi) fn mark_post_common_views_inbox_open_loop_metric_logged(&mut self) {
        self.post_hierarchy_actions
            .post_common_views_inbox_open_loop_metric_logged = true;
    }

    pub(in crate::mapi) fn mark_post_inbox_fai_reopen_logged(&mut self) {
        self.post_hierarchy_actions.post_inbox_fai_reopen_logged = true;
    }

    pub(in crate::mapi) fn mark_post_inbox_fai_folder_type_probe_loop_logged(&mut self) {
        self.post_hierarchy_actions
            .post_inbox_fai_folder_type_probe_loop_logged = true;
    }

    pub(in crate::mapi) fn mark_post_rule_organizer_stream_reopen_logged(&mut self) {
        self.post_hierarchy_actions
            .post_rule_organizer_stream_reopen_logged = true;
    }

    pub(in crate::mapi) fn record_recent_probe_action(&mut self, action: String) {
        if self.post_hierarchy_actions.recent_probe_actions.len() >= 8 {
            self.post_hierarchy_actions.recent_probe_actions.remove(0);
        }
        self.post_hierarchy_actions
            .recent_probe_actions
            .push(action);
    }

    pub(in crate::mapi) fn record_outlook_view_failure_trace_event(&mut self, event: String) {
        if self
            .post_hierarchy_actions
            .outlook_view_failure_trace_events
            .len()
            >= MAX_OUTLOOK_VIEW_FAILURE_TRACE_EVENTS
        {
            self.post_hierarchy_actions
                .outlook_view_failure_trace_events
                .remove(0);
        }
        self.post_hierarchy_actions
            .outlook_view_failure_trace_events
            .push(event);
    }

    pub(in crate::mapi) fn record_default_view_advertised(
        &mut self,
        request_id: &str,
        owner_folder_id: u64,
        view_folder_id: u64,
        view_message_id: u64,
        view_name: &str,
    ) {
        let prior_open = self
            .default_view_advertisements
            .get(&owner_folder_id)
            .filter(|state| {
                state.view_folder_id == view_folder_id && state.view_message_id == view_message_id
            })
            .map(|state| (state.opened, state.open_request_id.clone()));
        self.post_hierarchy_actions
            .last_advertised_default_view_owner_folder_id = Some(owner_folder_id);
        self.post_hierarchy_actions
            .last_advertised_default_view_folder_id = Some(view_folder_id);
        self.post_hierarchy_actions
            .last_advertised_default_view_message_id = Some(view_message_id);
        self.post_hierarchy_actions
            .last_advertised_default_view_name = view_name.to_string();
        self.post_hierarchy_actions
            .last_advertised_default_view_request_id = request_id.to_string();
        self.post_hierarchy_actions
            .last_advertised_default_view_opened = prior_open
            .as_ref()
            .map(|(opened, _)| *opened)
            .unwrap_or(false);
        self.post_hierarchy_actions
            .last_advertised_default_view_open_request_id = prior_open
            .as_ref()
            .map(|(_, open_request_id)| open_request_id.clone())
            .unwrap_or_default();
        self.default_view_advertisements.insert(
            owner_folder_id,
            DefaultViewAdvertisementState {
                owner_folder_id,
                view_folder_id,
                view_message_id,
                view_name: view_name.to_string(),
                request_id: request_id.to_string(),
                opened: prior_open
                    .as_ref()
                    .map(|(opened, _)| *opened)
                    .unwrap_or(false),
                open_request_id: prior_open
                    .map(|(_, open_request_id)| open_request_id)
                    .unwrap_or_default(),
            },
        );
        self.record_outlook_view_failure_trace_event(format!(
            "default_view_advertised:request_id={request_id};owner_folder=0x{owner_folder_id:016x};view_folder=0x{view_folder_id:016x};view=0x{view_message_id:016x};name={view_name}"
        ));
    }

    pub(in crate::mapi) fn record_default_view_opened(
        &mut self,
        request_id: &str,
        view_folder_id: u64,
        view_message_id: u64,
    ) -> bool {
        let matched_owner =
            self.default_view_advertisements
                .iter()
                .find_map(|(owner_folder_id, state)| {
                    (state.view_folder_id == view_folder_id
                        && state.view_message_id == view_message_id)
                        .then_some(*owner_folder_id)
                });
        if let Some(owner_folder_id) = matched_owner {
            if let Some(state) = self.default_view_advertisements.get_mut(&owner_folder_id) {
                state.opened = true;
                state.open_request_id = request_id.to_string();
            }
        }
        let last_matched = self
            .post_hierarchy_actions
            .last_advertised_default_view_folder_id
            == Some(view_folder_id)
            && self
                .post_hierarchy_actions
                .last_advertised_default_view_message_id
                == Some(view_message_id);
        if last_matched {
            self.post_hierarchy_actions
                .last_advertised_default_view_opened = true;
            self.post_hierarchy_actions
                .last_advertised_default_view_open_request_id = request_id.to_string();
        }
        matched_owner.is_some() || last_matched
    }

    fn format_default_view_advertisement_state(state: &DefaultViewAdvertisementState) -> String {
        format!(
            "owner_folder=0x{:016x};owner_role={};view_folder=0x{:016x};view=0x{:016x};name={};advertised_request={};opened={};open_request={}",
            state.owner_folder_id,
            debug_role_for_folder_id(state.owner_folder_id),
            state.view_folder_id,
            state.view_message_id,
            state.view_name,
            state.request_id,
            state.opened,
            if state.open_request_id.is_empty() {
                "none"
            } else {
                &state.open_request_id
            }
        )
    }

    pub(in crate::mapi) fn default_view_advertisement_state(&self) -> String {
        let actions = &self.post_hierarchy_actions;
        let Some(owner_folder_id) = actions.last_advertised_default_view_owner_folder_id else {
            return "none".to_string();
        };
        let view_folder_id = actions
            .last_advertised_default_view_folder_id
            .map(|id| format!("0x{id:016x}"))
            .unwrap_or_else(|| "none".to_string());
        let view_message_id = actions
            .last_advertised_default_view_message_id
            .map(|id| format!("0x{id:016x}"))
            .unwrap_or_else(|| "none".to_string());
        format!(
            "owner_folder=0x{owner_folder_id:016x};owner_role={};view_folder={view_folder_id};view={view_message_id};name={};advertised_request={};opened={};open_request={}",
            debug_role_for_folder_id(owner_folder_id),
            actions.last_advertised_default_view_name,
            actions.last_advertised_default_view_request_id,
            actions.last_advertised_default_view_opened,
            if actions
                .last_advertised_default_view_open_request_id
                .is_empty()
            {
                "none"
            } else {
                &actions.last_advertised_default_view_open_request_id
            }
        )
    }

    pub(in crate::mapi) fn default_view_advertisement_state_for_folder(
        &self,
        owner_folder_id: u64,
    ) -> String {
        if let Some(state) = self.default_view_advertisements.get(&owner_folder_id) {
            return Self::format_default_view_advertisement_state(state);
        }
        if self.default_view_advertisements.is_empty() {
            return "none".to_string();
        }
        format!(
            "none_for_folder=0x{owner_folder_id:016x};owner_role={};advertisement_summary={}",
            debug_role_for_folder_id(owner_folder_id),
            self.default_view_advertisement_summary()
        )
    }

    pub(in crate::mapi) fn default_view_advertisement_summary(&self) -> String {
        if self.default_view_advertisements.is_empty() {
            return self.default_view_advertisement_state();
        }
        let mut states: Vec<_> = self.default_view_advertisements.values().collect();
        states.sort_by_key(|state| (state.owner_folder_id, state.view_folder_id));
        states
            .into_iter()
            .map(Self::format_default_view_advertisement_state)
            .collect::<Vec<_>>()
            .join("|")
    }

    pub(in crate::mapi) fn default_view_folder_open_match_state(
        &self,
        opened_folder_id: u64,
        default_view_target: Option<(u64, u64)>,
    ) -> String {
        if let Some(advertised) = self.default_view_advertisements.get(&opened_folder_id) {
            let entry_id_decoded = default_view_target.is_some();
            let entry_id_matches_advertised = default_view_target
                .map(|(view_folder_id, view_message_id)| {
                    advertised.view_folder_id == view_folder_id
                        && advertised.view_message_id == view_message_id
                })
                .unwrap_or(false);
            return format!(
                "advertised=true;opened_folder_matches_owner=true;entry_id_decoded={entry_id_decoded};entry_id_matches_advertised={entry_id_matches_advertised};pending_open={}",
                !advertised.opened
            );
        }
        let Some(last_advertised_owner_folder_id) = self
            .post_hierarchy_actions
            .last_advertised_default_view_owner_folder_id
        else {
            return "advertised=false;advertised_for_folder=false;opened_folder_matches_owner=false;entry_id_decoded=false;entry_id_matches_advertised=false;pending_open=false".to_string();
        };
        let entry_id_decoded = default_view_target.is_some();
        let entry_id_matches_advertised = default_view_target
            .map(|(view_folder_id, view_message_id)| {
                self.post_hierarchy_actions
                    .last_advertised_default_view_folder_id
                    == Some(view_folder_id)
                    && self
                        .post_hierarchy_actions
                        .last_advertised_default_view_message_id
                        == Some(view_message_id)
            })
            .unwrap_or(false);
        format!(
            "advertised=false;advertised_for_folder=false;last_advertised_owner=0x{last_advertised_owner_folder_id:016x};last_advertised_owner_role={};opened_folder_matches_owner={};entry_id_decoded={entry_id_decoded};entry_id_matches_last_advertised={entry_id_matches_advertised};pending_last_advertised_open={}",
            debug_role_for_folder_id(last_advertised_owner_folder_id),
            opened_folder_id == last_advertised_owner_folder_id,
            !self
                .post_hierarchy_actions
                .last_advertised_default_view_opened
        )
    }

    pub(in crate::mapi) fn advertised_default_view_pending_open(&self) -> bool {
        if self
            .default_view_advertisements
            .values()
            .any(|state| !state.opened)
        {
            return true;
        }
        self.post_hierarchy_actions
            .last_advertised_default_view_owner_folder_id
            .is_some()
            && !self
                .post_hierarchy_actions
                .last_advertised_default_view_opened
    }

    pub(in crate::mapi) fn record_outlook_stream_batch_observed(&mut self, summary: String) {
        self.post_hierarchy_actions.outlook_stream_batch_observed = true;
        if self
            .post_hierarchy_actions
            .outlook_stream_batch_summaries
            .len()
            >= MAX_OUTLOOK_STREAM_BATCH_EVENTS
        {
            self.post_hierarchy_actions
                .outlook_stream_batch_summaries
                .remove(0);
        }
        self.post_hierarchy_actions
            .outlook_stream_batch_summaries
            .push(summary);
    }

    pub(in crate::mapi) fn record_post_hierarchy_request_contract(&mut self, contract: String) {
        if !self.hierarchy_sync_completed()
            || self.post_hierarchy_actions.content_sync_configure_observed
            || contract.is_empty()
        {
            return;
        }
        if self.post_hierarchy_actions.request_contract_sequence.len()
            >= MAX_POST_HIERARCHY_REQUEST_CONTRACTS
        {
            self.post_hierarchy_actions
                .request_contract_sequence
                .remove(0);
        }
        self.post_hierarchy_actions
            .request_contract_sequence
            .push(contract);
    }

    pub(in crate::mapi) fn record_post_hierarchy_getprops_contract(&mut self, contract: String) {
        if !self.hierarchy_sync_completed()
            || self.post_hierarchy_actions.content_sync_configure_observed
            || contract.is_empty()
        {
            return;
        }
        self.post_hierarchy_actions.last_getprops_request_contract = contract;
    }

    pub(in crate::mapi) fn record_post_hierarchy_setprops_contract(&mut self, contract: String) {
        if !self.hierarchy_sync_completed()
            || self.post_hierarchy_actions.content_sync_configure_observed
            || contract.is_empty()
        {
            return;
        }
        self.post_hierarchy_actions.last_setprops_request_contract = contract;
    }

    pub(in crate::mapi) fn record_special_folder_alias(&mut self, alias_id: u64, folder_id: u64) {
        if alias_id == folder_id {
            return;
        }
        self.special_folder_aliases.insert(alias_id, folder_id);
    }

    pub(in crate::mapi) fn replace_special_folder_aliases(
        &mut self,
        aliases: impl IntoIterator<Item = (u64, u64)>,
    ) {
        for (alias_id, folder_id) in aliases {
            self.record_special_folder_alias(alias_id, folder_id);
        }
    }

    pub(in crate::mapi) fn remember_search_folder_definition(
        &mut self,
        folder_id: u64,
        definition: SearchFolderDefinition,
    ) {
        self.saved_search_folder_definitions
            .insert(folder_id, MapiSavedSearchFolderDefinition { definition });
    }

    pub(in crate::mapi) fn search_folder_definition(
        &self,
        folder_id: u64,
    ) -> Option<&SearchFolderDefinition> {
        self.saved_search_folder_definitions
            .get(&folder_id)
            .map(|saved| &saved.definition)
    }

    pub(in crate::mapi) fn forget_search_folder_definition(
        &mut self,
        folder_id: u64,
    ) -> Option<SearchFolderDefinition> {
        let definition = self
            .saved_search_folder_definitions
            .remove(&folder_id)
            .map(|saved| saved.definition);
        if definition.is_some() {
            self.deleted_search_folder_definitions.insert(folder_id);
        }
        definition
    }

    pub(in crate::mapi) fn search_folder_definition_was_deleted(&self, folder_id: u64) -> bool {
        self.deleted_search_folder_definitions.contains(&folder_id)
    }

    pub(in crate::mapi) fn resolve_special_folder_alias(&self, folder_id: u64) -> u64 {
        self.special_folder_aliases
            .get(&folder_id)
            .copied()
            .unwrap_or(folder_id)
    }

    pub(in crate::mapi) fn record_deleted_advertised_special_folder(&mut self, folder_id: u64) {
        self.deleted_advertised_special_folders.insert(folder_id);
    }

    pub(in crate::mapi) fn advertised_special_folder_was_deleted(&self, folder_id: u64) -> bool {
        self.deleted_advertised_special_folders.contains(&folder_id)
    }

    pub(in crate::mapi) fn allocate_output_handle(
        &mut self,
        output_handle_index: Option<u8>,
        object: MapiObject,
    ) -> u32 {
        self.allocate_output_handle_avoiding(output_handle_index, object, &HashSet::new())
    }

    pub(in crate::mapi) fn allocate_output_handle_avoiding(
        &mut self,
        output_handle_index: Option<u8>,
        object: MapiObject,
        reserved_handles: &HashSet<u32>,
    ) -> u32 {
        while self.handles.contains_key(&self.next_handle)
            || reserved_handles.contains(&self.next_handle)
        {
            self.next_handle = self.next_handle.saturating_add(1).max(1);
        }
        let preferred = output_handle_index.map(|index| index as u32 + 1);
        let handle = preferred
            .filter(|handle| {
                *handle >= self.next_handle
                    && !self.handles.contains_key(handle)
                    && !reserved_handles.contains(handle)
            })
            .unwrap_or(self.next_handle);
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        if handle >= self.next_handle {
            self.next_handle = handle.saturating_add(1).max(1);
        }
        self.handles.insert(handle, object);
        handle
    }

    pub(in crate::mapi) fn hierarchy_sync_completed(&self) -> bool {
        self.post_hierarchy_actions
            .last_completed_hierarchy_sync_root
            .is_some()
    }

    pub(in crate::mapi) fn record_completed_hierarchy_sync(
        &mut self,
        sync_root_folder_id: u64,
        get_buffer_summary: String,
        default_folder_membership_summary: String,
    ) {
        self.post_hierarchy_actions
            .last_completed_hierarchy_sync_root = Some(sync_root_folder_id);
        self.post_hierarchy_actions
            .last_successful_hierarchy_get_buffer_summary = get_buffer_summary;
        self.post_hierarchy_actions
            .last_default_folder_hierarchy_membership_summary = default_folder_membership_summary;
    }

    pub(in crate::mapi) fn record_content_sync_configure(&mut self) {
        self.post_hierarchy_actions.content_sync_configure_observed = true;
    }

    pub(in crate::mapi) fn record_logoff_after_hierarchy_completion(&mut self) {
        if self.hierarchy_sync_completed() {
            self.post_hierarchy_actions.logoff_client_initiated = true;
        }
    }

    pub(in crate::mapi) fn record_execute_after_hierarchy_completion(
        &mut self,
        rop_ids: &[u8],
        rop_names: &str,
    ) -> PostHierarchyExecuteObservation {
        if !self.hierarchy_sync_completed() {
            return PostHierarchyExecuteObservation::default();
        }
        let first_execute = self.post_hierarchy_actions.execute_count == 0;
        let contains_bootstrap_probe = rop_ids.iter().any(|rop_id| matches!(rop_id, 0x02 | 0x07));
        let first_bootstrap_probe =
            contains_bootstrap_probe && !self.post_hierarchy_actions.bootstrap_probe_observed;
        if contains_bootstrap_probe {
            self.post_hierarchy_actions.bootstrap_probe_observed = true;
        }
        let contains_set_properties_probe =
            rop_ids.iter().any(|rop_id| matches!(rop_id, 0x0A | 0x79));
        let first_set_properties_probe = contains_set_properties_probe
            && !self.post_hierarchy_actions.set_properties_probe_observed;
        if contains_set_properties_probe {
            self.post_hierarchy_actions.set_properties_probe_observed = true;
        }
        self.post_hierarchy_actions.execute_count =
            self.post_hierarchy_actions.execute_count.saturating_add(1);
        for rop_id in rop_ids.iter().copied() {
            if self.post_hierarchy_actions.rop_ids_seen.len() < MAX_POST_HIERARCHY_ROP_IDS
                && !self.post_hierarchy_actions.rop_ids_seen.contains(&rop_id)
            {
                self.post_hierarchy_actions.rop_ids_seen.push(rop_id);
            }
        }
        if rop_ids.contains(&0x01) {
            self.post_hierarchy_actions.release_client_initiated = true;
        }
        self.record_post_visible_inbox_release_create_save_batch(rop_ids, rop_names);
        self.record_visible_inbox_open_create_save_batch(rop_ids, rop_names);
        PostHierarchyExecuteObservation {
            first_execute,
            first_bootstrap_probe,
            first_set_properties_probe,
        }
    }

    fn execute_has_create_save_batch(rop_ids: &[u8]) -> bool {
        let has_create = rop_ids.contains(&RopId::CreateMessage.as_u8());
        let has_save = rop_ids.contains(&RopId::SaveChangesMessage.as_u8());
        let has_set_properties = rop_ids.iter().any(|rop_id| {
            matches!(
                RopId::from_u8(*rop_id),
                Some(RopId::SetProperties | RopId::SetPropertiesNoReplicate)
            )
        });
        has_create && has_save && has_set_properties
    }

    fn record_visible_inbox_open_create_save_batch(&mut self, rop_ids: &[u8], rop_names: &str) {
        if !Self::execute_has_create_save_batch(rop_ids) {
            return;
        }
        let actions = &mut self.post_hierarchy_actions;
        if !actions.inbox_normal_contents_table_observed
            || actions.inbox_normal_contents_table_query_rows_observed
            || actions.inbox_normal_contents_table_find_row_observed
            || actions
                .last_inbox_related_release_context
                .contains("visible_inbox_release_without_query_rows=true")
        {
            return;
        }
        actions.visible_inbox_open_create_save_batch_count = actions
            .visible_inbox_open_create_save_batch_count
            .saturating_add(1);
        let context = format!(
            "request_rops={rop_names};batch_count={};last_contents_table={};last_setcolumns={};last_query_rows={};last_findrow={};recent_actions={}",
            actions.visible_inbox_open_create_save_batch_count,
            session_debug_context_or_none(&actions.last_inbox_contents_table_context),
            session_debug_context_or_none(
                &actions.last_inbox_normal_contents_table_setcolumns_context
            ),
            session_debug_context_or_none(&actions.last_inbox_normal_contents_table_query_rows_context),
            session_debug_context_or_none(&actions.last_inbox_normal_contents_table_find_row_context),
            actions.recent_probe_actions.join(">")
        );
        actions.last_visible_inbox_open_create_save_batch_context = context.clone();
        self.record_outlook_view_failure_trace_event(format!(
            "visible_inbox_open_create_save_batch:{context}"
        ));
    }

    fn record_post_visible_inbox_release_create_save_batch(
        &mut self,
        rop_ids: &[u8],
        rop_names: &str,
    ) {
        if !Self::execute_has_create_save_batch(rop_ids) {
            return;
        }
        let actions = &mut self.post_hierarchy_actions;
        if !actions.inbox_normal_contents_table_setcolumns_observed
            || actions.inbox_normal_contents_table_query_rows_observed
            || !actions
                .last_inbox_related_release_context
                .contains("visible_inbox_release_without_query_rows=true")
        {
            return;
        }
        actions.post_visible_inbox_release_create_save_batch_count = actions
            .post_visible_inbox_release_create_save_batch_count
            .saturating_add(1);
        let context = format!(
            "request_rops={rop_names};batch_count={};last_visible_release={};recent_actions={}",
            actions.post_visible_inbox_release_create_save_batch_count,
            actions.last_inbox_related_release_context,
            actions.recent_probe_actions.join(">")
        );
        actions.last_post_visible_inbox_release_create_save_batch_context = context.clone();
        self.record_outlook_view_failure_trace_event(format!(
            "post_visible_inbox_release_create_save_batch:{context}"
        ));
    }
}

impl MapiObject {
    pub(in crate::mapi) fn folder_id(&self) -> Option<u64> {
        match self {
            MapiObject::AttachmentStream { .. } | MapiObject::NotificationSubscription { .. } => {
                None
            }
            MapiObject::Logon => Some(ROOT_FOLDER_ID),
            MapiObject::PublicFolderLogon => Some(PUBLIC_FOLDERS_ROOT_FOLDER_ID),
            MapiObject::Folder { folder_id, .. }
            | MapiObject::Message { folder_id, .. }
            | MapiObject::Contact { folder_id, .. }
            | MapiObject::Event { folder_id, .. }
            | MapiObject::Task { folder_id, .. }
            | MapiObject::Note { folder_id, .. }
            | MapiObject::JournalEntry { folder_id, .. }
            | MapiObject::ConversationAction { folder_id, .. }
            | MapiObject::NavigationShortcut { folder_id, .. }
            | MapiObject::CommonViewNamedView { folder_id, .. }
            | MapiObject::SearchFolderDefinitionMessage { folder_id, .. }
            | MapiObject::AssociatedConfig { folder_id, .. }
            | MapiObject::DelegateFreeBusyMessage { folder_id, .. }
            | MapiObject::RecoverableItem { folder_id, .. }
            | MapiObject::PublicFolderItem { folder_id, .. }
            | MapiObject::PendingMessage { folder_id, .. }
            | MapiObject::PendingAssociatedMessage { folder_id, .. }
            | MapiObject::PendingContact { folder_id, .. }
            | MapiObject::PendingEvent { folder_id, .. }
            | MapiObject::PendingTask { folder_id, .. }
            | MapiObject::PendingNote { folder_id, .. }
            | MapiObject::PendingJournalEntry { folder_id, .. }
            | MapiObject::PendingConversationAction { folder_id, .. }
            | MapiObject::PendingNavigationShortcut { folder_id, .. }
            | MapiObject::HierarchyTable { folder_id, .. }
            | MapiObject::ContentsTable { folder_id, .. }
            | MapiObject::AttachmentTable { folder_id, .. }
            | MapiObject::PermissionTable { folder_id, .. }
            | MapiObject::RuleTable { folder_id, .. }
            | MapiObject::Attachment { folder_id, .. }
            | MapiObject::PendingAttachment { folder_id, .. }
            | MapiObject::SavedAttachment { folder_id, .. }
            | MapiObject::SynchronizationSource { folder_id, .. }
            | MapiObject::SynchronizationCollector { folder_id, .. }
            | MapiObject::FastTransferDestination { folder_id, .. } => Some(*folder_id),
        }
    }
}

pub(in crate::mapi) fn input_object<'a>(
    session: &'a MapiSession,
    input_handles: &[u32],
    request: &RopRequest,
) -> Option<&'a MapiObject> {
    let handle = input_handle(input_handles, request)?;
    session.handles.get(&handle)
}

pub(in crate::mapi) fn input_object_mut<'a>(
    session: &'a mut MapiSession,
    input_handles: &[u32],
    request: &RopRequest,
) -> Option<&'a mut MapiObject> {
    let handle = input_handle(input_handles, request)?;
    session.handles.get_mut(&handle)
}

pub(in crate::mapi) fn synchronization_context_state(
    object: Option<&MapiObject>,
) -> Option<(
    u64,
    Option<Uuid>,
    MapiCheckpointKind,
    u64,
    u64,
    bool,
    &'static str,
    u8,
    Vec<u8>,
)> {
    match object {
        Some(MapiObject::SynchronizationSource {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            checkpoint_change_sequence,
            checkpoint_modseq,
            checkpoint_store_allowed,
            checkpoint_skip_reason,
            sync_type,
            initial_state,
            state,
            transfer_buffer,
            transfer_position,
            ..
        }) => {
            let transfer_state = if *transfer_position >= transfer_buffer.len() {
                state.clone()
            } else {
                initial_state.clone()
            };
            Some((
                *folder_id,
                *mailbox_id,
                *checkpoint_kind,
                *checkpoint_change_sequence,
                *checkpoint_modseq,
                *checkpoint_store_allowed,
                *checkpoint_skip_reason,
                *sync_type,
                transfer_state,
            ))
        }
        Some(MapiObject::SynchronizationCollector {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            sync_type,
            state,
            ..
        }) => Some((
            *folder_id,
            *mailbox_id,
            *checkpoint_kind,
            0,
            1,
            true,
            "",
            *sync_type,
            state.clone(),
        )),
        _ => None,
    }
}

pub(in crate::mapi) fn input_handle(input_handles: &[u32], request: &RopRequest) -> Option<u32> {
    input_handles
        .get(request.input_handle_index()? as usize)
        .copied()
        .filter(|handle| *handle != u32::MAX)
}

pub(in crate::mapi) fn set_handle_slot(
    handle_slots: &mut Vec<u32>,
    output_handle_index: Option<u8>,
    handle: u32,
) {
    let Some(index) = output_handle_index.map(usize::from) else {
        return;
    };
    if handle_slots.len() <= index {
        handle_slots.resize(index + 1, u32::MAX);
    }
    handle_slots[index] = handle;
}

pub(in crate::mapi) fn release_handle_slot(
    session: &mut MapiSession,
    handle_slots: &mut [u32],
    request: &RopRequest,
) {
    let Some(index) = request.input_handle_index().map(usize::from) else {
        return;
    };
    let Some(handle) = handle_slots.get_mut(index) else {
        return;
    };
    if *handle != u32::MAX {
        session.forget_table_notification_handle(*handle);
        session.handles.remove(handle);
        session.message_handle_generations.remove(handle);
    }
    *handle = u32::MAX;
}

pub(in crate::mapi) fn response_handle_table(
    handle_slots: &[u32],
    output_handles: &[u32],
    echo_input_handles: bool,
) -> Vec<u32> {
    if echo_input_handles {
        return handle_slots.to_vec();
    }
    let mut handles = handle_slots.to_vec();
    while handles.last().is_some_and(|handle| *handle == u32::MAX) {
        handles.pop();
    }
    if handles.is_empty() {
        output_handles.to_vec()
    } else {
        handles
    }
}

pub(in crate::mapi) fn response_handle_table_with_released_handle_sentinel(
    handle_slots: &[u32],
    output_handles: &[u32],
    echo_input_handles: bool,
    released_handle_indexes: &[u8],
) -> Vec<u32> {
    let mut handles = response_handle_table(handle_slots, output_handles, echo_input_handles);
    if !released_handle_indexes.is_empty() {
        // MS-OXCROPS Appendix A notes Exchange can return a non-0xFFFFFFFF invalid handle
        // for released slots in multi-ROP release batches.
        for released_index in released_handle_indexes {
            if let Some(handle) = handles.get_mut(usize::from(*released_index)) {
                if *handle == u32::MAX {
                    *handle = RELEASED_HANDLE_RESPONSE_SENTINEL;
                }
            }
        }
    }
    handles
}

pub(in crate::mapi) fn reset_table_state(object: &mut MapiObject) -> bool {
    match object {
        MapiObject::HierarchyTable {
            columns,
            columns_set,
            sort_orders,
            category_count,
            expanded_count,
            collapsed_categories,
            restriction,
            position,
            bookmarks,
            ..
        } => {
            columns.clear();
            *columns_set = false;
            sort_orders.clear();
            *category_count = 0;
            *expanded_count = 0;
            collapsed_categories.clear();
            *restriction = None;
            *position = 0;
            bookmarks.clear();
            true
        }
        MapiObject::ContentsTable {
            columns,
            columns_set,
            sort_orders,
            category_count,
            expanded_count,
            collapsed_categories,
            restriction,
            position,
            bookmarks,
            ..
        } => {
            columns.clear();
            *columns_set = false;
            sort_orders.clear();
            *category_count = 0;
            *expanded_count = 0;
            collapsed_categories.clear();
            *restriction = None;
            *position = 0;
            bookmarks.clear();
            true
        }
        MapiObject::AttachmentTable {
            columns,
            columns_set,
            sort_orders,
            restriction,
            position,
            bookmarks,
            ..
        } => {
            columns.clear();
            *columns_set = false;
            sort_orders.clear();
            *restriction = None;
            *position = 0;
            bookmarks.clear();
            true
        }
        MapiObject::PermissionTable {
            columns,
            columns_set,
            position,
            ..
        }
        | MapiObject::RuleTable {
            columns,
            columns_set,
            position,
            ..
        } => {
            columns.clear();
            *columns_set = false;
            *position = 0;
            true
        }
        _ => false,
    }
}

pub(in crate::mapi) fn read_handle_table(handle_table: &[u8]) -> Result<Vec<u32>> {
    if handle_table.len() % 4 != 0 {
        return Err(anyhow!("ROP handle table length is not a multiple of 4"));
    }
    Ok(handle_table
        .chunks_exact(4)
        .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        .collect())
}

#[cfg(test)]
mod tests;
