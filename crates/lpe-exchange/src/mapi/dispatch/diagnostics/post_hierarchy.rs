#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi::dispatch) struct PostHierarchyReleaseDebugEvent {
    pub(in crate::mapi::dispatch) input_handle_index: u8,
    pub(in crate::mapi::dispatch) handle: String,
    pub(in crate::mapi::dispatch) object_kind: String,
    pub(in crate::mapi::dispatch) folder_id: String,
    pub(in crate::mapi::dispatch) remaining_before: usize,
    pub(in crate::mapi::dispatch) remaining_after: usize,
    pub(in crate::mapi::dispatch) logon_before_content_sync: bool,
}

pub(in crate::mapi::dispatch) fn format_post_hierarchy_release_kinds(
    events: &[PostHierarchyReleaseDebugEvent],
) -> String {
    events
        .iter()
        .map(|event| event.object_kind.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi::dispatch) fn format_post_hierarchy_release_context(
    events: &[PostHierarchyReleaseDebugEvent],
) -> String {
    events
        .iter()
        .map(|event| {
            format!(
                "in={};handle={};kind={};folder={};before={};after={};logon_before_content={}",
                event.input_handle_index,
                event.handle,
                event.object_kind,
                event.folder_id,
                event.remaining_before,
                event.remaining_after,
                event.logon_before_content_sync
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(in crate::mapi::dispatch) fn post_sync_release_flags(
    events: &[PostHierarchyReleaseDebugEvent],
) -> String {
    let mut logon = 0usize;
    let mut public_folder_logon = 0usize;
    let mut folder = 0usize;
    let mut message = 0usize;
    let mut contents_table = 0usize;
    let mut hierarchy_table = 0usize;
    let mut synchronization_source = 0usize;
    let mut synchronization_collector = 0usize;
    let mut notification_subscription = 0usize;
    for event in events {
        match event.object_kind.as_str() {
            "logon" => logon += 1,
            "public_folder_logon" => public_folder_logon += 1,
            "folder" => folder += 1,
            "message" => message += 1,
            "contents_table" => contents_table += 1,
            "hierarchy_table" => hierarchy_table += 1,
            "synchronization_source" => synchronization_source += 1,
            "synchronization_collector" => synchronization_collector += 1,
            "notification_subscription" => notification_subscription += 1,
            _ => {}
        }
    }
    format!(
        "logon={logon};public_folder_logon={public_folder_logon};folder={folder};message={message};contents_table={contents_table};hierarchy_table={hierarchy_table};synchronization_source={synchronization_source};synchronization_collector={synchronization_collector};notification_subscription={notification_subscription}"
    )
}
