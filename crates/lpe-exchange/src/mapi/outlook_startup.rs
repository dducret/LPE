use super::session::MapiSession;
use super::sync::{INBOX_FOLDER_ID, IPM_SUBTREE_FOLDER_ID};
use super::*;

const STARTUP_GATES: [&str; 10] = [
    "profile_session_established",
    "ipm_subtree_hierarchy_opened",
    "inbox_folder_opened",
    "inbox_associated_contents_table_opened",
    "ipm_configuration_findrow_matched",
    "fai_content_delivered",
    "receive_folder_verification_passed",
    "normal_inbox_contents_table_opened",
    "normal_inbox_visible_row_observed",
    "outlook_did_not_abandon_immediately_after_fai",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct OutlookStartupGateSummary {
    pub(in crate::mapi) last_successful_gate: &'static str,
    pub(in crate::mapi) first_missing_gate: &'static str,
    pub(in crate::mapi) gates: String,
    pub(in crate::mapi) gate_count: usize,
    pub(in crate::mapi) passed_count: usize,
    pub(in crate::mapi) abandoned_immediately_after_fai: bool,
}

pub(in crate::mapi) fn normalized_rop_sequence_signature(names_csv: &str) -> String {
    let mut compressed = Vec::new();
    let mut current = "";
    let mut count = 0usize;
    for name in names_csv
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        if name == current {
            count += 1;
            continue;
        }
        push_compressed_rop(&mut compressed, current, count);
        current = name;
        count = 1;
    }
    push_compressed_rop(&mut compressed, current, count);
    if compressed.is_empty() {
        "empty".to_string()
    } else {
        compressed.join(">")
    }
}

fn push_compressed_rop(compressed: &mut Vec<String>, name: &str, count: usize) {
    if name.is_empty() {
        return;
    }
    if count > 1 {
        compressed.push(format!("{name}x{count}"));
    } else {
        compressed.push(name.to_string());
    }
}

pub(in crate::mapi) fn outlook_startup_gate_summary(
    session: &MapiSession,
) -> OutlookStartupGateSummary {
    let actions = &session.post_hierarchy_actions;
    let abandoned = session.abandoned_after_inbox_fai_query_rows();
    let ipm_subtree_hierarchy_opened = actions.opened_folder_ids.contains(&IPM_SUBTREE_FOLDER_ID)
        || actions.last_completed_hierarchy_sync_root == Some(IPM_SUBTREE_FOLDER_ID)
        || actions.receive_folder_verification_passed
        || !actions.last_common_views_inbox_shortcut_context.is_empty();
    let inbox_folder_opened = actions.opened_folder_ids.contains(&INBOX_FOLDER_ID)
        || actions.inbox_open_folder_probe_count > 0
        || actions.receive_folder_verification_passed;
    let passed = [
        session.logon_identity.is_some(),
        ipm_subtree_hierarchy_opened,
        inbox_folder_opened,
        actions.inbox_associated_contents_table_observed,
        actions.inbox_associated_broad_ipm_configuration_findrow_matched
            || actions.inbox_associated_exact_ipm_configuration_findrow_matched,
        actions.inbox_associated_findrow_returned_content
            || actions.inbox_associated_query_rows_returned_non_empty,
        actions.receive_folder_verification_passed,
        actions.inbox_normal_contents_table_observed,
        actions.inbox_normal_contents_table_query_rows_observed
            || actions.inbox_normal_contents_table_find_row_observed,
        !abandoned,
    ];
    let first_missing_index = passed.iter().position(|passed| !passed);
    let last_successful_gate = match first_missing_index {
        Some(0) => "none",
        Some(index) => STARTUP_GATES[index - 1],
        None => STARTUP_GATES[STARTUP_GATES.len() - 1],
    };
    let first_missing_gate = first_missing_index
        .map(|index| STARTUP_GATES[index])
        .unwrap_or("none");
    OutlookStartupGateSummary {
        last_successful_gate,
        first_missing_gate,
        gates: STARTUP_GATES
            .iter()
            .zip(passed.iter())
            .map(|(gate, passed)| format!("{gate}={passed}"))
            .collect::<Vec<_>>()
            .join(";"),
        gate_count: STARTUP_GATES.len(),
        passed_count: passed.iter().filter(|passed| **passed).count(),
        abandoned_immediately_after_fai: abandoned,
    }
}

pub(in crate::mapi) fn configured_smart_input_variant() -> String {
    let variant = env::var("LPE_MAPI_OUTLOOK_SMART_INPUT_VARIANT")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match variant.as_str() {
        "" | "none" => "none".to_string(),
        "broad_findrow_no_handoff"
        | "fai_cursor_reset_before_query_rows"
        | "synthetic_elc_findrow_not_found" => variant,
        _ => format!("unknown:{variant}"),
    }
}

#[cfg(test)]
mod tests {
    use super::super::session::MapiLogonIdentityDebug;
    use super::*;

    #[test]
    fn normalized_signature_collapses_repeated_release_loops() {
        assert_eq!(
            normalized_rop_sequence_signature("Release,Release,OpenFolder,GetPropertiesSpecific"),
            "Releasex2>OpenFolder>GetPropertiesSpecific"
        );
        assert_eq!(
            normalized_rop_sequence_signature("GetContentsTable,SetColumns,SortTable,FindRow"),
            "GetContentsTable>SetColumns>SortTable>FindRow"
        );
    }

    #[test]
    fn classifier_reports_first_missing_gate_after_fai_query_rows() {
        let mut session = crate::mapi::transport::tests::test_session_for_outlook_startup();
        session.logon_identity = Some(MapiLogonIdentityDebug::default());
        session.record_opened_folder(IPM_SUBTREE_FOLDER_ID);
        session.record_opened_folder(INBOX_FOLDER_ID);
        session.record_inbox_associated_contents_table();
        session.record_inbox_associated_exact_findrow(true);
        session.record_inbox_associated_query_rows_returned_non_empty();
        session.record_receive_folder_verification_passed();

        let summary = outlook_startup_gate_summary(&session);

        assert_eq!(
            summary.last_successful_gate,
            "receive_folder_verification_passed"
        );
        assert_eq!(
            summary.first_missing_gate,
            "normal_inbox_contents_table_opened"
        );
    }

    #[test]
    fn classifier_requires_inbox_query_rows_after_inbox_table_open() {
        let mut session = crate::mapi::transport::tests::test_session_for_outlook_startup();
        session.logon_identity = Some(MapiLogonIdentityDebug::default());
        session.record_opened_folder(IPM_SUBTREE_FOLDER_ID);
        session.record_opened_folder(INBOX_FOLDER_ID);
        session.record_inbox_associated_contents_table();
        session.record_inbox_associated_exact_findrow(true);
        session.record_inbox_associated_query_rows_returned_non_empty();
        session.record_receive_folder_verification_passed();
        session.record_inbox_normal_contents_table();
        session.record_default_view_normal_contents_table_query_rows(None, "role=drafts".into());

        let summary = outlook_startup_gate_summary(&session);

        assert_eq!(
            summary.last_successful_gate,
            "normal_inbox_contents_table_opened"
        );
        assert_eq!(
            summary.first_missing_gate,
            "normal_inbox_visible_row_observed"
        );
        assert!(summary
            .gates
            .contains("normal_inbox_visible_row_observed=false"));
    }

    #[test]
    fn classifier_accepts_inbox_query_rows_after_inbox_table_open() {
        let mut session = crate::mapi::transport::tests::test_session_for_outlook_startup();
        session.logon_identity = Some(MapiLogonIdentityDebug::default());
        session.record_opened_folder(IPM_SUBTREE_FOLDER_ID);
        session.record_opened_folder(INBOX_FOLDER_ID);
        session.record_inbox_associated_contents_table();
        session.record_inbox_associated_exact_findrow(true);
        session.record_inbox_associated_query_rows_returned_non_empty();
        session.record_receive_folder_verification_passed();
        session.record_inbox_normal_contents_table();
        session.record_inbox_normal_contents_table_query_rows(Some(27), "role=inbox".into());

        let summary = outlook_startup_gate_summary(&session);

        assert_eq!(summary.first_missing_gate, "none");
        assert!(summary
            .gates
            .contains("normal_inbox_visible_row_observed=true"));
    }

    #[test]
    fn classifier_accepts_inbox_find_row_after_inbox_table_open() {
        let mut session = crate::mapi::transport::tests::test_session_for_outlook_startup();
        session.logon_identity = Some(MapiLogonIdentityDebug::default());
        session.record_opened_folder(IPM_SUBTREE_FOLDER_ID);
        session.record_opened_folder(INBOX_FOLDER_ID);
        session.record_inbox_associated_contents_table();
        session.record_inbox_associated_exact_findrow(true);
        session.record_inbox_associated_query_rows_returned_non_empty();
        session.record_receive_folder_verification_passed();
        session.record_inbox_normal_contents_table();
        session.record_inbox_normal_contents_table_find_row(Some(27), "role=inbox".into());

        let summary = outlook_startup_gate_summary(&session);

        assert_eq!(summary.first_missing_gate, "none");
        assert!(summary
            .gates
            .contains("normal_inbox_visible_row_observed=true"));
    }

    #[test]
    fn classifier_accepts_exact_ipm_configuration_findrow_gate() {
        let mut session = crate::mapi::transport::tests::test_session_for_outlook_startup();
        session.logon_identity = Some(MapiLogonIdentityDebug::default());
        session.record_opened_folder(IPM_SUBTREE_FOLDER_ID);
        session.record_opened_folder(INBOX_FOLDER_ID);
        session.record_inbox_associated_contents_table();
        session.record_inbox_associated_exact_findrow(true);

        let summary = outlook_startup_gate_summary(&session);

        assert_eq!(
            summary.last_successful_gate,
            "ipm_configuration_findrow_matched"
        );
        assert_eq!(summary.first_missing_gate, "fai_content_delivered");
        assert!(summary
            .gates
            .contains("ipm_configuration_findrow_matched=true"));
    }

    #[test]
    fn classifier_accepts_findrow_delivered_fai_content() {
        let mut session = crate::mapi::transport::tests::test_session_for_outlook_startup();
        session.logon_identity = Some(MapiLogonIdentityDebug::default());
        session.record_opened_folder(IPM_SUBTREE_FOLDER_ID);
        session.record_opened_folder(INBOX_FOLDER_ID);
        session.record_inbox_associated_contents_table();
        session.record_inbox_associated_exact_findrow(true);
        session.record_inbox_associated_findrow_returned_content();

        let summary = outlook_startup_gate_summary(&session);

        assert_eq!(summary.last_successful_gate, "fai_content_delivered");
        assert_eq!(
            summary.first_missing_gate,
            "receive_folder_verification_passed"
        );
        assert!(summary.gates.contains("fai_content_delivered=true"));
    }

    #[test]
    fn classifier_reports_inbox_contents_gate_after_receive_folder_verified() {
        let mut session = crate::mapi::transport::tests::test_session_for_outlook_startup();
        session.logon_identity = Some(MapiLogonIdentityDebug::default());
        session.record_receive_folder_verification_passed();

        let summary = outlook_startup_gate_summary(&session);

        assert_eq!(summary.last_successful_gate, "inbox_folder_opened");
        assert_eq!(
            summary.first_missing_gate,
            "inbox_associated_contents_table_opened"
        );
    }
}
