use super::*;
use crate::mapi_store::MapiRule;

pub(super) const PID_TAG_RULE_ID: u32 = 0x6674_0014;
pub(super) const PID_TAG_RULE_SEQUENCE: u32 = 0x6676_0003;
pub(super) const PID_TAG_RULE_STATE: u32 = 0x6677_0003;
pub(super) const PID_TAG_RULE_USER_FLAGS: u32 = 0x6678_0003;
pub(super) const PID_TAG_RULE_CONDITION: u32 = 0x6679_00FD;
pub(super) const PID_TAG_RULE_ACTIONS: u32 = 0x6680_00FE;
pub(super) const PID_TAG_RULE_PROVIDER: u32 = 0x6681_001F;
pub(super) const PID_TAG_RULE_NAME: u32 = 0x6682_001F;
pub(super) const PID_TAG_RULE_LEVEL: u32 = 0x6683_0003;
pub(super) const PID_TAG_RULE_PROVIDER_DATA: u32 = 0x6684_0102;
pub(super) const ST_ENABLED: u32 = 0x0000_0001;

pub(in crate::mapi) fn rop_get_rules_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x3F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn serialize_rule_row(rule: &MapiRule, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_RULE_ID => write_u64(&mut row, rule.id),
            PID_TAG_RULE_SEQUENCE => write_u32(&mut row, rule_sequence(rule.id)),
            PID_TAG_RULE_STATE => write_u32(&mut row, if rule.is_active { ST_ENABLED } else { 0 }),
            PID_TAG_RULE_USER_FLAGS | PID_TAG_RULE_LEVEL => write_u32(&mut row, 0),
            PID_TAG_RULE_PROVIDER => write_utf16z(&mut row, "LPE Sieve"),
            PID_TAG_RULE_NAME => write_utf16z(&mut row, &rule.name),
            PID_TAG_RULE_PROVIDER_DATA => {
                let data = serde_json::json!({
                    "sourceKind": "sieve_script",
                    "conditionSummary": rule.condition_summary,
                    "actionSummary": rule.action_summary,
                    "updatedAt": rule.updated_at,
                })
                .to_string();
                write_u16_prefixed_bytes(&mut row, data.as_bytes());
            }
            PID_TAG_RULE_CONDITION | PID_TAG_RULE_ACTIONS => {
                write_property_default(&mut row, *column)
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

fn rule_sequence(rule_id: u64) -> u32 {
    crate::mapi::identity::global_counter_from_store_id(rule_id)
        .unwrap_or(rule_id)
        .min(u64::from(u32::MAX)) as u32
}
