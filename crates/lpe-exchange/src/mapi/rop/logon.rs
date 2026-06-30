use super::{write_object_id, write_u32, write_u64, RopRequest};
use crate::mapi::identity::STORE_REPLICA_ID;
use crate::mapi::sync::{PRIVATE_LOGON_SPECIAL_FOLDER_IDS, PUBLIC_LOGON_SPECIAL_FOLDER_IDS};
use crate::mapi::AccountPrincipal;
use crate::mapi_mailstore;
use std::time::{Duration, SystemTime};

pub(in crate::mapi) fn rop_logon_response_body(
    principal: &AccountPrincipal,
    request: &RopRequest,
) -> Vec<u8> {
    let output_handle_index = request.output_handle_index.unwrap_or(0);
    let logon_flags = request.payload.first().copied().unwrap_or(0x01) & 0x07 | 0x01;
    let mut response = Vec::new();
    response.push(0xFE);
    response.push(output_handle_index);
    write_u32(&mut response, 0);
    response.push(logon_flags);
    for folder_id in PRIVATE_LOGON_SPECIAL_FOLDER_IDS {
        write_object_id(&mut response, folder_id);
    }
    response.push(0x07);
    response.extend_from_slice(&principal.account_id.to_bytes_le());
    response.extend_from_slice(&1u16.to_le_bytes());
    response.extend_from_slice(&mapi_mailstore::STORE_REPLICA_GUID);
    let now = SystemTime::now();
    response.extend_from_slice(&logon_time_bytes(now));
    write_u64(&mut response, gwart_time_marker(now));
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_public_folder_logon_response_body(
    principal: &AccountPrincipal,
    request: &RopRequest,
) -> Vec<u8> {
    let output_handle_index = request.output_handle_index.unwrap_or(0);
    let logon_flags = request.payload.first().copied().unwrap_or(0) & 0x07 & !0x01;
    let mut response = Vec::new();
    response.push(0xFE);
    response.push(output_handle_index);
    write_u32(&mut response, 0);
    response.push(logon_flags);
    for folder_id in PUBLIC_LOGON_SPECIAL_FOLDER_IDS {
        write_object_id(&mut response, folder_id);
    }
    response.push(0x00);
    response.extend_from_slice(&principal.tenant_id.to_bytes_le());
    response.extend_from_slice(&STORE_REPLICA_ID.to_le_bytes()[..2]);
    response.extend_from_slice(&mapi_mailstore::STORE_REPLICA_GUID);
    let now = SystemTime::now();
    response.extend_from_slice(&logon_time_bytes(now));
    write_u64(&mut response, gwart_time_marker(now));
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn gwart_time_marker(now: SystemTime) -> u64 {
    now.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
        .max(1)
}

pub(in crate::mapi) fn logon_time_bytes(now: SystemTime) -> [u8; 8] {
    let duration = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    let seconds = duration.as_secs();
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = seconds % 86_400;
    let hour = (seconds_of_day / 3_600) as u8;
    let minute = ((seconds_of_day % 3_600) / 60) as u8;
    let second = (seconds_of_day % 60) as u8;
    let day_of_week = ((days + 4).rem_euclid(7)) as u8;
    let (year, month, day) = civil_from_unix_days(days);
    let year = (year as u16).to_le_bytes();
    [
        second,
        minute,
        hour,
        day_of_week,
        day,
        month,
        year[0],
        year[1],
    ]
}

pub(in crate::mapi) fn civil_from_unix_days(days: i64) -> (i32, u8, u8) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u8, day as u8)
}
