use super::*;
use lpe_domain::{
    days_from_civil, unix_seconds_from_windows_filetime, windows_filetime_from_unix_seconds,
    WINDOWS_FILETIME_TICKS_PER_SECOND,
};

pub(in crate::mapi) fn event_start_filetime(event: &AccessibleEvent) -> u64 {
    date_time_to_filetime_in_time_zone(&event.date, &event.time, &event.time_zone)
}

pub(in crate::mapi) fn event_end_filetime(event: &AccessibleEvent) -> u64 {
    let start = event_start_filetime(event);
    let duration = event.duration_minutes.max(1) as u64 * 60 * WINDOWS_FILETIME_TICKS_PER_SECOND;
    start.saturating_add(duration)
}

pub(in crate::mapi) fn date_time_to_filetime(date: &str, time: &str) -> u64 {
    let year = date
        .get(0..4)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(1970);
    let month = date
        .get(5..7)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let day = date
        .get(8..10)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let hour = time
        .get(0..2)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let minute = time
        .get(3..5)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let days = days_from_civil(i64::from(year), i64::from(month), i64::from(day)).max(0) as u64;
    let unix_seconds = days
        .saturating_mul(86_400)
        .saturating_add(u64::from(hour.min(23)) * 3_600)
        .saturating_add(u64::from(minute.min(59)) * 60);
    unix_seconds_to_filetime(unix_seconds)
}

pub(in crate::mapi) fn filetime_to_date_time(filetime: i64) -> Option<(String, String)> {
    let filetime = u64::try_from(filetime).ok()?;
    let unix_seconds = filetime_to_unix_seconds(filetime)?;
    Some(unix_seconds_to_date_time(unix_seconds))
}

pub(in crate::mapi) fn date_time_to_filetime_in_time_zone(
    date: &str,
    time: &str,
    time_zone: &str,
) -> u64 {
    let utc_filetime = date_time_to_filetime(date, time);
    if !is_western_europe_calendar_time_zone(time_zone) {
        return utc_filetime;
    }
    let Some(local_unix_seconds) = filetime_to_unix_seconds(utc_filetime) else {
        return utc_filetime;
    };
    let year = date
        .get(0..4)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(1970);
    let offset = western_europe_local_utc_offset_seconds(year, local_unix_seconds);
    unix_seconds_to_filetime(local_unix_seconds.saturating_sub(offset))
}

pub(in crate::mapi) fn filetime_to_date_time_in_time_zone(
    filetime: i64,
    time_zone: &str,
) -> Option<(String, String)> {
    let filetime = u64::try_from(filetime).ok()?;
    let unix_seconds = filetime_to_unix_seconds(filetime)?;
    let offset = if is_western_europe_calendar_time_zone(time_zone) {
        western_europe_utc_offset_seconds(unix_seconds)
    } else {
        0
    };
    Some(unix_seconds_to_date_time(
        unix_seconds.saturating_add(offset),
    ))
}

pub(in crate::mapi) fn is_western_europe_calendar_time_zone(time_zone: &str) -> bool {
    time_zone.eq_ignore_ascii_case("W. Europe Standard Time")
        || time_zone.eq_ignore_ascii_case("Europe/Zurich")
        || time_zone.eq_ignore_ascii_case("Europe/Berlin")
        || time_zone.eq_ignore_ascii_case("Europe/Rome")
        || time_zone.eq_ignore_ascii_case("Europe/Vienna")
        || time_zone
            .eq_ignore_ascii_case("(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna")
}

fn unix_seconds_to_date_time(unix_seconds: u64) -> (String, String) {
    let days = unix_seconds / 86_400;
    let seconds = unix_seconds % 86_400;
    let (year, month, day) = civil_from_unix_days(days as i64);
    let hour = seconds / 3_600;
    let minute = (seconds % 3_600) / 60;
    (
        format!("{year:04}-{month:02}-{day:02}"),
        format!("{hour:02}:{minute:02}"),
    )
}

fn western_europe_local_utc_offset_seconds(year: i32, local_unix_seconds: u64) -> u64 {
    // The recurring Windows rule emitted by LPE starts daylight time at
    // 02:00 local on the last Sunday in March and ends it at 03:00 local on
    // the last Sunday in October. For the repeated October hour, choose the
    // daylight occurrence deterministically.
    let daylight_start = western_europe_transition_seconds(year, 3, 2);
    let standard_start = western_europe_transition_seconds(year, 10, 3);
    if (daylight_start..standard_start).contains(&local_unix_seconds) {
        7_200
    } else {
        3_600
    }
}

fn western_europe_utc_offset_seconds(unix_seconds: u64) -> u64 {
    let (year, _, _) = civil_from_unix_days((unix_seconds / 86_400) as i64);
    // [MS-OXOCAL] sections 2.2.1.5, 2.2.1.6, 3.1.5.5, and 3.1.5.5.1:
    // StartWhole and EndWhole are UTC PtypTime values; the appointment time
    // zone supplies the offset used to recover the floating civil time.
    let daylight_start = western_europe_transition_seconds(year, 3, 1);
    let standard_start = western_europe_transition_seconds(year, 10, 1);
    if (daylight_start..standard_start).contains(&unix_seconds) {
        7_200
    } else {
        3_600
    }
}

fn western_europe_transition_seconds(year: i32, month: u32, hour: u64) -> u64 {
    let last_day = 31i64;
    let last_day_days = days_from_civil(i64::from(year), i64::from(month), last_day);
    let sunday_based_weekday = (last_day_days + 4).rem_euclid(7);
    last_day_days.saturating_sub(sunday_based_weekday).max(0) as u64 * 86_400 + hour * 3_600
}

pub(in crate::mapi) fn unix_seconds_to_filetime(unix_seconds: u64) -> u64 {
    windows_filetime_from_unix_seconds(unix_seconds)
}

pub(in crate::mapi) fn filetime_to_unix_seconds(filetime: u64) -> Option<u64> {
    unix_seconds_from_windows_filetime(filetime)
}
