use super::*;

pub(super) fn calendar_recurrence_blob(event: &AccessibleEvent) -> Option<Vec<u8>> {
    let recurrence = recurrence_pattern_from_canonical(event).ok()?;
    let mut value = Vec::new();
    value.extend_from_slice(&0x3004u16.to_le_bytes());
    value.extend_from_slice(&0x3004u16.to_le_bytes());
    value.extend_from_slice(&recurrence.frequency.to_le_bytes());
    value.extend_from_slice(&recurrence.pattern_type.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&recurrence.first_date_time.to_le_bytes());
    value.extend_from_slice(&recurrence.period.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    for extra in &recurrence.pattern_extra {
        value.extend_from_slice(&extra.to_le_bytes());
    }
    value.extend_from_slice(&recurrence.end_type.to_le_bytes());
    value.extend_from_slice(&recurrence.count.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(
        &((recurrence.deleted_dates.len() + recurrence.modified_exceptions.len()) as u32)
            .to_le_bytes(),
    );
    for deleted in &recurrence.deleted_dates {
        value.extend_from_slice(&deleted.to_le_bytes());
    }
    for modified in &recurrence.modified_exceptions {
        value.extend_from_slice(&modified.original_start.to_le_bytes());
    }
    value.extend_from_slice(&(recurrence.modified_exceptions.len() as u32).to_le_bytes());
    for modified in &recurrence.modified_exceptions {
        value.extend_from_slice(&modified.original_start.to_le_bytes());
    }
    value.extend_from_slice(&recurrence_minutes_since_1601(&event.date).to_le_bytes());
    value.extend_from_slice(&recurrence.end_date.to_le_bytes());
    value.extend_from_slice(&0x0000_3006u32.to_le_bytes());
    value.extend_from_slice(&0x0000_3009u32.to_le_bytes());
    value.extend_from_slice(&event_start_minutes(event).to_le_bytes());
    value.extend_from_slice(&event_end_minutes(event).to_le_bytes());
    value.extend_from_slice(&(recurrence.modified_exceptions.len() as u16).to_le_bytes());
    for modified in &recurrence.modified_exceptions {
        value.extend_from_slice(&modified.start.to_le_bytes());
        value.extend_from_slice(&modified.end.to_le_bytes());
        value.extend_from_slice(&modified.original_start.to_le_bytes());
        let override_flags = recurrence_exception_override_flags(modified);
        value.extend_from_slice(&override_flags.to_le_bytes());
        if let Some(title) = modified.title.as_deref() {
            append_recur_ansi_string(&mut value, title);
        }
        if let Some(location) = modified.location.as_deref() {
            append_recur_ansi_string(&mut value, location);
        }
    }
    value.extend_from_slice(&0u32.to_le_bytes());
    for modified in &recurrence.modified_exceptions {
        let override_flags = recurrence_exception_override_flags(modified);
        value.extend_from_slice(&4u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        value.extend_from_slice(&0u32.to_le_bytes());
        if override_flags != 0 {
            value.extend_from_slice(&modified.start.to_le_bytes());
            value.extend_from_slice(&modified.end.to_le_bytes());
            value.extend_from_slice(&modified.original_start.to_le_bytes());
            if let Some(title) = modified.title.as_deref() {
                append_recur_wide_string(&mut value, title);
            }
            if let Some(location) = modified.location.as_deref() {
                append_recur_wide_string(&mut value, location);
            }
            value.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    value.extend_from_slice(&0u32.to_le_bytes());
    Some(value)
}

fn recurrence_exception_override_flags(exception: &CanonicalRecurrenceException) -> u16 {
    let mut flags = 0u16;
    if exception
        .title
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        flags |= 0x0001;
    }
    if exception
        .location
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        flags |= 0x0010;
    }
    flags
}

struct CanonicalRecurrencePattern {
    frequency: u16,
    pattern_type: u16,
    first_date_time: u32,
    period: u32,
    pattern_extra: Vec<u32>,
    end_type: u32,
    count: u32,
    end_date: u32,
    deleted_dates: Vec<u32>,
    modified_exceptions: Vec<CanonicalRecurrenceException>,
}

struct CanonicalRecurrenceException {
    original_start: u32,
    start: u32,
    end: u32,
    title: Option<String>,
    location: Option<String>,
}

fn recurrence_pattern_from_canonical(
    event: &AccessibleEvent,
) -> Result<CanonicalRecurrencePattern> {
    let parts = parse_canonical_recurrence_rule(&event.recurrence_rule);
    let frequency = recurrence_rule_value(&parts, "FREQ").unwrap_or_default();
    let interval = recurrence_rule_value(&parts, "INTERVAL")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1)
        .max(1);
    let by_day = recurrence_rule_value(&parts, "BYDAY").unwrap_or_default();
    let by_month =
        recurrence_rule_value(&parts, "BYMONTH").and_then(|value| value.parse::<u32>().ok());
    let by_month_day =
        recurrence_rule_value(&parts, "BYMONTHDAY").and_then(|value| value.parse::<u32>().ok());
    let by_set_pos =
        recurrence_rule_value(&parts, "BYSETPOS").and_then(|value| value.parse::<i32>().ok());
    let (frequency, pattern_type, period, pattern_extra) = match frequency.as_str() {
        "DAILY" => (
            0x200Au16,
            0x0000u16,
            interval.saturating_mul(1440),
            Vec::new(),
        ),
        "WEEKLY" => (
            0x200Bu16,
            0x0001u16,
            interval,
            vec![recurrence_day_mask(&by_day)?],
        ),
        "MONTHLY" if by_month_day == Some(31) => (0x200Cu16, 0x0004u16, interval, vec![31]),
        "MONTHLY" if by_month_day.is_some() => {
            (0x200Cu16, 0x0002u16, interval, vec![by_month_day.unwrap()])
        }
        "MONTHLY" if !by_day.is_empty() && by_set_pos.is_some() => (
            0x200Cu16,
            0x0003u16,
            interval,
            vec![
                recurrence_day_mask(&by_day)?,
                recurrence_set_position_to_mapi(by_set_pos.unwrap())?,
            ],
        ),
        "YEARLY" if by_month_day.is_some() => {
            (0x200Du16, 0x0002u16, 12, vec![by_month_day.unwrap()])
        }
        "YEARLY" if !by_day.is_empty() && by_set_pos.is_some() => (
            0x200Du16,
            0x0003u16,
            12,
            vec![
                recurrence_day_mask(&by_day)?,
                recurrence_set_position_to_mapi(by_set_pos.unwrap())?,
            ],
        ),
        _ => bail!("unsupported canonical recurrence rule"),
    };
    if period == 0 {
        bail!("unsupported canonical recurrence interval");
    }
    let (end_type, count, end_date) = if let Some(count) =
        recurrence_rule_value(&parts, "COUNT").and_then(|value| value.parse::<u32>().ok())
    {
        (
            0x0000_2022,
            count,
            recurrence_minutes_since_1601(&event.date),
        )
    } else if let Some(until) = recurrence_rule_value(&parts, "UNTIL") {
        (
            0x0000_2021,
            0,
            recurrence_minutes_since_1601(&until_date(&until)),
        )
    } else {
        (0x0000_2023, 0, recurrence_minutes_since_1601(&event.date))
    };
    if end_type == 0x0000_2022 && count == 0 {
        bail!("unsupported canonical recurrence count");
    }
    Ok(CanonicalRecurrencePattern {
        frequency,
        pattern_type,
        first_date_time: recurrence_first_date_minutes(event, by_month, by_month_day),
        period,
        pattern_extra,
        end_type,
        count,
        end_date,
        deleted_dates: recurrence_deleted_dates_from_json(&event.recurrence_exceptions_json),
        modified_exceptions: recurrence_modified_exceptions_from_json(
            &event.recurrence_exceptions_json,
        ),
    })
}

fn parse_canonical_recurrence_rule(rule: &str) -> Vec<(String, String)> {
    rule.split(';')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((
                key.trim().to_ascii_uppercase(),
                value.trim().to_ascii_uppercase(),
            ))
        })
        .collect()
}

fn recurrence_rule_value(parts: &[(String, String)], key: &str) -> Option<String> {
    parts
        .iter()
        .find_map(|(candidate, value)| (candidate == key).then_some(value.clone()))
}

fn recurrence_first_date_minutes(
    event: &AccessibleEvent,
    by_month: Option<u32>,
    by_month_day: Option<u32>,
) -> u32 {
    if let Some(month) = by_month {
        let day = by_month_day.or_else(|| {
            event
                .date
                .get(8..10)
                .and_then(|value| value.parse::<u32>().ok())
        });
        let year = event
            .date
            .get(0..4)
            .and_then(|value| value.parse::<i32>().ok());
        if (1..=12).contains(&month)
            && day.is_some_and(|day| (1..=31).contains(&day))
            && year.is_some()
        {
            return recurrence_minutes_since_1601(&format!(
                "{:04}-{month:02}-{:02}",
                year.unwrap(),
                day.unwrap()
            ));
        }
    }
    recurrence_minutes_since_1601(&event.date)
}

fn recurrence_day_mask(value: &str) -> Result<u32> {
    let mut mask = 0u32;
    for day in value
        .split(',')
        .map(str::trim)
        .filter(|day| !day.is_empty())
    {
        mask |= match day {
            "SU" => 0x01,
            "MO" => 0x02,
            "TU" => 0x04,
            "WE" => 0x08,
            "TH" => 0x10,
            "FR" => 0x20,
            "SA" => 0x40,
            _ => bail!("unsupported canonical recurrence day"),
        };
    }
    if mask == 0 {
        bail!("unsupported canonical recurrence day");
    }
    Ok(mask)
}

fn recurrence_set_position_to_mapi(value: i32) -> Result<u32> {
    match value {
        1..=4 => Ok(value as u32),
        -1 => Ok(5),
        _ => bail!("unsupported canonical recurrence set position"),
    }
}

fn recurrence_deleted_dates_from_json(value: &str) -> Vec<u32> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter(|value| value.get("excluded").and_then(|value| value.as_bool()) == Some(true))
        .filter_map(|value| {
            value
                .get("recurrenceId")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .map(|date| recurrence_minutes_since_1601(&date))
        .collect()
}

fn recurrence_modified_exceptions_from_json(value: &str) -> Vec<CanonicalRecurrenceException> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter(|value| value.get("excluded").and_then(|value| value.as_bool()) != Some(true))
        .filter_map(|value| {
            let recurrence_id = value.get("recurrenceId")?.as_str()?;
            let start = value.get("start")?.as_str()?;
            let end = value.get("end")?.as_str()?;
            Some(CanonicalRecurrenceException {
                original_start: recurrence_minutes_since_1601(recurrence_id),
                start: recurrence_datetime_minutes_since_1601(start)?,
                end: recurrence_datetime_minutes_since_1601(end)?,
                title: value
                    .get("title")
                    .and_then(|value| value.as_str())
                    .map(str::to_string),
                location: value
                    .get("location")
                    .and_then(|value| value.as_str())
                    .map(str::to_string),
            })
        })
        .filter(|exception| exception.start < exception.end)
        .collect()
}

fn until_date(value: &str) -> String {
    if value.len() >= 8 && value.as_bytes()[0..8].iter().all(u8::is_ascii_digit) {
        format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8])
    } else {
        value.get(0..10).unwrap_or(value).to_string()
    }
}

fn event_start_minutes(event: &AccessibleEvent) -> u32 {
    time_to_minutes(&event.time)
}

fn event_end_minutes(event: &AccessibleEvent) -> u32 {
    event_start_minutes(event)
        .saturating_add(event.duration_minutes.max(1) as u32)
        .min(24 * 60)
}

fn time_to_minutes(time: &str) -> u32 {
    let hour = time
        .get(0..2)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0)
        .min(23);
    let minute = time
        .get(3..5)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0)
        .min(59);
    hour * 60 + minute
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MapiAppointmentRecurrence {
    pub(super) recurrence_rule: String,
    pub(super) recurrence_json: String,
    pub(super) recurrence_exceptions_json: String,
}

struct MapiRecurrenceException {
    original_start: u32,
    start: u32,
    end: u32,
    title: Option<String>,
    location: Option<String>,
    override_flags: u16,
}

pub(super) fn appointment_recurrence_from_mapi(value: &[u8]) -> Result<MapiAppointmentRecurrence> {
    let mut offset = 0usize;
    let reader_version = read_recur_u16(value, &mut offset)?;
    let writer_version = read_recur_u16(value, &mut offset)?;
    if reader_version != 0x3004 || writer_version != 0x3004 {
        bail!("unsupported MAPI calendar recurrence version");
    }
    let frequency = read_recur_u16(value, &mut offset)?;
    let pattern_type = read_recur_u16(value, &mut offset)?;
    let calendar_type = read_recur_u16(value, &mut offset)?;
    if calendar_type != 0 {
        bail!("unsupported MAPI calendar recurrence calendar type");
    }
    let first_date_time = read_recur_u32(value, &mut offset)?;
    let period = read_recur_u32(value, &mut offset)?;
    if period == 0 {
        bail!("unsupported MAPI calendar recurrence interval");
    }
    let sliding_flag = read_recur_u32(value, &mut offset)?;
    if sliding_flag != 0 {
        bail!("unsupported MAPI calendar recurrence sliding flag");
    }

    let pattern = read_recur_pattern(
        value,
        &mut offset,
        frequency,
        pattern_type,
        period,
        first_date_time,
    )?;
    let end_type = read_recur_u32(value, &mut offset)?;
    let occurrence_count = read_recur_u32(value, &mut offset)?;
    let _first_dow = read_recur_u32(value, &mut offset)?;
    let deleted = read_recur_dates(value, &mut offset)?;
    let modified = read_recur_dates(value, &mut offset)?;
    if modified.len() > deleted.len() {
        bail!("unsupported MAPI calendar recurrence modified instance list");
    }
    let _start_date = read_recur_u32(value, &mut offset)?;
    let end_date = read_recur_u32(value, &mut offset)?;
    let reader_version2 = read_recur_u32(value, &mut offset)?;
    let writer_version2 = read_recur_u32(value, &mut offset)?;
    if reader_version2 != 0x0000_3006 || !matches!(writer_version2, 0x0000_3008 | 0x0000_3009) {
        bail!("unsupported MAPI appointment recurrence version");
    }
    let _start_time_offset = read_recur_u32(value, &mut offset)?;
    let _end_time_offset = read_recur_u32(value, &mut offset)?;
    let exception_count = read_recur_u16(value, &mut offset)?;
    if usize::from(exception_count) != modified.len() {
        bail!("unsupported MAPI calendar recurrence exception payload");
    }
    let exceptions = read_recur_exception_infos(value, &mut offset, usize::from(exception_count))?;
    let reserved_block1_size = read_recur_u32(value, &mut offset)?;
    if reserved_block1_size != 0 {
        bail!("unsupported MAPI calendar recurrence reserved block");
    }
    read_recur_extended_exceptions(value, &mut offset, writer_version2, &exceptions)?;
    let reserved_block2_size = read_recur_u32(value, &mut offset)?;
    if reserved_block2_size != 0 {
        bail!("unsupported MAPI calendar recurrence reserved block");
    }

    let mut rule_parts = vec![format!("FREQ={}", pattern.frequency)];
    let mut json_parts = vec![format!(
        "\"frequency\":\"{}\"",
        pattern.frequency.to_ascii_lowercase()
    )];
    if pattern.interval != 1 {
        rule_parts.push(format!("INTERVAL={}", pattern.interval));
        json_parts.push(format!("\"interval\":{}", pattern.interval));
    }
    match end_type {
        0x0000_2022 => {
            if occurrence_count == 0 {
                bail!("unsupported MAPI calendar recurrence count");
            }
            rule_parts.push(format!("COUNT={occurrence_count}"));
            json_parts.push(format!("\"count\":{occurrence_count}"));
        }
        0x0000_2021 => {
            let until = recurrence_date_yyyymmdd(end_date)?;
            rule_parts.push(format!("UNTIL={until}"));
            json_parts.push(format!("\"until\":\"{until}\""));
        }
        0x0000_2023 | 0xFFFF_FFFF => {}
        _ => bail!("unsupported MAPI calendar recurrence end type"),
    }
    if !pattern.by_day.is_empty() {
        rule_parts.push(format!("BYDAY={}", pattern.by_day.join(",")));
        json_parts.push(format!(
            "\"byDay\":[{}]",
            pattern
                .by_day
                .iter()
                .map(|day| format!("\"{day}\""))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(day) = pattern.by_month_day {
        rule_parts.push(format!("BYMONTHDAY={day}"));
        json_parts.push(format!("\"byMonthDay\":{day}"));
    }
    if let Some(month) = pattern.by_month {
        rule_parts.push(format!("BYMONTH={month}"));
        json_parts.push(format!("\"byMonth\":{month}"));
    }
    if let Some(position) = pattern.by_set_pos {
        rule_parts.push(format!("BYSETPOS={position}"));
        json_parts.push(format!("\"bySetPosition\":{position}"));
    }

    let modified_set = modified.iter().copied().collect::<HashSet<_>>();
    let mut overrides = deleted
        .into_iter()
        .filter(|date| !modified_set.contains(date))
        .map(|date| {
            recurrence_date_string(date)
                .map(|date| format!(r#"{{"recurrenceId":"{date}","excluded":true}}"#))
        })
        .collect::<Result<Vec<_>>>()?;
    for exception in exceptions {
        let recurrence_id = recurrence_date_string(exception.original_start)?;
        let start = recurrence_datetime_string(exception.start)?;
        let end = recurrence_datetime_string(exception.end)?;
        let mut override_value = serde_json::json!({
            "recurrenceId": recurrence_id,
            "start": start,
            "end": end,
        });
        if let Some(title) = exception.title {
            override_value["title"] = serde_json::Value::String(title);
        }
        if let Some(location) = exception.location {
            override_value["location"] = serde_json::Value::String(location);
        }
        overrides.push(override_value.to_string());
    }

    Ok(MapiAppointmentRecurrence {
        recurrence_rule: rule_parts.join(";"),
        recurrence_json: format!("{{{}}}", json_parts.join(",")),
        recurrence_exceptions_json: format!("[{}]", overrides.join(",")),
    })
}

struct MapiRecurPattern {
    frequency: &'static str,
    interval: u32,
    by_day: Vec<&'static str>,
    by_month: Option<u32>,
    by_month_day: Option<u32>,
    by_set_pos: Option<i32>,
}

fn read_recur_pattern(
    value: &[u8],
    offset: &mut usize,
    frequency: u16,
    pattern_type: u16,
    period: u32,
    first_date_time: u32,
) -> Result<MapiRecurPattern> {
    match (frequency, pattern_type) {
        (0x200A, 0x0000) => Ok(MapiRecurPattern {
            frequency: "DAILY",
            interval: (period / 1440).max(1),
            by_day: Vec::new(),
            by_month: None,
            by_month_day: None,
            by_set_pos: None,
        }),
        (0x200B, 0x0001) => {
            let mask = read_recur_u32(value, offset)?;
            Ok(MapiRecurPattern {
                frequency: "WEEKLY",
                interval: period,
                by_day: recurrence_days_from_mask(mask)?,
                by_month: None,
                by_month_day: None,
                by_set_pos: None,
            })
        }
        (0x200C, 0x0002) => {
            let day = read_recur_u32(value, offset)?;
            if !(1..=31).contains(&day) {
                bail!("unsupported MAPI monthly recurrence day");
            }
            Ok(MapiRecurPattern {
                frequency: "MONTHLY",
                interval: period,
                by_day: Vec::new(),
                by_month: None,
                by_month_day: Some(day),
                by_set_pos: None,
            })
        }
        (0x200C, 0x0004) => {
            let day = read_recur_u32(value, offset)?;
            if day != 31 {
                bail!("unsupported MAPI month-end recurrence day");
            }
            Ok(MapiRecurPattern {
                frequency: "MONTHLY",
                interval: period,
                by_day: Vec::new(),
                by_month: None,
                by_month_day: Some(31),
                by_set_pos: None,
            })
        }
        (0x200C, 0x0003) | (0x200D, 0x0003) => {
            let mask = read_recur_u32(value, offset)?;
            let n = read_recur_u32(value, offset)?;
            let set_pos = match n {
                1..=4 => n as i32,
                5 => -1,
                _ => bail!("unsupported MAPI monthly nth recurrence position"),
            };
            Ok(MapiRecurPattern {
                frequency: if frequency == 0x200D {
                    "YEARLY"
                } else {
                    "MONTHLY"
                },
                interval: if frequency == 0x200D { 1 } else { period },
                by_day: recurrence_days_from_mask(mask)?,
                by_month: (frequency == 0x200D)
                    .then(|| recurrence_month_from_minutes(first_date_time))
                    .transpose()?,
                by_month_day: None,
                by_set_pos: Some(set_pos),
            })
        }
        (0x200D, 0x0002) => {
            let day = read_recur_u32(value, offset)?;
            if period != 12 || !(1..=31).contains(&day) {
                bail!("unsupported MAPI yearly recurrence");
            }
            Ok(MapiRecurPattern {
                frequency: "YEARLY",
                interval: 1,
                by_day: Vec::new(),
                by_month: Some(recurrence_month_from_minutes(first_date_time)?),
                by_month_day: Some(day),
                by_set_pos: None,
            })
        }
        _ => bail!("unsupported MAPI calendar recurrence pattern"),
    }
}

fn recurrence_days_from_mask(mask: u32) -> Result<Vec<&'static str>> {
    let days = [
        (0x01, "SU"),
        (0x02, "MO"),
        (0x04, "TU"),
        (0x08, "WE"),
        (0x10, "TH"),
        (0x20, "FR"),
        (0x40, "SA"),
    ]
    .into_iter()
    .filter_map(|(bit, day)| (mask & bit != 0).then_some(day))
    .collect::<Vec<_>>();
    if days.is_empty() || mask & !0x7F != 0 {
        bail!("unsupported MAPI recurrence day mask");
    }
    Ok(days)
}

fn read_recur_dates(value: &[u8], offset: &mut usize) -> Result<Vec<u32>> {
    let count = read_recur_u32(value, offset)? as usize;
    let mut dates = Vec::with_capacity(count);
    for _ in 0..count {
        dates.push(read_recur_u32(value, offset)?);
    }
    Ok(dates)
}

fn read_recur_exception_infos(
    value: &[u8],
    offset: &mut usize,
    count: usize,
) -> Result<Vec<MapiRecurrenceException>> {
    let mut exceptions = Vec::with_capacity(count);
    for _ in 0..count {
        let start = read_recur_u32(value, offset)?;
        let end = read_recur_u32(value, offset)?;
        let original_start = read_recur_u32(value, offset)?;
        let override_flags = read_recur_u16(value, offset)?;
        if start >= end {
            bail!("unsupported MAPI calendar recurrence exception time range");
        }
        if override_flags & !0x0011 != 0 {
            bail!("unsupported MAPI calendar recurrence exception override");
        }
        let title = if override_flags & 0x0001 != 0 {
            Some(read_recur_ansi_string(value, offset)?)
        } else {
            None
        };
        let location = if override_flags & 0x0010 != 0 {
            Some(read_recur_ansi_string(value, offset)?)
        } else {
            None
        };
        exceptions.push(MapiRecurrenceException {
            original_start,
            start,
            end,
            title,
            location,
            override_flags,
        });
    }
    Ok(exceptions)
}

fn read_recur_extended_exceptions(
    value: &[u8],
    offset: &mut usize,
    writer_version2: u32,
    exceptions: &[MapiRecurrenceException],
) -> Result<()> {
    if writer_version2 < 0x0000_3009 {
        return Ok(());
    }
    for exception in exceptions {
        let change_highlight_size = read_recur_u32(value, offset)? as usize;
        skip_recur_bytes(value, offset, change_highlight_size)?;
        let reserved_block_ee1_size = read_recur_u32(value, offset)?;
        if reserved_block_ee1_size != 0 {
            bail!("unsupported MAPI calendar recurrence extended exception reserved block");
        }
        if exception.override_flags & 0x0011 != 0 {
            let extended_start = read_recur_u32(value, offset)?;
            let extended_end = read_recur_u32(value, offset)?;
            let extended_original = read_recur_u32(value, offset)?;
            if extended_start != exception.start
                || extended_end != exception.end
                || extended_original != exception.original_start
            {
                bail!("unsupported MAPI calendar recurrence extended exception mismatch");
            }
            if exception.override_flags & 0x0001 != 0 {
                let _ = read_recur_wide_string(value, offset)?;
            }
            if exception.override_flags & 0x0010 != 0 {
                let _ = read_recur_wide_string(value, offset)?;
            }
            let reserved_block_ee2_size = read_recur_u32(value, offset)?;
            if reserved_block_ee2_size != 0 {
                bail!("unsupported MAPI calendar recurrence extended exception reserved block");
            }
        }
    }
    Ok(())
}

fn read_recur_ansi_string(value: &[u8], offset: &mut usize) -> Result<String> {
    let length_with_nul = read_recur_u16(value, offset)?;
    let length = read_recur_u16(value, offset)? as usize;
    if usize::from(length_with_nul) < length {
        bail!("unsupported MAPI calendar recurrence exception string length");
    }
    let bytes = value
        .get(*offset..offset.saturating_add(length))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += length;
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

fn read_recur_wide_string(value: &[u8], offset: &mut usize) -> Result<String> {
    let length = read_recur_u16(value, offset)? as usize;
    let byte_len = length.saturating_mul(2);
    let bytes = value
        .get(*offset..offset.saturating_add(byte_len))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += byte_len;
    let units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>();
    Ok(String::from_utf16_lossy(&units))
}

pub(super) fn append_recur_ansi_string(value: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    value.extend_from_slice(&((bytes.len() + 1) as u16).to_le_bytes());
    value.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    value.extend_from_slice(bytes);
}

pub(super) fn append_recur_wide_string(value: &mut Vec<u8>, text: &str) {
    let units = text.encode_utf16().collect::<Vec<_>>();
    value.extend_from_slice(&(units.len() as u16).to_le_bytes());
    for unit in units {
        value.extend_from_slice(&unit.to_le_bytes());
    }
}

fn skip_recur_bytes(value: &[u8], offset: &mut usize, len: usize) -> Result<()> {
    value
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += len;
    Ok(())
}

fn read_recur_u16(value: &[u8], offset: &mut usize) -> Result<u16> {
    let bytes = value
        .get(*offset..offset.saturating_add(2))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += 2;
    Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_recur_u32(value: &[u8], offset: &mut usize) -> Result<u32> {
    let bytes = value
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| anyhow!("truncated MAPI calendar recurrence"))?;
    *offset += 4;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

pub(super) fn recurrence_minutes_since_1601(date: &str) -> u32 {
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
    let days = days_from_civil(i64::from(year), i64::from(month), i64::from(day))
        - days_from_civil(1601, 1, 1);
    days.max(0).saturating_mul(1440).min(i64::from(u32::MAX)) as u32
}

fn recurrence_datetime_minutes_since_1601(value: &str) -> Option<u32> {
    let date_minutes = recurrence_minutes_since_1601(value);
    let hour = value.get(11..13)?.parse::<u32>().ok()?.min(23);
    let minute = value.get(14..16)?.parse::<u32>().ok()?.min(59);
    Some(date_minutes.saturating_add(hour * 60 + minute))
}

fn recurrence_date_yyyymmdd(minutes_since_1601: u32) -> Result<String> {
    let date = recurrence_date_string(minutes_since_1601)?;
    Ok(date.replace('-', ""))
}

pub(super) fn recurrence_date_string(minutes_since_1601: u32) -> Result<String> {
    let unix_days =
        days_from_civil(1601, 1, 1).saturating_add(i64::from(minutes_since_1601 / 1440));
    let (year, month, day) = civil_from_days(unix_days);
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn recurrence_month_from_minutes(minutes_since_1601: u32) -> Result<u32> {
    let unix_days =
        days_from_civil(1601, 1, 1).saturating_add(i64::from(minutes_since_1601 / 1440));
    let (_, month, _) = civil_from_days(unix_days);
    if (1..=12).contains(&month) {
        Ok(month as u32)
    } else {
        bail!("unsupported MAPI yearly recurrence month")
    }
}

fn recurrence_datetime_string(minutes_since_1601: u32) -> Result<String> {
    let date = recurrence_date_string(minutes_since_1601)?;
    let minutes = minutes_since_1601 % 1440;
    Ok(format!("{date}T{:02}:{:02}:00", minutes / 60, minutes % 60))
}
