use std::collections::{HashMap, HashSet};

use chrono::{Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc, Weekday};

use super::{icalendar_parameter, icalendar_property};

#[derive(Debug, Clone)]
pub(super) struct IcalendarTimezone {
    fixed_offset: Option<i32>,
    observances: Vec<IcalendarTimezoneObservance>,
}

#[derive(Debug, Clone)]
struct IcalendarTimezoneObservance {
    starts_at: NaiveDateTime,
    offset_from: i32,
    offset_to: i32,
    recurrence: IcalendarTimezoneRecurrence,
}

#[derive(Debug, Clone, Copy)]
struct IcalendarTimezoneRecurrence {
    month: u32,
    day: IcalendarTimezoneRecurrenceDay,
}

#[derive(Debug, Clone, Copy)]
enum IcalendarTimezoneRecurrenceDay {
    MonthDay(i32),
    Weekday { occurrence: i32, weekday: Weekday },
}

#[derive(Debug, Clone, Copy)]
struct IcalendarTimezoneTransition {
    starts_at: NaiveDateTime,
    offset_from: i32,
    offset_to: i32,
}

// [MS-OXCICAL] section 2.1.3.1.1.19 requires matching VTIMEZONE
// components to resolve local DATE-TIME values carrying a TZID parameter.
pub(super) fn icalendar_timezones(lines: &[String]) -> HashMap<String, IcalendarTimezone> {
    let mut timezones = HashMap::new();
    let mut seen_tzids = HashSet::new();
    for component in icalendar_components(lines, "VTIMEZONE") {
        let Some(tzid) = single_icalendar_value(component, "TZID")
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if !seen_tzids.insert(tzid.clone()) {
            timezones.remove(&tzid);
            continue;
        }
        let standard = icalendar_components(component, "STANDARD");
        let daylight = icalendar_components(component, "DAYLIGHT");
        if standard.len() != 1 || daylight.len() > 1 {
            continue;
        }
        if daylight.is_empty() {
            if ["DTSTART", "TZOFFSETFROM", "TZOFFSETTO", "RRULE", "RDATE"]
                .iter()
                .any(|name| optional_single_icalendar_value(standard[0], name).is_none())
            {
                continue;
            }
            let Some(fixed_offset) = single_icalendar_value(standard[0], "TZOFFSETTO")
                .and_then(|value| parse_icalendar_offset(&value))
            else {
                continue;
            };
            timezones.insert(
                tzid,
                IcalendarTimezone {
                    fixed_offset: Some(fixed_offset),
                    observances: Vec::new(),
                },
            );
            continue;
        }
        let components = standard.into_iter().chain(daylight).collect::<Vec<_>>();
        let timezone = match components
            .iter()
            .copied()
            .map(parse_icalendar_timezone_observance)
            .collect::<Option<Vec<_>>>()
        {
            Some(mut observances) => {
                // [MS-OXCICAL] sections 2.1.3.1.1.19.2.4 and .19.3.4
                // direct importers to ignore TZOFFSETFROM. Each transition
                // starts from the other observance's authoritative offset.
                let standard_offset = observances[0].offset_to;
                let daylight_offset = observances[1].offset_to;
                observances[0].offset_from = daylight_offset;
                observances[1].offset_from = standard_offset;
                IcalendarTimezone {
                    fixed_offset: None,
                    observances,
                }
            }
            None => {
                let Some(fixed_offset) = simplified_fixed_timezone_offset(&components) else {
                    continue;
                };
                IcalendarTimezone {
                    fixed_offset: Some(fixed_offset),
                    observances: Vec::new(),
                }
            }
        };
        timezones.insert(tzid, timezone);
    }
    timezones
}

fn optional_single_icalendar_value(lines: &[String], name: &str) -> Option<Option<String>> {
    let mut values = lines.iter().filter_map(|line| {
        icalendar_property(line, name).map(|(_, value)| value.trim().to_string())
    });
    let value = values.next();
    values.next().is_none().then_some(value)
}

fn single_icalendar_value(lines: &[String], name: &str) -> Option<String> {
    optional_single_icalendar_value(lines, name)?.filter(|value| !value.is_empty())
}

fn simplified_fixed_timezone_offset(components: &[&[String]]) -> Option<i32> {
    let mut fixed_offset = None;
    for component in components {
        for name in ["DTSTART", "TZOFFSETFROM", "TZOFFSETTO", "RRULE", "RDATE"] {
            optional_single_icalendar_value(component, name)?;
        }
        if optional_single_icalendar_value(component, "RRULE")?.is_some()
            || optional_single_icalendar_value(component, "RDATE")?.is_some()
        {
            return None;
        }
        if let Some(value) = optional_single_icalendar_value(component, "DTSTART")? {
            parse_icalendar_naive_datetime(&value)?;
        }
        let offset_to = parse_icalendar_offset(&single_icalendar_value(component, "TZOFFSETTO")?)?;
        if let Some(value) = optional_single_icalendar_value(component, "TZOFFSETFROM")? {
            if parse_icalendar_offset(&value)? != offset_to {
                return None;
            }
        }
        if fixed_offset
            .replace(offset_to)
            .is_some_and(|offset| offset != offset_to)
        {
            return None;
        }
    }
    fixed_offset
}

fn icalendar_components<'a>(lines: &'a [String], name: &str) -> Vec<&'a [String]> {
    let begin = format!("BEGIN:{name}");
    let end = format!("END:{name}");
    let mut components = Vec::new();
    let mut start = None;
    for (index, line) in lines.iter().enumerate() {
        if line.eq_ignore_ascii_case(&begin) {
            start = Some(index + 1);
        } else if line.eq_ignore_ascii_case(&end) {
            if let Some(start) = start.take() {
                components.push(&lines[start..index]);
            }
        }
    }
    components
}

fn parse_icalendar_timezone_observance(lines: &[String]) -> Option<IcalendarTimezoneObservance> {
    if optional_single_icalendar_value(lines, "RDATE")?.is_some() {
        return None;
    }
    let starts_at = parse_icalendar_naive_datetime(&single_icalendar_value(lines, "DTSTART")?)?;
    let offset_from = parse_icalendar_offset(&single_icalendar_value(lines, "TZOFFSETFROM")?)?;
    let offset_to = parse_icalendar_offset(&single_icalendar_value(lines, "TZOFFSETTO")?)?;
    let recurrence = match optional_single_icalendar_value(lines, "RRULE")? {
        Some(value) => parse_icalendar_timezone_recurrence(&value, starts_at)?,
        // [MS-OXCICAL] sections 2.1.3.1.1.19.2.1 and .19.3.1 define
        // the preferred recurring SYSTEMTIME projection derived from DTSTART.
        None => icalendar_timezone_recurrence_from_start(starts_at)?,
    };
    Some(IcalendarTimezoneObservance {
        starts_at,
        offset_from,
        offset_to,
        recurrence,
    })
}

fn icalendar_timezone_recurrence_from_start(
    starts_at: NaiveDateTime,
) -> Option<IcalendarTimezoneRecurrence> {
    let date = starts_at.date();
    let next_week = date.checked_add_signed(Duration::days(7))?;
    let occurrence = if next_week.month() != date.month() {
        -1
    } else {
        i32::try_from((date.day() - 1) / 7 + 1).ok()?
    };
    Some(IcalendarTimezoneRecurrence {
        month: date.month(),
        day: IcalendarTimezoneRecurrenceDay::Weekday {
            occurrence,
            weekday: date.weekday(),
        },
    })
}

// [MS-OXCICAL] sections 2.1.3.1.1.19.2.2 and 2.1.3.1.1.19.3.2
// constrain Microsoft STANDARD/DAYLIGHT rules to yearly BYDAY or BYMONTHDAY
// transitions. Unsupported recurrence shapes remain non-actionable.
fn parse_icalendar_timezone_recurrence(
    value: &str,
    starts_at: NaiveDateTime,
) -> Option<IcalendarTimezoneRecurrence> {
    let mut frequency = None;
    let mut interval = None;
    let mut month = None;
    let mut by_day = None;
    let mut by_month_day = None;
    let mut seen_parts = HashSet::new();
    for part in value.split(';') {
        let (name, value) = part.split_once('=')?;
        let name = name.trim().to_ascii_uppercase();
        if !seen_parts.insert(name.clone()) {
            return None;
        }
        match name.as_str() {
            "FREQ" => frequency = Some(value.trim().to_ascii_uppercase()),
            "INTERVAL" => interval = Some(value.trim().parse::<u32>().ok()?),
            "BYMONTH" => month = Some(value.trim().parse::<u32>().ok()?),
            "BYDAY" => by_day = Some(value.trim().to_ascii_uppercase()),
            "BYMONTHDAY" => by_month_day = Some(value.trim().parse::<i32>().ok()?),
            "WKST" => {
                weekday_from_icalendar(value.trim())?;
            }
            _ => return None,
        }
    }
    if frequency.as_deref() != Some("YEARLY") || interval.is_some_and(|value| value != 1) {
        return None;
    }
    let month = month.unwrap_or_else(|| starts_at.month());
    if !(1..=12).contains(&month) || by_day.is_some() == by_month_day.is_some() {
        return None;
    }
    let day = if let Some(value) = by_day {
        if value.len() < 3 || value.contains(',') {
            return None;
        }
        let (occurrence, weekday) = value.split_at(value.len() - 2);
        let occurrence = occurrence.parse::<i32>().ok()?;
        if !matches!(occurrence, -1 | 1..=4) {
            return None;
        }
        IcalendarTimezoneRecurrenceDay::Weekday {
            occurrence,
            weekday: weekday_from_icalendar(weekday)?,
        }
    } else {
        let day = by_month_day?;
        if !matches!(day, -1 | 1..=31) {
            return None;
        }
        IcalendarTimezoneRecurrenceDay::MonthDay(day)
    };
    Some(IcalendarTimezoneRecurrence { month, day })
}

fn weekday_from_icalendar(value: &str) -> Option<Weekday> {
    match value.to_ascii_uppercase().as_str() {
        "MO" => Some(Weekday::Mon),
        "TU" => Some(Weekday::Tue),
        "WE" => Some(Weekday::Wed),
        "TH" => Some(Weekday::Thu),
        "FR" => Some(Weekday::Fri),
        "SA" => Some(Weekday::Sat),
        "SU" => Some(Weekday::Sun),
        _ => None,
    }
}

impl IcalendarTimezone {
    fn offset_at(&self, local: NaiveDateTime) -> Option<i32> {
        if let Some(offset) = self.fixed_offset {
            return Some(offset);
        }
        let previous_year = local.year().checked_sub(1)?;
        let mut transitions = Vec::new();
        for observance in &self.observances {
            for year in [previous_year, local.year()] {
                if let Some(starts_at) = observance.transition_for_year(year) {
                    transitions.push(IcalendarTimezoneTransition {
                        starts_at,
                        offset_from: observance.offset_from,
                        offset_to: observance.offset_to,
                    });
                }
            }
        }
        for transition in &transitions {
            let offset_change = transition.offset_to - transition.offset_from;
            let shifted = transition
                .starts_at
                .checked_add_signed(Duration::seconds(i64::from(offset_change)))?;
            if (offset_change > 0 && local >= transition.starts_at && local < shifted)
                || (offset_change < 0 && local >= shifted && local < transition.starts_at)
            {
                return None;
            }
        }
        transitions
            .iter()
            .filter(|transition| transition.starts_at <= local)
            .max_by_key(|transition| transition.starts_at)
            .map(|transition| transition.offset_to)
            .or_else(|| {
                transitions
                    .iter()
                    .min_by_key(|transition| transition.starts_at)
                    .map(|transition| transition.offset_from)
            })
    }
}

impl IcalendarTimezoneObservance {
    fn transition_for_year(&self, year: i32) -> Option<NaiveDateTime> {
        if year < self.starts_at.year() {
            return None;
        }
        let date = self.recurrence.date_for_year(year)?;
        let transition = date.and_time(self.starts_at.time());
        (transition >= self.starts_at).then_some(transition)
    }
}

impl IcalendarTimezoneRecurrence {
    fn date_for_year(&self, year: i32) -> Option<NaiveDate> {
        let days_in_month = icalendar_days_in_month(year, self.month)?;
        let day = match self.day {
            IcalendarTimezoneRecurrenceDay::MonthDay(day) if day > 0 => day,
            IcalendarTimezoneRecurrenceDay::MonthDay(day) => {
                i32::try_from(days_in_month).ok()? + day + 1
            }
            IcalendarTimezoneRecurrenceDay::Weekday {
                occurrence,
                weekday,
            } if occurrence > 0 => {
                let first = NaiveDate::from_ymd_opt(year, self.month, 1)?;
                let weekday_delta = (7 + i32::try_from(weekday.num_days_from_monday()).ok()?
                    - i32::try_from(first.weekday().num_days_from_monday()).ok()?)
                    % 7;
                1 + weekday_delta + 7 * (occurrence - 1)
            }
            IcalendarTimezoneRecurrenceDay::Weekday {
                occurrence,
                weekday,
            } => {
                let last = NaiveDate::from_ymd_opt(year, self.month, days_in_month)?;
                let weekday_delta = (7 + i32::try_from(last.weekday().num_days_from_monday())
                    .ok()?
                    - i32::try_from(weekday.num_days_from_monday()).ok()?)
                    % 7;
                let occurrence_from_end = -occurrence;
                i32::try_from(days_in_month).ok()? - weekday_delta - 7 * (occurrence_from_end - 1)
            }
        };
        NaiveDate::from_ymd_opt(year, self.month, u32::try_from(day).ok()?)
    }
}

fn icalendar_days_in_month(year: i32, month: u32) -> Option<u32> {
    let (next_year, next_month) = if month == 12 {
        (year.checked_add(1)?, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)?
        .pred_opt()
        .map(|date| date.day())
}

fn parse_icalendar_offset(value: &str) -> Option<i32> {
    let bytes = value.trim().as_bytes();
    if !matches!(bytes.len(), 5 | 7)
        || !matches!(bytes[0], b'+' | b'-')
        || !bytes[1..].iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let hours = i32::from((bytes[1] - b'0') * 10 + bytes[2] - b'0');
    let minutes = i32::from((bytes[3] - b'0') * 10 + bytes[4] - b'0');
    let seconds = if bytes.len() == 7 {
        i32::from((bytes[5] - b'0') * 10 + bytes[6] - b'0')
    } else {
        0
    };
    (hours <= 23 && minutes <= 59 && seconds <= 59)
        .then_some((hours * 3_600 + minutes * 60 + seconds) * if bytes[0] == b'-' { -1 } else { 1 })
}

fn parse_icalendar_naive_datetime(value: &str) -> Option<NaiveDateTime> {
    let value = value.trim();
    if value.len() != 15
        || value.as_bytes().get(8) != Some(&b'T')
        || !value
            .as_bytes()
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 8 || byte.is_ascii_digit())
    {
        return None;
    }
    let parse = |start, end| value.get(start..end)?.parse::<u32>().ok();
    NaiveDate::from_ymd_opt(parse(0, 4)? as i32, parse(4, 6)?, parse(6, 8)?)?.and_hms_opt(
        parse(9, 11)?,
        parse(11, 13)?,
        parse(13, 15)?,
    )
}

pub(super) fn parse_icalendar_datetime(
    parameters: &str,
    value: &str,
    timezones: &HashMap<String, IcalendarTimezone>,
) -> Option<String> {
    let value = value.trim();
    let utc = value.ends_with('Z');
    let value = value.strip_suffix('Z').unwrap_or(value);
    let date_time = parse_icalendar_naive_datetime(value)?;
    let offset = if utc {
        0
    } else {
        let tzid = icalendar_parameter(parameters, "TZID")?.to_ascii_lowercase();
        timezones.get(&tzid)?.offset_at(date_time)?
    };
    Some(
        FixedOffset::east_opt(offset)?
            .from_local_datetime(&date_time)
            .single()?
            .with_timezone(&Utc)
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn microsoft_yearly_transition_rules_resolve_bounded_occurrences() {
        let starts_at = parse_icalendar_naive_datetime("16010101T020000").unwrap();
        let last_sunday =
            parse_icalendar_timezone_recurrence("FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU", starts_at)
                .unwrap();
        assert_eq!(
            last_sunday.date_for_year(2026),
            NaiveDate::from_ymd_opt(2026, 10, 25)
        );

        let second_sunday = parse_icalendar_timezone_recurrence(
            "FREQ=YEARLY;WKST=MO;INTERVAL=1;BYMONTH=3;BYDAY=2SU",
            starts_at,
        )
        .unwrap();
        assert_eq!(
            second_sunday.date_for_year(2026),
            NaiveDate::from_ymd_opt(2026, 3, 8)
        );
        assert!(parse_icalendar_timezone_recurrence(
            "FREQ=YEARLY;INTERVAL=2;BYMONTH=3;BYDAY=2SU",
            starts_at,
        )
        .is_none());
        assert!(
            parse_icalendar_timezone_recurrence("FREQ=YEARLY;BYMONTH=3;BYDAY=5SU", starts_at,)
                .is_none()
        );
        assert!(
            parse_icalendar_timezone_recurrence("FREQ=YEARLY;BYMONTH=3;BYDAY=-2SU", starts_at,)
                .is_none()
        );
        assert!(parse_icalendar_timezone_recurrence(
            "FREQ=YEARLY;BYMONTH=3;BYMONTHDAY=-2",
            starts_at,
        )
        .is_none());
        assert!(parse_icalendar_timezone_recurrence(
            "FREQ=DAILY;FREQ=YEARLY;BYMONTH=3;BYDAY=2SU",
            starts_at,
        )
        .is_none());
    }

    #[test]
    fn fixed_timezone_and_unique_tzid_rules_fail_closed() {
        let fixed = concat!(
            "BEGIN:VTIMEZONE\r\n",
            "TZID:Fixed Test Time\r\n",
            "BEGIN:STANDARD\r\n",
            "DTSTART:16010101T030000\r\n",
            "TZOFFSETFROM:+0200\r\n",
            "TZOFFSETTO:+0100\r\n",
            "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU\r\n",
            "END:STANDARD\r\n",
            "END:VTIMEZONE\r\n",
        )
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let timezones = icalendar_timezones(&fixed);
        assert_eq!(
            parse_icalendar_datetime("TZID=Fixed Test Time", "20260701T083000", &timezones),
            Some("2026-07-01T07:30:00Z".to_string())
        );

        let duplicate = concat!(
            "BEGIN:VTIMEZONE\r\n",
            "TZID:Duplicate Time\r\n",
            "BEGIN:STANDARD\r\n",
            "TZOFFSETTO:+0100\r\n",
            "END:STANDARD\r\n",
            "END:VTIMEZONE\r\n",
            "BEGIN:VTIMEZONE\r\n",
            "TZID:duplicate time\r\n",
            "BEGIN:STANDARD\r\n",
            "TZOFFSETTO:+0200\r\n",
            "END:STANDARD\r\n",
            "END:VTIMEZONE\r\n",
        )
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
        assert!(!icalendar_timezones(&duplicate).contains_key("duplicate time"));
    }

    #[test]
    fn dtstart_derived_transitions_ignore_declared_offset_from() {
        let lines = concat!(
            "BEGIN:VTIMEZONE\r\n",
            "TZID:Derived Test Time\r\n",
            "BEGIN:STANDARD\r\n",
            "DTSTART:20251026T030000\r\n",
            "TZOFFSETFROM:+0900\r\n",
            "TZOFFSETTO:+0100\r\n",
            "END:STANDARD\r\n",
            "BEGIN:DAYLIGHT\r\n",
            "DTSTART:20250330T020000\r\n",
            "TZOFFSETFROM:-0700\r\n",
            "TZOFFSETTO:+0200\r\n",
            "END:DAYLIGHT\r\n",
            "END:VTIMEZONE\r\n",
        )
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let timezones = icalendar_timezones(&lines);
        assert_eq!(
            parse_icalendar_datetime("TZID=Derived Test Time", "20260701T083000", &timezones),
            Some("2026-07-01T06:30:00Z".to_string())
        );
        assert_eq!(
            parse_icalendar_datetime("TZID=Derived Test Time", "20260329T033000", &timezones),
            Some("2026-03-29T01:30:00Z".to_string())
        );
        assert_eq!(
            parse_icalendar_datetime("TZID=Derived Test Time", "20260101T083000", &timezones),
            Some("2026-01-01T07:30:00Z".to_string())
        );
    }
}
