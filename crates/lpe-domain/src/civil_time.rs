#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UtcDateTime {
    pub year: i64,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub unix_days: i64,
}

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_position = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_position + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

pub fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let days = days_since_epoch + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = month_position + if month_position < 10 { 3 } else { -9 };
    (year + i64::from(month <= 2), month, day)
}

pub fn utc_from_unix_seconds(total_seconds: u64) -> UtcDateTime {
    let unix_days = (total_seconds / 86_400) as i64;
    let seconds_of_day = total_seconds % 86_400;
    let (year, month, day) = civil_from_days(unix_days);
    UtcDateTime {
        year,
        month: month as u8,
        day: day as u8,
        hour: (seconds_of_day / 3_600) as u8,
        minute: ((seconds_of_day % 3_600) / 60) as u8,
        second: (seconds_of_day % 60) as u8,
        unix_days,
    }
}

pub fn weekday_abbrev_from_unix_days(days_since_epoch: i64) -> &'static str {
    const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    WEEKDAYS[days_since_epoch.rem_euclid(7) as usize]
}

pub fn month_abbrev(month: u8) -> Option<&'static str> {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    if month == 0 {
        return None;
    }
    MONTHS.get(usize::from(month - 1)).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_round_trip_handles_epoch_and_leap_day() {
        for (year, month, day, days) in [
            (1970, 1, 1, 0),
            (2000, 2, 29, 11_016),
            (2026, 6, 27, 20_631),
            (1601, 1, 1, -134_774),
        ] {
            assert_eq!(days_from_civil(year, month, day), days);
            assert_eq!(civil_from_days(days), (year, month, day));
        }
    }

    #[test]
    fn utc_parts_include_weekday_and_month_names() {
        let date = utc_from_unix_seconds(1_780_144_640);
        assert_eq!(date.year, 2026);
        assert_eq!(date.month, 5);
        assert_eq!(date.day, 30);
        assert_eq!(date.hour, 12);
        assert_eq!(date.minute, 37);
        assert_eq!(date.second, 20);
        assert_eq!(weekday_abbrev_from_unix_days(date.unix_days), "Sat");
        assert_eq!(month_abbrev(date.month), Some("May"));
    }
}
