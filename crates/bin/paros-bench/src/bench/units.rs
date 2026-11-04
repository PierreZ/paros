//! The bench's quantities on the command line (#724 (bench roles), #725
//! (bench phases)): durations (`500ms`, `10s`, `2m`), sizes (`64`, `4KiB`,
//! `10GiB`, `1MB`) and rates (calls per second, bytes per second).

use std::time::Duration;

/// A duration as `500ms`, `10s` or `2m`.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let (digits, unit) = text
        .find(|c: char| !c.is_ascii_digit())
        .map_or((text, ""), |at| text.split_at(at));
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("{text:?}: a duration is a number and a unit (ms, s, m)"))?;
    match unit {
        "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => Ok(Duration::from_secs(value * 60)),
        _ => Err(format!("{text:?}: the unit is ms, s or m")),
    }
}

/// A time as a number of nanoseconds: `200us`, `5ms`, `1.5s` (the unit is
/// needed). For `--expect` limits.
pub fn parse_time_nanos(text: &str) -> Result<f64, String> {
    let (number, unit) = split_number(text);
    let value = number_of(text, number)?;
    let scale = match unit {
        "ns" => 1.0,
        "us" => 1e3,
        "ms" => 1e6,
        "s" => 1e9,
        _ => return Err(format!("{text:?}: a time takes a unit (ns, us, ms, s)")),
    };
    Ok(value * scale)
}

/// A size in bytes: `64`, `64B`, `4KiB`, `10GiB`, `1MB` (`K`, `M`, `G`
/// are powers of 1000; `Ki`, `Mi`, `Gi` powers of 1024).
pub fn parse_size(text: &str) -> Result<u64, String> {
    let (number, unit) = split_number(text);
    let value = number_of(text, number)?;
    let scale = scale_of(unit).ok_or_else(|| {
        format!("{text:?}: a size is a number and B, KB, KiB, MB, MiB, GB or GiB")
    })?;
    let bytes = value * scale;
    if bytes < 0.0 || bytes > 2f64.powi(63) {
        return Err(format!("{text:?}: out of range"));
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "checked to be in 0..2^63 above"
    )]
    Ok(bytes.round() as u64)
}

/// A rate in bytes per second: `100MiB/s`, `100MiB` (the `/s` is
/// optional), above zero.
pub fn parse_bytes_per_sec(text: &str) -> Result<f64, String> {
    let size = text.strip_suffix("/s").unwrap_or(text);
    let bytes = parse_size(size)?;
    if bytes == 0 {
        return Err(format!("{text:?}: a rate is above 0"));
    }
    Ok(float(bytes))
}

/// A rate in calls per second, above zero.
pub fn parse_rate(text: &str) -> Result<f64, String> {
    match text.parse::<f64>() {
        Ok(rate) if rate.is_finite() && rate > 0.0 => Ok(rate),
        _ => Err(format!(
            "{text:?}: a rate is a number of calls per second above 0"
        )),
    }
}

/// A byte count for people: `1.5 GiB`.
pub fn bytes_label(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// The number at the head of `text`, and the unit after it.
fn split_number(text: &str) -> (&str, &str) {
    let text = text.trim();
    let at = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(at);
    (number, unit.trim())
}

fn number_of(text: &str, number: &str) -> Result<f64, String> {
    number
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
        .ok_or_else(|| format!("{text:?}: no number"))
}

fn scale_of(unit: &str) -> Option<f64> {
    Some(match unit {
        "" | "B" => 1.0,
        "KB" | "kB" => 1e3,
        "MB" => 1e6,
        "GB" => 1e9,
        "TB" => 1e12,
        "KiB" => 1024.0,
        "MiB" => 1024.0 * 1024.0,
        "GiB" => 1024.0 * 1024.0 * 1024.0,
        "TiB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    })
}

/// A count as a float, for rates and display.
#[allow(
    clippy::cast_precision_loss,
    reason = "a rate or a printed number needs no 64-bit precision"
)]
pub fn float(count: u64) -> f64 {
    count as f64
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        bytes_label, parse_bytes_per_sec, parse_duration, parse_rate, parse_size, parse_time_nanos,
    };

    #[test]
    fn durations_parse_with_their_unit() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("60s"), Ok(Duration::from_secs(60)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert!(parse_duration("60").is_err());
        assert!(parse_duration("s").is_err());
        assert!(parse_duration("1h").is_err());
    }

    #[test]
    fn a_rate_is_above_zero() {
        assert_eq!(parse_rate("250"), Ok(250.0));
        assert_eq!(parse_rate("0.5"), Ok(0.5));
        assert!(parse_rate("0").is_err());
        assert!(parse_rate("-1").is_err());
        assert!(parse_rate("inf").is_err());
        assert!(parse_rate("fast").is_err());
    }

    #[test]
    fn sizes_parse_in_both_unit_families() {
        assert_eq!(parse_size("64"), Ok(64));
        assert_eq!(parse_size("64B"), Ok(64));
        assert_eq!(parse_size("4KiB"), Ok(4096));
        assert_eq!(parse_size("1MB"), Ok(1_000_000));
        assert_eq!(parse_size("10GiB"), Ok(10 << 30));
        assert_eq!(parse_size("1.5KiB"), Ok(1536));
        assert!(parse_size("10 parsecs").is_err());
        assert!(parse_size("GiB").is_err());
        assert!(parse_size("").is_err());
    }

    #[test]
    fn byte_rates_and_times_parse() {
        assert_eq!(parse_bytes_per_sec("100MiB/s"), Ok(100.0 * 1024.0 * 1024.0));
        assert_eq!(parse_bytes_per_sec("2KB"), Ok(2000.0));
        assert!(parse_bytes_per_sec("0/s").is_err());
        assert_eq!(parse_time_nanos("5ms"), Ok(5e6));
        assert_eq!(parse_time_nanos("1.5s"), Ok(1.5e9));
        assert_eq!(parse_time_nanos("200us"), Ok(2e5));
        assert!(parse_time_nanos("5").is_err());
    }

    #[test]
    fn a_byte_count_prints_in_its_largest_unit() {
        assert_eq!(bytes_label(512.0), "512 B");
        assert_eq!(bytes_label(1536.0), "1.5 KiB");
        assert_eq!(bytes_label(3.0 * f64::from(1 << 30)), "3.0 GiB");
    }
}
