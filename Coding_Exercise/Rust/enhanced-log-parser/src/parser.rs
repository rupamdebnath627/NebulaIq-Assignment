use clap::ValueEnum;
use serde::Serialize;

// By adding #[value(name = "...")] we tell the CLI to expect the exact uppercase strings.
// The `alias` attributes let it act as a fallback in case you accidentally type lowercase!
#[derive(Debug, PartialEq, Serialize, Clone, ValueEnum)]
pub enum LogLevel {
    #[value(name = "INFO", alias = "info")]
    Info,
    #[value(name = "WARN", alias = "warn", alias = "warning", alias = "WARNING")]
    Warning,
    #[value(name = "ERROR", alias = "error")]
    Error,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct LogEntry {
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct LogStats {
    pub info: u32,
    pub warning: u32,
    pub error: u32,
}

// --- PARSING LOGIC ---

pub fn parse_line(line: String) -> Option<LogEntry> {
    // This perfectly matches the [INFO], [WARN], and [ERROR] in your logs
    if line.contains("[INFO]") {
        Some(LogEntry { level: LogLevel::Info, message: line })
    } else if line.contains("[WARN]") {
        Some(LogEntry { level: LogLevel::Warning, message: line })
    } else if line.contains("[ERROR]") {
        Some(LogEntry { level: LogLevel::Error, message: line })
    } else {
        None
    }
}

pub fn count_logs(entries: &[LogEntry]) -> LogStats {
    let mut stats = LogStats { info: 0, warning: 0, error: 0 };
    for entry in entries {
        match entry.level {
            LogLevel::Info => stats.info += 1,
            LogLevel::Warning => stats.warning += 1,
            LogLevel::Error => stats.error += 1,
        }
    }
    stats
}

pub fn filter_logs<'a>(entries: &'a [LogEntry], target_level: &LogLevel) -> Vec<&'a LogEntry> {
    let mut filtered = Vec::new();
    for entry in entries {
        if entry.level == *target_level {
            filtered.push(entry);
        }
    }
    filtered
}

// --- UNIT TESTS ---
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_line_success() {
        let line = String::from("2026-04-03 20:22:37.222 [ERROR] 12345 --- [ main] o.s.w.s.DispatcherServlet : Failed to save");
        let result = parse_line(line).unwrap();
        
        assert_eq!(result.level, LogLevel::Error);
        assert!(result.message.contains("[ERROR]"));
    }

    #[test]
    fn test_parse_line_warn() {
        // Testing your specific log format
        let line = String::from("2026-04-03 20:22:28.809 [WARN]  12345 --- [ restartedMain] o.s.w.s.DispatcherServlet");
        let result = parse_line(line).unwrap();
        
        assert_eq!(result.level, LogLevel::Warning);
    }

    #[test]
    fn test_parse_line_invalid() {
        let line = String::from("Just some random text without a log level");
        let result = parse_line(line);
        assert_eq!(result, None);
    }

    #[test]
    fn test_count_logs() {
        let entries = vec![
            LogEntry { level: LogLevel::Info, message: String::from("[INFO] Test 1") },
            LogEntry { level: LogLevel::Error, message: String::from("[ERROR] Test 2") },
            LogEntry { level: LogLevel::Info, message: String::from("[INFO] Test 3") },
        ];

        let stats = count_logs(&entries);
        assert_eq!(stats.info, 2);
        assert_eq!(stats.warning, 0);
        assert_eq!(stats.error, 1);
    }
}