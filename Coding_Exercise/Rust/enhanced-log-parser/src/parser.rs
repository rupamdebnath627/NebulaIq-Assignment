use serde::Serialize;

// Derive Serialize so serde_json can convert this to JSON automatically.
#[derive(Debug, Serialize, PartialEq)]
pub struct LogStats {
    pub info: u32,
    pub warn: u32,
    pub error: u32,
}

// The filter function (same as before, but public 'pub')
pub fn filter_logs(content: &str, target_level: &str) -> Vec<String> {
    let mut filtered_lines = Vec::new();
    for line in content.lines() {
        if line.contains(target_level) {
            filtered_lines.push(line.to_string());
        }
    }
    filtered_lines
}

// The counting function (same as before, but public 'pub')
pub fn count_logs(content: &str) -> LogStats {
    let mut stats = LogStats { info: 0, warn: 0, error: 0 };
    for line in content.lines() {
        if line.contains("[INFO]") {
            stats.info += 1;
        } else if line.contains("[WARN]") {
            stats.warn += 1;
        } else if line.contains("[ERROR]") {
            stats.error += 1;
        }
    }
    stats
}

// --- UNIT TESTS ---
// The #[cfg(test)] attribute tells Rust to only compile this when running tests.
#[cfg(test)]
mod tests {
    use super::*; 

    #[test]
    fn test_count_logs() {
        let sample_data = "[INFO] User logged in\n[ERROR] DB crash\n[INFO] Data loaded";
        let stats = count_logs(sample_data);
        
        let expected = LogStats { info: 2, warn: 0, error: 1 };
        assert_eq!(stats, expected, "The log counts should match perfectly.");
    }

    #[test]
    fn test_filter_logs() {
        let sample_data = "[INFO] User logged in\n[ERROR] DB crash\n[INFO] Data loaded";
        let filtered = filter_logs(sample_data, "[ERROR]");
        
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0], "[ERROR] DB crash");
    }
}