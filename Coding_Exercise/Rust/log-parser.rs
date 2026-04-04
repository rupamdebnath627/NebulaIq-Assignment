use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Debug, PartialEq)]
enum LogLevel {
    Info,
    Warning,
    Error,
}

struct LogEntry {
    level: LogLevel,
    message: String,
}

struct LogStats {
    info: u32,
    warning: u32,
    error: u32,
}

fn main() {
    // Read command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Please provide a file path! Example: ./log-parser logs.txt");
        return;
    }

    let file_path = &args[1];

    // Open the file and create a BufReader
    let file = File::open(file_path).expect("Failed to open the file!");
    let reader = BufReader::new(file);

    let mut log_entries: Vec<LogEntry> = Vec::new();

    // Read line-by-line
    for line_result in reader.lines() {
        let line = line_result.expect("Failed to read a line");
        
        if let Some(entry) = parse_line(line) {
            log_entries.push(entry);
        }
    }

    // Generate statistics
    let stats = count_logs(&log_entries);

    print_stats(stats);

    // Filter the logs
    let target_level = LogLevel::Error;
    let errors_only = filter_logs(&log_entries, &target_level);

    println!("Found {} error(s).", errors_only.len());

    if let Some(first_error) = errors_only.first() {
        println!("First error message: {}", first_error.message);
    }
}

// OWNERSHIP: Takes ownership of `line` (String) and packages it into a `LogEntry`.
fn parse_line(line: String) -> Option<LogEntry> {
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

// BORROWING: Takes a slice of LogEntries (`&[LogEntry]`). 
fn count_logs(entries: &[LogEntry]) -> LogStats {
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

// BORROWING: This takes a slice of entries, and returns a Vector of REFERENCES (`&LogEntry`).
fn filter_logs<'a>(entries: &'a [LogEntry], target_level: &LogLevel) -> Vec<&'a LogEntry> {
    let mut filtered = Vec::new();

    for entry in entries {
        if entry.level == *target_level {
            filtered.push(entry); 
        }
    }
    filtered
}

// OWNERSHIP MOVED: Takes full ownership of `LogStats` to print it, then drops it.
fn print_stats(stats: LogStats) {
    println!("\n--- Log Statistics ---");
    println!("INFO count:    {}", stats.info);
    println!("WARNING count: {}", stats.warning);
    println!("ERROR count:   {}", stats.error);
    println!("----------------------\n");
}