use std::env;
use std::fs;

// A simple struct to hold our counted statistics
struct LogStats {
    info: u32,
    warn: u32,
    error: u32,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        println!("Please provide a file path! Example: cargo run logs.txt");
        return;
    }

    let file_path = &args[1]; 

    let log_content = fs::read_to_string(file_path)
        .expect("Failed to read the file. Make sure logs.txt exists!");

    let errors_only = filter_logs(&log_content, "[ERROR]");
    println!("Filtered Results: Found {} error(s).", errors_only.len());

    let statistics = count_logs(&log_content);
    print_stats(statistics);
}

// This function takes references (&str). It only looks at the data.
fn filter_logs(content: &str, target_level: &str) -> Vec<String> {
    let mut filtered_lines = Vec::new();

    for line in content.lines() {
        if line.contains(target_level) {
            filtered_lines.push(line.to_string());
        }
    }

    filtered_lines
}

// This function also just looks at the data to count it.
fn count_logs(content: &str) -> LogStats {
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

    stats // Return the newly created struct
}

// Log statistics
fn print_stats(stats: LogStats) {
    println!("\n--- Log Statistics ---");
    println!("INFO count:  {}", stats.info);
    println!("WARN count:  {}", stats.warn);
    println!("ERROR count: {}", stats.error);
    println!("----------------------\n");
    
}