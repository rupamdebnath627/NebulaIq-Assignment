mod parser;

use parser::{count_logs, filter_logs, parse_line, LogEntry, LogLevel, LogStats};

use clap::{Parser, ValueEnum};
use std::fmt;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

// --- CUSTOM ERROR TYPE ---
#[derive(Debug)]
pub enum LogError {
    Io(std::io::Error),
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogError::Io(err) => write!(f, "File Error: {}", err),
        }
    }
}

impl From<std::io::Error> for LogError {
    fn from(err: std::io::Error) -> Self {
        LogError::Io(err)
    }
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Console,
    Json,
}

// --- CLI ARGUMENTS (CLAP) ---
#[derive(Parser)]
#[command(name = "Log Parser", about = "An async log parser")]
struct Cli {
    #[arg(short, long)]
    file: String,

    #[arg(short = 'l', long)]
    filter_level: Option<LogLevel>,

    #[arg(short = 'o', long, value_enum, default_value_t = OutputFormat::Console)]
    format: OutputFormat,
}

// --- ASYNC MAIN (TOKIO) ---
#[tokio::main]
async fn main() -> Result<(), LogError> {
    let cli = Cli::parse();

    let file = File::open(&cli.file).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut log_entries: Vec<LogEntry> = Vec::new();

    while let Some(line) = lines.next_line().await? {
        if let Some(entry) = parse_line(line) {
            log_entries.push(entry);
        }
    }

    let stats = count_logs(&log_entries);

    match cli.format {
        OutputFormat::Console => {
            print_stats(&stats);

            if let Some(target_level) = cli.filter_level {
                let filtered = filter_logs(&log_entries, &target_level);
                println!("Found {} {} log(s).", filtered.len(), stringify_level(&target_level));
                for entry in filtered {
                    println!("{}", entry.message);
                }
            }
        }
        OutputFormat::Json => {
            let json_out = serde_json::to_string_pretty(&stats).expect("Failed to serialize");
            println!("{}", json_out);
        }
    }

    Ok(())
}

// --- HELPER FUNCTIONS ---
fn print_stats(stats: &LogStats) {
    println!("\n--- Log Statistics ---");
    println!("INFO count:    {}", stats.info);
    println!("WARNING count: {}", stats.warning);
    println!("ERROR count:   {}", stats.error);
    println!("----------------------\n");
}

fn stringify_level(level: &LogLevel) -> &str {
    match level {
        LogLevel::Info => "INFO",
        LogLevel::Warning => "WARNING",
        LogLevel::Error => "ERROR",
    }
}