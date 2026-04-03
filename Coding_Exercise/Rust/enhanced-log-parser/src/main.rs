mod parser;

use parser::{count_logs, filter_logs};
use clap::{Parser, ValueEnum};
use tokio::fs;
use std::fmt;

// --- CUSTOM ERROR TYPE ---
#[derive(Debug)]
pub enum LogError {
    IoError(std::io::Error),
}

// Implement standard formatting so it can be printed
impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogError::IoError(err) => write!(f, "Failed to read the file: {}", err),
        }
    }
}

// Allow the '?' operator to automatically convert std::io::Error into our LogError
impl From<std::io::Error> for LogError {
    fn from(error: std::io::Error) -> Self {
        LogError::IoError(error)
    }
}

// --- CLI ARGUMENTS ---
#[derive(Parser)]
#[command(name = "Log Parser", about = "An async CLI tool to parse logs")]
struct Cli {
    /// The path to the log file
    #[arg(short, long)]
    file: String,

    /// Filter by a specific level (e.g., [ERROR])
    #[arg(short = 'l', long)]
    filter_level: Option<String>,

    /// The output format (console or json)
    #[arg(short = 'o', long, value_enum, default_value_t = OutputFormat::Console)]
    format: OutputFormat,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Console,
    Json,
}

// --- ASYNC MAIN ---
#[tokio::main]
async fn main() -> Result<(), LogError> {
    // Parse the command line arguments
    let cli = Cli::parse();

    // Read file asynchronously. 
    let log_content = fs::read_to_string(&cli.file).await?;

    // Generate Statistics
    let stats = count_logs(&log_content);

    // Output results based on user's chosen format
    match cli.format {
        OutputFormat::Console => {
            if let Some(level) = cli.filter_level {
                let filtered_lines = filter_logs(&log_content, &level);
                println!("--- Filtered Logs ({}) ---", level);
                for line in filtered_lines {
                    println!("{}", line);
                }
                println!("");
            }

            // Print stats
            println!("--- Log Statistics ---");
            println!("INFO count:  {}", stats.info);
            println!("WARN count:  {}", stats.warn);
            println!("ERROR count: {}", stats.error);
            println!("----------------------");
        }
        OutputFormat::Json => {
            // Serialize the struct to a pretty JSON string
            let json_output = serde_json::to_string_pretty(&stats).expect("Failed to serialize to JSON");
            println!("{}", json_output);
        }
    }

    Ok(())
}