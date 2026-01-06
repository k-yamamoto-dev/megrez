use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::formats::Format;
use clap::{ArgAction, builder::BoolishValueParser};

#[derive(Debug, Parser)]
#[command(
    name = "megrez",
    version,
    about = "Detect data formats and print schema or content"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Detect format and print schema
    Schema {
        /// Input file path
        file: PathBuf,
        /// Override detected format
        #[arg(long, value_enum)]
        format: Option<FormatArg>,
        /// Show the detected format name
        #[arg(
            long,
            default_value_t = true,
            action = ArgAction::Set,
            value_parser = BoolishValueParser::new()
        )]
        show_format_name: bool,
        /// Show the column header row
        #[arg(
            long,
            default_value_t = true,
            action = ArgAction::Set,
            value_parser = BoolishValueParser::new()
        )]
        show_columns: bool,
    },
    /// Print file contents (raw for JSON/CSV, JSON Lines for binary formats)
    Cat {
        /// Input file path
        file: PathBuf,
        /// Override detected format
        #[arg(long, value_enum)]
        format: Option<FormatArg>,
        /// Limit number of records for Parquet/Avro/ORC
        #[arg(long)]
        limit: Option<usize>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum FormatArg {
    Json,
    Csv,
    Parquet,
    Avro,
    Orc,
}

impl FormatArg {
    pub fn to_format(self) -> Format {
        match self {
            FormatArg::Json => Format::Json,
            FormatArg::Csv => Format::Csv,
            FormatArg::Parquet => Format::Parquet,
            FormatArg::Avro => Format::Avro,
            FormatArg::Orc => Format::Orc,
        }
    }
}
