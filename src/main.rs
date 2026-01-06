use std::process;

use anyhow::Result;
use clap::Parser;

use megrez::cli::Cli;
use megrez::commands;
use megrez::util::errors::UnsupportedFormatError;

fn main() {
    let cli = Cli::parse();
    let result = run(cli);
    match result {
        Ok(()) => process::exit(0),
        Err(err) => {
            if err.downcast_ref::<UnsupportedFormatError>().is_some() {
                eprintln!("{err}");
                process::exit(2);
            }
            eprintln!("{err}");
            process::exit(1);
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        megrez::cli::Command::Schema {
            file,
            format,
            show_format_name,
            show_columns,
        } => commands::schema::run(
            &file,
            format.map(|format| format.to_format()),
            show_format_name,
            show_columns,
        ),
        megrez::cli::Command::Cat {
            file,
            format,
            limit,
        } => commands::cat::run(&file, format.map(|format| format.to_format()), limit),
    }
}
