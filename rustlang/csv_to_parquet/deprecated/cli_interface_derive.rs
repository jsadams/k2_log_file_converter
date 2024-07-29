use clap::Arg;
use clap::Command;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "csv_to_parquet",
    version = "1.0",
    author = "jsadams<jsadams@umbc.edu>"
)]
pub struct Cli {
    #[clap(
        short = "f",
        long = "force",
        help = "Forces conversion even if file exists"
    )]
    pub force: bool,

    #[clap(
        short = 'o',
        long = "output-dir",
        value_name = "DIR",
        help = "Specifies the output directory",
        default_value = "./output"
        )]
    pub output_dir: String,

    #[clap(
        short = 'v',
        long = "verbosity",
        value_name = "LEVEL",
        help = "Set verbosity level (0-10)",
        default_value_t = 2,
        value_parser = clap::value_parser!(i32).range(0..=10)
    )]
    pub verbosity: i32,

    // #[clap(short = 'g', long = "gain", value_name = "VALUE", help = "Set gain value (float)", default_value_t = 1.0, value_parser = clap::value_parser!(f32))]
    // gain: f32,
    #[clap(
        value_name = "PATHS",
        help = "Paths to files or directories",
        num_args = 1..
    )]
    pub paths: Vec<String>,
}

pub fn process_cli_via_derive_api() -> (std::string::String, bool, i32, Vec<std::string::String>) {
    let cli = Cli::parse();

    // Access arguments as needed
    //let args: Vec<&str> = cli.paths.iter().map(|s| s as &str).collect();
    let args: Vec<String> = cli
        .paths
        .iter()
        .map(|s| std::string::String::from(s))
        .collect();
    let verbosity = cli.verbosity;
    //let gain = cli.gain;
    let force = cli.force;
    let output_dir = cli.output_dir;

    (output_dir, force, verbosity, args)
}
