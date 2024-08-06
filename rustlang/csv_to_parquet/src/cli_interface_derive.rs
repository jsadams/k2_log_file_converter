use clap::crate_version;
use clap::Arg;
use clap::Command;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "csv_to_parquet",
    author = "jsadams<jsadams@umbc.edu>",
    version=crate_version!())]
//#[command(name = "example", version = crate_version!())]
pub struct Cli {
    #[clap(short = 'f', long = "force", help = "Forces conversion even if file exists")]
    pub force: bool,

    // #[clap(
    //     short = 'o',
    //     long = "output-dir",
    //     value_name = "DIR",
    //     help = "Specifies the output directory",
    //     default_value = "./output"
    // )]
    // pub output_dir: String,
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

    #[clap(
        short = 'T',
        long = "decimate_period_sec",
        value_name = "VALUE",
        help = "seconds over which to decimate",
        default_value_t = 10.0,
        value_parser = clap::value_parser!(f32)
    )]
    pub downsample_period_sec: f32,

    #[clap(short = 'd', long = "decimate", help = "Forces a decimated output")]
    pub do_downsample: bool,

    #[clap(
        short = 'e',
        long = "default_extension",
        value_name = "STRING",
        help = "Specifies the default input extension for directory mode",
        default_value = ".dat"
    )]
    pub default_input_extension: String,
}

// pub fn process_cli_via_derive_api() -> (std::string::String, bool, i32, Vec<std::string::String>) {
//     let cli = Cli::parse();
//
//     // Access arguments as needed
//     //let args: Vec<&str> = cli.paths.iter().map(|s| s as &str).collect();
//     let args: Vec<String> = cli
//         .paths
//         .iter()
//         .map(|s| std::string::String::from(s))
//         .collect();
//     let verbosity = cli.verbosity;
//     //let gain = cli.gain;
//     let force = cli.force;
//     let output_dir = cli.output_dir;
//
//     (output_dir, force, verbosity, args)
// }

pub fn process_cli_via_derive_api() -> (Cli, Vec<std::string::String>) {
    let cli = Cli::parse();

    // Access arguments as needed
    //let args: Vec<&str> = cli.paths.iter().map(|s| s as &str).collect();
    let args: Vec<String> = cli.paths.iter().map(|s| std::string::String::from(s)).collect();
    // let verbosity = cli.verbosity;
    // //let gain = cli.gain;
    // let force = cli.force;
    // let output_dir = cli.output_dir;

    (cli, args)
}
