#![allow(unused_imports)]
#![allow(dead_code)]

mod csv_to_parquet_utils;
mod file_utils;

mod cli_interface_builder;
mod cli_interface_derive;
mod stopwatch;
// mod file_processing_utils;

//use std::fs::{File, OpenOptions};

use clap::ValueHint;
use std::fs;
use std::fs::OpenOptions;
use std::path::Path;
//use indicatif::{ProgressBar, ProgressStyle};
use clap::Parser;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use std::path::PathBuf;

//use std::time::Instant;
//use humantime::format_duration;
use polars::error::PolarsError;
// use clap::Arg;
// use clap::Command;
// // use clap::{arg, command, value_parser, ArgAction, Command};
// // //use clap::{App, Subcommand};
// // use clap::App;
//
// //#[derive(Parser)]
// //#[command(version, about, long_about = None)]
//
// use clap::{Parser, Subcommand};
//
// #[derive(Parser, Debug)]
// struct Cli {
//     #[clap(short, long, help = "Forces the operation")]
//     force: bool,
//
//     #[clap(short = 'o', long = "output-dir", value_name = "DIR", help = "Specifies the output directory", default_value = "./output")]
//     output_dir: String,
//
//     #[clap(short = 'v', long = "verbosity", value_name = "LEVEL", help = "Set verbosity level (0-10)", default_value_t = 2, value_parser = clap::value_parser!(i32).range(0..=10))]
//     verbosity: i32,
//
//     // #[clap(short = 'g', long = "gain", value_name = "VALUE", help = "Set gain value (float)", default_value_t = 1.0, value_parser = clap::value_parser!(f32))]
//     // gain: f32,
//
//     #[clap(value_name = "PATHS", help = "Paths to files or directories", num_args = 1..)]
//     paths: Vec<String>,
// }
// use cli_interface_derive;
// use cli_interface_builder;

//Result<(), Box<dyn std::error::Error>>
fn main() -> Result<(), PolarsError> {
    let (output_dir, force, verbosity, args) = cli_interface_builder::process_cli_via_builder_api();

    //let (output_dir, force, verbosity, args)= cli_interface_derive::process_cli_via_derive_api();

    // let cli = cli_interface::Cli::parse();
    //
    //
    //
    // // Access arguments as needed
    // let args: Vec<&str> = cli.paths.iter().map(|s| s as &str).collect();
    // let verbosity = cli.verbosity;
    // //let gain = cli.gain;
    // let force = cli.force;
    // let output_dir=cli.output_dir;

    if args.len() == 1 {
        // if there is only one arg on the command line, check if it is a directory
        //let filepath_0=args[0];
        let first_element: &str = args.first().expect("no element");
        let metadata = fs::metadata(first_element)?;
        if metadata.is_dir() {
            print!("{} is a directory", first_element)
        }
    }

    // // Accessing specific arguments
    // let force = matches.get_flag("force");
    // let output_dir = matches.get_one::<String>("output_dir").expect("Expected outputdir");

    //let mut args = std::env::args().skip(1);

    println!("Force: {}", force);
    println!("Output directory: {}", output_dir);
    println!("Additional arguments: {:?}", args);

    // Parse command-line arguments

    let mut processed_files = 0;
    let total_files = args.len();

    //let output_dir = "./out";
    file_utils::create_dir_if_not_exists(&output_dir)?;

    // Preamble for main loop
    let sw1 = stopwatch::Stopwatch::new();
    let bar = ProgressBar::new(total_files as u64);

    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{total} ({msg})")
            .unwrap(),
    );

    let mut file_count = 0;
    let mut total_delta_file_size = 0 as i64;

    // main loop
    for csv_filename in args {
        //while let Some(csv_filename) = args.next() {
        let parquet_filename = file_utils::replace_file_extension(&csv_filename, ".parquet");
        let parquet_filename =
            file_utils::prepend_output_dir_to_filename(&output_dir, &parquet_filename);

        //let csv_filename: &str= &csv_filename;
        //let parquet_filename: &str=&parquet_filename;

        let parquet_filename_path = Path::new(&parquet_filename);

        let sw2 = stopwatch::Stopwatch::new();

        let file_exists = parquet_filename_path.exists();

        if (!file_exists) || force {
            csv_to_parquet_utils::convert_csv_file_to_parquet_file(
                &csv_filename,
                &parquet_filename,
            )?;

            let file_size_csv = file_utils::get_file_size(&csv_filename)?;
            let file_size_parquet = file_utils::get_file_size(&parquet_filename)?;

            let file_size_ratio = file_size_parquet as f64 / file_size_csv as f64;
            let delta_file_size = file_size_csv as i64 - file_size_parquet as i64;

            total_delta_file_size += delta_file_size;

            if verbosity > 3 {
                print!("\n");
                print!("Converted {} ({} bytes)", csv_filename, file_size_csv);
                print!("---> {} ({} bytes)", parquet_filename, file_size_parquet);
                print!(
                    "\t delta file size: {:.2} Mbytes",
                    delta_file_size as f64 / (1024.0 * 1024.0)
                );
                print!(" reduction ratio={:.2} %", file_size_ratio * 100.0);
                //print!(" dt={}", sw2.elapsed().as_secs());
                print!(" dt={}", sw2.elapsed_formatted_human());
            }
        } else {
            print!(
                "\n Skipped processing on already existing file {}",
                csv_filename
            );
        }

        processed_files += 1;
        let fraction_complete = processed_files as f64 / total_files as f64;
        bar.set_message(format!(
            "{}/{} {:.1}%",
            processed_files,
            total_files,
            fraction_complete * 100.0
        ));
        bar.inc(1);

        file_count += 1;
    }

    bar.finish();

    println!("{} CSV files successfully converted to Parquet", file_count);
    println!(
        " total_delta_file_size={:.4} Mb",
        total_delta_file_size / (1024 * 1024)
    );
    println!(
        "Total Time elapsed in foo() is: {:?}",
        sw1.elapsed_formatted_human()
    );

    Ok(())
}
