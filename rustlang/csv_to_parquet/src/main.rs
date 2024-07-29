#![allow(unused_imports)]
#![allow(dead_code)]

mod csv_to_parquet_utils;
mod file_utils;

mod stopwatch;

//use std::fs::{File, OpenOptions};

use std::path::Path;
use std::fs::OpenOptions;
//use indicatif::{ProgressBar, ProgressStyle};
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use std::path::PathBuf;

//use std::time::Instant;
//use humantime::format_duration;
use polars::error::PolarsError;
use clap::Arg;
use clap::Command;
// use clap::{arg, command, value_parser, ArgAction, Command};
// //use clap::{App, Subcommand};
// use clap::App;

//#[derive(Parser)]
//#[command(version, about, long_about = None)]
fn main() -> Result<(), PolarsError> {

    let matches = Command::new("myapp")
        .version("1.0")
        .about("An example CLI app")
        .arg(
            Arg::new("force")
                .short('f')
                .long("force")
                .help("Forces the operation")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("outputdir")
                .short('o')
                .long("outputdir")
                .value_name("outputdir")
                .help("Specifies the output directory")
                .value_parser(clap::value_parser!(String))
                .default_value("./out")
        )
        .arg(
            Arg::new("args")
                .help("Additional arguments")
                .num_args(1..)
                .allow_hyphen_values(true),
        )
        .get_matches();

    // let force = matches.get_flag("force");
    // let outputdir = matches.get_one::<String>("outputdir").unwrap_or(&"default/output/dir".to_string());
    let args: Vec<&String> = matches.get_many::<String>("args").unwrap_or_default().collect();

    // Accessing specific arguments
    let force = matches.get_flag("force");
    let outputdir = matches.get_one::<String>("outputdir").expect("Expected outputdir");

    //let mut args = std::env::args().skip(1);

    println!("Force: {}", force);
    println!("Output directory: {}", outputdir);
    println!("Additional arguments: {:?}", args);

        println!("Force: {}", force);
        println!("Output directory: {}", outputdir);
        println!("Additional arguments: {:?}", args);

        // Parse command-line arguments

    let mut processed_files = 0;
    let total_files = args.len();


    let output_dir = "./out";
    file_utils::create_dir_if_not_exists(output_dir)?;

    // Preamble for main loop
    let sw1 = stopwatch::Stopwatch::new();
    let bar = ProgressBar::new(total_files as u64);

    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{total} ({msg})").unwrap(),
    );

    let mut file_count = 0;
    let mut total_delta_file_size = 0 as i64;

    // main loop
    for csv_filename in args
    {
    //while let Some(csv_filename) = args.next() {
        let parquet_filename = file_utils::replace_file_extension(&csv_filename, ".parquet");
        let parquet_filename =
            file_utils::prepend_output_dir_to_filename(output_dir, &parquet_filename);

        //let csv_filename: &str= &csv_filename;
        //let parquet_filename: &str=&parquet_filename;

        let parquet_filename_path = Path::new(&parquet_filename);

        let sw2 = stopwatch::Stopwatch::new();

        let file_exists=parquet_filename_path.exists();


        if ! file_exists
        {
            csv_to_parquet_utils::convert_csv_file_to_parquet_file(&csv_filename, &parquet_filename)?;


            let file_size_csv = file_utils::get_file_size(&csv_filename)?;
            let file_size_parquet = file_utils::get_file_size(&parquet_filename)?;

            let file_size_ratio = file_size_parquet as f64 / file_size_csv as f64;
            let delta_file_size = file_size_csv as i64 - file_size_parquet as i64;

            total_delta_file_size += delta_file_size;

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
        else
        {
            print!("\n Skipped processing on already existing file {}", csv_filename);
        }

        processed_files += 1;
        let fraction_complete=processed_files as f64 /total_files as f64;
        bar.set_message(format!("{}/{} {:.1}%", processed_files,total_files, fraction_complete*100.0));
        bar.inc(1);

        file_count += 1;
    }

    bar.finish();

    println!("{} CSV files successfully converted to Parquet", file_count);
    println!(" total_delta_file_size={:.4} Mb", total_delta_file_size / (1024 * 1024));
    println!("Total Time elapsed in foo() is: {:?}",sw1.elapsed_formatted_human());

    Ok(())
}
