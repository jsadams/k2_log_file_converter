#![allow(unused_imports)]
#![allow(dead_code)]

// mod csv_to_parquet_utils;
// mod file_utils;
//
// mod stopwatch;
// mod file_processing_utils;
//use std::fs::{File, OpenOptions};

use crate::csv_to_parquet_utils;
use crate::stopwatch::Stopwatch;
use std::fs;
use std::fs::OpenOptions;
use std::path::Path;
//use indicatif::{ProgressBar, ProgressStyle};
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use std::path::PathBuf;

//use std::time::Instant;
//use humantime::format_duration;
use clap::Arg;
use clap::Command;
use polars::error::PolarsError;
// use clap::{arg, command, value_parser, ArgAction, Command};
// //use clap::{App, Subcommand};
// use clap::App;

use crate::file_utils;

fn process_single_file(
    csv_filename: &str,
    output_dir: &str,
    file_count: &mut i32,
    &total_delta_file_size: &i64,
    &processed_files: &i32,
    &total_files: &i32,
    bar: &ProgressBar,
) -> Result<(), PolarsError> {
    //while let Some(csv_filename) = args.next() {
    let parquet_filename = file_utils::replace_file_extension(&csv_filename, ".parquet");
    let parquet_filename = file_utils::prepend_output_dir_to_filename(output_dir, &parquet_filename);

    //let csv_filename: &str= &csv_filename;
    //let parquet_filename: &str=&parquet_filename;

    let parquet_filename_path = Path::new(&parquet_filename);

    let sw2 = Stopwatch::new();

    let file_exists = parquet_filename_path.exists();

    if !file_exists {
        csv_to_parquet_utils::convert_csv_file_to_parquet_file(&csv_filename, &parquet_filename)?;

        let file_size_csv = file_utils::get_file_size(&csv_filename)?;
        let file_size_parquet = file_utils::get_file_size(&parquet_filename)?;

        let file_size_ratio = file_size_parquet as f64 / file_size_csv as f64;
        let delta_file_size = file_size_csv as i64 - file_size_parquet as i64;

        total_delta_file_size += delta_file_size;

        print!("\n");
        print!("Converted {} ({} bytes)", csv_filename, file_size_csv);
        print!("---> {} ({} bytes)", parquet_filename, file_size_parquet);
        print!("\t delta file size: {:.2} Mbytes", delta_file_size as f64 / (1024.0 * 1024.0));
        print!(" reduction ratio={:.2} %", file_size_ratio * 100.0);
        //print!(" dt={}", sw2.elapsed().as_secs());
        print!(" dt={}", sw2.elapsed_formatted_human());
    } else {
        print!("\n Skipped processing on already existing file {}", csv_filename);
    }

    processed_files += 1;
    let fraction_complete = processed_files as f64 / total_files as f64;
    bar.set_message(format!("{}/{} {:.1}%", processed_files, total_files, fraction_complete * 100.0));
    bar.inc(1);

    *file_count += 1;

    return Ok(());
}
