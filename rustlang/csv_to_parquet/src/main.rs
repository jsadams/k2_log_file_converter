#![allow(unused_imports)]
#![allow(dead_code)]

use crate::file_utils::is_directory;
use std::{io, string};

mod csv_to_parquet_utils;
mod file_utils;

//mod cli_interface_builder;
mod cli_interface_derive;
mod decimation_utils;
mod downsample_utils;
mod polars_conversion_utils;
mod sample_rate_utils;
///mod cli_interface_derive;
mod stopwatch;
//mod main_loop_sub;
// mod file_processing_utils;

//use std::fs::{File, OpenOptions};

use clap::ValueHint;
use std::fs;
use std::fs::OpenOptions;
use std::path::Path;
//use indicatif::{ProgressBar, ProgressStyle};
use clap::builder::Str;
use clap::Parser;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use std::path::PathBuf;
//use std::time::Instant;
//use humantime::format_duration;
use polars::error::PolarsError;

fn get_vector_of_all_files(args: Vec<String>, do_downsampling: bool, default_input_extension: &str) -> Result<Vec<(String, String)>, std::io::Error> {
    let mut vec_of_tuples: Vec<(String, String)> = Vec::new();

    for arg_i in args {
        //let mut input_pathnames: Vec<String> = Vec::new();

        if is_directory(&arg_i)? {
            let input_directory = arg_i;

            print!("{} is a directory", input_directory);
            let output_directory = if do_downsampling {
                input_directory.to_owned() + "_parquet_ds"
            } else {
                input_directory.to_owned() + "_parquet"
            };

            let input_pathnames = file_utils::get_files_inside_directory(&input_directory, default_input_extension)
                .unwrap()
                .clone();

            for input_filename_subdir in input_pathnames {
                //let input_filename = input_filename_j;
                vec_of_tuples.push((input_filename_subdir.clone(), output_directory.clone()));

                if is_directory(&input_filename_subdir)? {
                    println!("warning skipping subdirectory{}", input_filename_subdir)
                }
            }
        } else {
            let input_filename = arg_i;
            let output_directory = String::from(".");

            vec_of_tuples.push((input_filename, output_directory.clone()));
        }
    }

    return Ok(vec_of_tuples);
}

fn get_postfix_plus_extension(do_downsampling: bool) -> String {
    let mut default_postfix = String::from("");
    let default_extension = String::from(".parquet");

    if do_downsampling {
        default_postfix = String::from("_ds");
    }

    //let postfix_plus_extension=default_postfix+default_extension;
    let postfix_plus_extension = format!("{default_postfix}{default_extension}");

    return postfix_plus_extension;
}

fn main() -> Result<(), PolarsError> {
    let (cli, args) = cli_interface_derive::process_cli_via_derive_api();

    //let mut input_pathnames: Vec<String> = args.clone();
    let force = cli.force;
    let do_downsampling = cli.do_downsample;
    let downsampling_period_sec = cli.downsample_period_sec;
    //let mut output_dir = cli.output_dir.clone();
    let verbosity = cli.verbosity;
    let default_input_extension = cli.default_input_extension.as_str();

    //let mut output_dir=String::from(".");

    let vec_of_tuples = get_vector_of_all_files(args, do_downsampling, default_input_extension)?;

    // Parse command-line arguments

    let mut processed_files = 0;
    let n_total_files = vec_of_tuples.len();

    //let output_dir = "./out";
    //file_utils::create_dir_if_not_exists(&output_dir)?;

    // Preamble for main loop
    let sw1 = stopwatch::Stopwatch::new();
    let bar = ProgressBar::new(n_total_files as u64);

    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{total} ({msg})")
            .unwrap(),
    );

    let mut file_count = 0;
    let mut total_delta_file_size = 0 as i64;

    let postfix_plus_extension = get_postfix_plus_extension(do_downsampling);

    //let output_directory = String::from((".");
    // main loop
    //for input_pathname in input_pathnames {

    // // Print the resulting vector of tuples
    // for (first, second) in &result_vec {
    //     println!("({}, {})", first, second);
    // }
    //
    for (input_pathname, output_dir) in &vec_of_tuples {
        file_utils::create_dir_if_not_exists(&output_dir)?;

        //while let Some(csv_filename) = args.next() {

        let parquet_filename = file_utils::replace_file_extension(&input_pathname, &postfix_plus_extension);
        let parquet_filename = file_utils::os_path_join(&output_dir, &parquet_filename);

        //let csv_filename: &str= &csv_filename;
        //let parquet_filename: &str=&parquet_filename;

        let parquet_filename_path = Path::new(&parquet_filename);

        let sw2 = stopwatch::Stopwatch::new();

        let file_exists = parquet_filename_path.exists();

        if (!file_exists) || force {
            csv_to_parquet_utils::convert_csv_file_to_parquet_file(&input_pathname, &parquet_filename, do_downsampling, downsampling_period_sec.into())?;

            let file_size_csv = file_utils::get_file_size(&input_pathname)?;
            let file_size_parquet = file_utils::get_file_size(&parquet_filename)?;

            let file_size_ratio = file_size_parquet as f64 / file_size_csv as f64;
            let delta_file_size = file_size_csv as i64 - file_size_parquet as i64;

            total_delta_file_size += delta_file_size;

            if verbosity > 3 {
                print!("\n");
                print!("Converted {} ({} bytes)", input_pathname, file_size_csv);
                print!("---> {} ({} bytes)", parquet_filename, file_size_parquet);
                print!("\t delta file size: {:.2} Mbytes", delta_file_size as f64 / (1024.0 * 1024.0));
                print!(" reduction ratio={:.2} %", file_size_ratio * 100.0);
                //print!(" dt={}", sw2.elapsed().as_secs());
                print!(" dt={}", sw2.elapsed_formatted_human());
            }
        } else {
            print!("\n Skipped processing on already existing file {}", input_pathname);
        }

        processed_files += 1;
        let fraction_complete = processed_files as f64 / n_total_files as f64;
        bar.set_message(format!("{}/{} {:.1}%", processed_files, n_total_files, fraction_complete * 100.0));
        bar.inc(1);

        file_count += 1;
    }

    bar.finish();

    println!("{} CSV files successfully converted to Parquet", file_count);
    println!(" total_delta_file_size={:.4} Mb", total_delta_file_size / (1024 * 1024));
    println!("Total Time elapsed in is: {:?}", sw1.elapsed_formatted_human());

    Ok(())
}
