#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(dead_code)]

use polars::frame::DataFrame;
use std::string;

mod csv_to_parquet_utils;
mod file_utils;

mod cli_interface_builder;
///mod cli_interface_derive;
mod stopwatch;
mod downsample_utils;
mod polars_conversion_utils;
mod polars_stats_utils;
//mod cli_interface_derive;
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
use clap::builder::Str;
//use std::time::Instant;
//use humantime::format_duration;
use polars::error::PolarsError;
//use polars::frame::DataFrame;
use crate::csv_to_parquet_utils::read_csv_file_into_df;

#[allow(non_snake_case)]
fn main() -> Result<(), PolarsError> {

    // let do_downsampling=true;
    // let downsample_period_sec = 60*2; // 2 minute in seconds

    //(output_dir, force, verbosity, args, do_downsampling, downsample_period_sec)
    let (output_dir, force, verbosity, args, do_downsampling, downsample_period_sec) = cli_interface_builder::process_cli_via_builder_api();

    //let (output_dir, force, verbosity, args)= cli_interface_derive::process_cli_via_derive_api();

    let mut files:Vec<String>=args.clone();

    if args.len() == 1 {
        // if there is only one arg on the command line, check if it is a directory
        //let filepath_0=args[0];
        let first_element: &str = args.first().expect("no element");
        let metadata = fs::metadata(first_element)?;
        if metadata.is_dir() {
            let input_directory=first_element;

            print!("{} is a directory", input_directory);
            let wildcard_pattern="*.dat";

            let dir_and_pattern=file_utils::os_path_join(&input_directory, wildcard_pattern);

            files=file_utils::get_files_matching_pattern(&dir_and_pattern).unwrap().clone();

            //let args=OK(args);

            // if do_downsampling
            // {
            //     output_dir = input_directory.to_owned() + "_parquet_ds";
            //
            //
            // }
            // else {
            //     output_dir = input_directory.to_owned() + "_parquet";
            // }

        }
    }

    // // Accessing specific arguments
    // let force = matches.get_flag("force");
    // let output_dir = matches.get_one::<String>("output_dir").expect("Expected outputdir");

    //let mut args = std::env::args().skip(1);

    // println!("Force: {}", force);
    // println!("Output directory: {}", output_dir);
    // println!("Files to process: {:?}", files);

    // Parse command-line arguments

    let mut processed_files = 0;
    let total_files = files.len();

    //let output_dir = "./out";
//    file_utils::create_dir_if_not_exists(&output_dir)?;

    // Preamble for main loop
    let sw1 = stopwatch::Stopwatch::new();
    let bar = ProgressBar::new(total_files as u64);

    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{total} ({msg})")
            .unwrap(),
    );

    // let mut file_count = 0;
    // let mut total_delta_file_size = 0 as i64;

    // main loop
    for csv_path in files {
        //while let Some(csv_filename) = args.next() {

        // let mut default_postfix = String::from("");
        // let default_extension = String::from(".parquet");
        //
        // if do_downsampling
        // {
        //     default_postfix = String::from("_ds");
        // }
        //
        // //let postfix_plus_extension=default_postfix+default_extension;
        // let postfix_plus_extension=format!("{default_postfix}{default_extension}");
        // let parquet_filename = file_utils::replace_file_extension(&csv_path, &postfix_plus_extension);
        // let parquet_filename = file_utils::os_path_join(&output_dir, &parquet_filename);
        //
        // //let csv_filename: &str= &csv_filename;
        // //let parquet_filename: &str=&parquet_filename;
        //
        // let parquet_filename_path = Path::new(&parquet_filename);
        // let sw2 = stopwatch::Stopwatch::new();
        // let file_exists = parquet_filename_path.exists();

        let df = read_csv_file_into_df(&csv_path)?;

        //polars_stats_utils::print_column_averages(&df);

        let column_name=String::from("T_adr");
        let T1=49.5;
        let T2=50.1;

        //let (mean_series, min_series, max_series, col_names_series) = crate::polars_stats_utils::calculate_statistics(&df, column_name, T1, T2)?;

        let df_f=polars_stats_utils::filter_df_on_key_and_range(&df, &column_name, T1, T2);

        println!("df_f: {:?}", df_f);



        // Print the results
        // println!("Column names: {:?}", col_names_series);
        // println!("Mean values: {:?}", mean_series);
        // println!("Min values: {:?}", min_series);
        // println!("Max values: {:?}", max_series);

        // if (!file_exists) || force {
        //     csv_to_parquet_utils::convert_csv_file_to_parquet_file(
        //         &csv_path,
        //         &parquet_filename,
        //         do_downsampling,
        //         downsample_period_sec.into()
        //     )?;
        //
        //     let file_size_csv = file_utils::get_file_size(&csv_path)?;
        //     let file_size_parquet = file_utils::get_file_size(&parquet_filename)?;
        //
        //     let file_size_ratio = file_size_parquet as f64 / file_size_csv as f64;
        //     let delta_file_size = file_size_csv as i64 - file_size_parquet as i64;
        //
        //     total_delta_file_size += delta_file_size;

            // if verbosity > 3 {
            //     print!("\n");
            //     print!("Converted {} ({} bytes)", csv_path, file_size_csv);
            //     print!("---> {} ({} bytes)", parquet_filename, file_size_parquet);
            //     print!(
            //         "\t delta file size: {:.2} Mbytes",
            //         delta_file_size as f64 / (1024.0 * 1024.0)
            //     );
            //     print!(" reduction ratio={:.2} %", file_size_ratio * 100.0);
            //     //print!(" dt={}", sw2.elapsed().as_secs());
            //     print!(" dt={}", sw2.elapsed_formatted_human());
            // }
        // } else {
        //     print!(
        //         "\n Skipped processing on already existing file {}",
        //         csv_path
        //     );
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

        //file_count += 1;
   // }

    // bar.finish();
    //
    // println!("{} CSV files successfully converted to Parquet", file_count);
    // println!(
    //     " total_delta_file_size={:.4} Mb",
    //     total_delta_file_size / (1024 * 1024)
    // );
    // println!(
    //     "Total Time elapsed in is: {:?}",
    //     sw1.elapsed_formatted_human()
    // );

    Ok(())
}
