#![allow(unused_imports)]
#![allow(dead_code)]

mod file_utils;
mod csv_to_parquet_utils;


//use std::fs::{File, OpenOptions};
use std::fs::OpenOptions;
//use indicatif::{ProgressBar, ProgressStyle};
use indicatif::ProgressBar;
use std::path::PathBuf;


use polars::error::PolarsError;

fn main() -> Result<(), PolarsError> {
    // Parse command-line arguments
    let mut args = std::env::args().skip(1);

    let mut processed_files = 0;
    let total_files = args.len();

    let bar = ProgressBar::new(total_files as u64);
    // bar.set_style(
    //     ProgressStyle::default()
    //         .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{total} ({msg})"),
    // );

    let output_dir="./out";
    file_utils::create_dir_if_not_exists(output_dir)?;

    let mut file_count=0;
    let mut total_delta_file_size=0 as i64;

    while let Some(csv_filename) = args.next() {


        let parquet_filename= file_utils::replace_file_extension(&csv_filename, ".parquet");
        let parquet_filename= file_utils::prepend_output_dir_to_filename(output_dir,&parquet_filename);

        //let csv_filename: &str= &csv_filename;
        //let parquet_filename: &str=&parquet_filename;

        csv_to_parquet_utils::convert_csv_file_to_parquet_file(&csv_filename, &parquet_filename)?;


        let file_size_csv=file_utils::get_file_size(&csv_filename)?;
        let file_size_parquet=file_utils::get_file_size(&parquet_filename)?;

        let file_size_ratio=file_size_parquet as f64 /file_size_csv as f64;
        let delta_file_size =file_size_csv as i64 -file_size_parquet as i64;

        total_delta_file_size += delta_file_size;

        print!("\n");
        print!("Converted {} ({} bytes)",csv_filename,file_size_csv);
        print!("---> {} ({} bytes)",parquet_filename,file_size_parquet);
        print!("\t delta file size: {} bytes", delta_file_size);
        print!(" ratio={:.2}" ,file_size_ratio);


        processed_files += 1;
        bar.set_message(format!("Processed {} files", processed_files));
        bar.inc(1);

        file_count += 1;

    }

    bar.finish();

    println!("{} CSV files successfully converted to Parquet", file_count);
    println!(" total_delta_file_size={:.4} Mb", total_delta_file_size/(1024*1024));

    Ok(())
}
