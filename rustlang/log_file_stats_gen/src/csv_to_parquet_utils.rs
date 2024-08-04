use polars::error::PolarsError;
use polars::frame::DataFrame;
use polars::prelude::{CsvParseOptions, CsvReadOptions, ParquetCompression, ParquetWriter};
use std::fs::OpenOptions;

use polars::chunked_array::ChunkedArray;
use polars::prelude::*;

use crate::polars_conversion_utils;
use crate::downsample_utils;

//use polars::series::IsUtf8;

// use std::fs::File;
// use indicatif::{ProgressBar, ProgressStyle};
// use std::path::PathBuf;
//
// use parquet::file::writer;
// use polars::prelude::*;
// use polars::lazy::frame::LazyFrame;

// fn main() -> Result<(), PolarsError> {
//     // Parse command-line arguments
//     let mut args = std::env::args().skip(1);
//
//     let mut processed_files = 0;
//     let total_files = args.len();
//
//     let bar = ProgressBar::new(total_files as u64);
//     // bar.set_style(
//     //     ProgressStyle::default()
//     //         .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{total} ({msg})"),
//     // );
//
//     while let Some(csv_file) = args.next() {
//         let csv_path = PathBuf::from(csv_file);
//
//         // Extract filename without extension
//         let filename = csv_path.file_stem().unwrap().to_str().unwrap();
//
//         // Create output Parquet filename
//         let output_parquet_path = PathBuf::from(format!("{}.parquet", filename));
//
//         // Read the CSV file
//         // Read the CSV file into a Polars DataFrame
//         let df = LazyFrame::scan_csv(input_csv_file, Default::default())?
//             .collect()?;
//
//         // let df = CsvReader::from_path(csv_path)?
//         //     .has_header(true) // Set header row if your CSV has a header
//         //     .finish()?;
//
//         // Write the DataFrame to Parquet file
//         //    write(&parquet_path, &df)?;
//
//         // Write the DataFrame to a Parquet file
//         let file = File::create(&output_parquet_path)?;
//         ParquetWriter::new(file)
//             .with_compression(ParquetCompression::Snappy)
//             .finish(&df)?;
//
//         processed_files += 1;
//         bar.set_message(format!("Processed {} files", processed_files));
//         bar.inc(1);
//     }
//
//     bar.finish();
//
//     println!("CSV files successfully converted to Parquet!");
//
//     Ok(())
// }

// use polars::prelude::*;
// use std::env;
// use std::fs::File;
// use std::path::Path;
// use indicatif::{ProgressBar, ProgressStyle};
//
// //fn main() -> Result<(), Box<dyn std::error::Error>>
// fn main()
// {
//     // Get the list of filenames from the command line arguments
//     let args: Vec<String> = env::args().collect();
//     if args.len() < 2 {
//         eprintln!("Usage: {} <input_csv_file> [<input_csv_file>...]", args[0]);
//         std::process::exit(1);
//     }
//
//     // Extract the input CSV files from the arguments
//     let input_csv_files = &args[1..];
//
//     // Create a progress bar
//     // let pb = ProgressBar::new(input_csv_files.len() as u64);
//     // pb.set_style(ProgressStyle::default_bar()
//     //     .template("{msg}\n{wide_bar} {pos}/{len}")
//     //     //.progress_chars("##-")
//     //              );
//
//     let pb = ProgressBar::new(input_csv_files.len() as u64);
//
//     // pb.set_style(ProgressStyle::default_bar()
//     //              .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{total} ({msg})"),
//     //  );
//
//
//     for input_csv_file in input_csv_files {
//         // Generate the output Parquet file name by replacing the extension
//         let output_parquet_file = Path::new(input_csv_file)
//             .with_extension("parquet")
//             .to_str()
//             .unwrap()
//             .to_string();
//
//         // Update progress bar message
//         pb.set_message(format!("Processing {}", input_csv_file));
//
//         // Read the CSV file into a Polars DataFrame
//         let df = CsvReader::from_path(input_csv_file)?
//             .has_header(true)
//             .finish()?;
//
//         // Write the DataFrame to a Parquet file
//         let file = File::create(&output_parquet_file)?;
//         ParquetWriter::new(file)
//             .with_compression(ParquetCompression::Snappy)
//             .finish(&df)?;
//
//         // Increment the progress bar
//         pb.inc(1);
//     }
//
//     // Finish the progress bar
//     pb.finish_with_message("All files processed");
//
//     println!("CSV files successfully written to Parquet files");
//
//     Ok(())
// }

//use parquet::file::writer;
use polars::prelude::*;
// //use polars::prelude::CsvReader;
// fn convert_column_to_float(df: &mut DataFrame, column_name: &str) -> Result<(), PolarsError> {
//     // Select the column as a Series
//     let series = df.column(column_name)?;
//
//     // Ensure the column is a Utf8Chunked
//     let utf8_series = series.utf8()?;
//
//     // Convert the Utf8Chunked to a Float64Chunked
//     let float_series = utf8_series
//         .into_iter()
//         .map(|opt_str| opt_str.and_then(|s| s.parse::<f64>().ok()))
//         .collect::<Float64Chunked>();
//
//     // Replace the original column with the new float column
//     df.replace(column_name, float_series.into_series())?;
//
//     Ok(())
// }
//use parquet::file::writer;
//use parquet;
use polars::prelude::*;
//use polars::series::IsUtf8;
//use polars::series::Series;

use polars::datatypes::DataType;
use polars::prelude::*;

// fn convert_columns_to_float_inplace_v1<Float64Array>(df: &mut DataFrame) -> Result<(), PolarsError> {
//     for name in df.get_column_names() {
//         let s = df.column(name)?;
//         if s.dtype() == &DataType::String {
//             let ca = s.utf8()?;
//             let fa = ca.as_str().unwrap().parse::<Float64Array>().map_err(|_| {
//                 PolarsError::ComputeError(format!("Error converting column '{}' to float", name))
//             })?;
//             df.replace_column(name, &fa.into_series())?;
//         }
//     }
//     Ok(())
// }

// fn convert_columns_to_float_inplace<Float64Array>(df: &mut DataFrame) -> Result<(), PolarsError> {
//     for name in df.get_column_names()
//     {
//         let s = df.column(name)?;
//         let fa=s.cast(&DataType::Float64)?;
//         // if s.dtype() == &DataType::String {
//         //     let ca = s.utf8()?;
//         //     let fa = ca.as_str().unwrap().parse::<Float64Array>().map_err(|_| {
//         //         PolarsError::ComputeError(format!("Error converting column '{}' to float", name))
//         //     })?;
//         // }
//         //df.replace_column(?name, fa);
//
//
//         df.replace(name, fa);
//
//     }
//     Ok(())
// }

pub fn read_csv_file_into_df(csv_path: &str) -> Result<DataFrame, PolarsError>
{

    let mut my_parse_options = CsvParseOptions::default();
    my_parse_options.separator = b' ';

    let df_raw_result = CsvReadOptions::default()
        .with_has_header(true)
        .with_ignore_errors(true)
        .with_parse_options(
        CsvParseOptions::default()
        .with_separator(b' ')
        .with_truncate_ragged_lines(true)
        .with_missing_is_null(true), //.with_null_values(999)
        )
        .try_into_reader_with_file_path(Some(csv_path.into()))?
        .finish();

        return df_raw_result;
        // let df = match df_raw_result {
        //     Ok(v) => v,
        //     Err(e) => return Err(e.into()),
        // };



}

pub fn write_df_to_parquet(mut df: DataFrame, output_parquet_path: &str) -> Result<(), PolarsError>
{
    // Write the DataFrame to Parquet file
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&output_parquet_path)?;

    let writer = ParquetWriter::new(file).with_compression(ParquetCompression::Snappy);

    writer.finish(&mut df)?; // Handle potential error here



    Ok(())
}

pub fn convert_csv_file_to_parquet_file(csv_path: &str, output_parquet_path: &str, do_downsampling: bool, downsample_period_sec: i64)
    -> Result<(), PolarsError>
{

    // let mut my_parse_options = CsvParseOptions::default();
    // my_parse_options.separator = b' ';

    // Read the CSV file directly
    //let file = OpenOptions::new().read(true).open(&csv_path)?;
    // let df = CsvReader::from_path(file)
    //     .has_header(true) // Set header row if your CSV has a header
    //     .finish()?;

    // let df_raw_result = CsvReadOptions::default()
    //     .with_has_header(true)
    //     .try_into_reader_with_file_path(Some(csv_path.into()))?
    //     .finish();

    // let df_raw_result = CsvReadOptions::default()
    //     .with_has_header(true)
    //     .with_ignore_errors(true)
    //     .with_parse_options(
    //         CsvParseOptions::default()
    //             .with_separator(b' ')
    //             .with_truncate_ragged_lines(true)
    //             .with_missing_is_null(true), //.with_null_values(999)
    //     )
    //     .try_into_reader_with_file_path(Some(csv_path.into()))?
    //     .finish();

    // let df = match df_raw_result {
    //     Ok(v) => v,
    //     Err(e) => return Err(e.into()),
    // };

    let mut df = read_csv_file_into_df(csv_path)?;

    if do_downsampling
    {
        let columns_to_convert = ["tv_sec", "tv_usec"];

        // Convert specified columns to Int64
        df = polars_conversion_utils::convert_columns_to_int64(&df, &columns_to_convert)?;
        //let df2=convert_i32_to_int64(&df1)?;

        //println!("{:?}", df2);

        df = downsample_utils::downsample_df_based_on_time(df, downsample_period_sec)?;
    }

    write_df_to_parquet(df,output_parquet_path)?;

    // // Write the DataFrame to Parquet file
    // let file = OpenOptions::new()
    //     .create(true)
    //     .write(true)
    //     .open(&output_parquet_path)?;
    //
    // let writer = ParquetWriter::new(file).with_compression(ParquetCompression::Snappy);
    //
    // writer.finish(&mut df)?; // Handle potential error here

    return Ok(());
}

