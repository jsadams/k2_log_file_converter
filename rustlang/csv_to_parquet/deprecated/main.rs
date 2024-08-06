// use std::fs::File;
// use indicatif::{ProgressBar, ProgressStyle};
// use std::path::PathBuf;
//
// use parquet::file::writer;
// use polars::prelude::*;
// use polars::lazy::frame::LazyFrame;
//
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

use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

//use parquet::file::writer;
use polars::prelude::CsvReader;
use polars::prelude::*;

use parquet;
use parquet::file::writer;

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

    while let Some(csv_file) = args.next() {
        let csv_path = PathBuf::from(csv_file);

        // Extract filename without extension
        let filename = csv_path.file_stem().unwrap().to_str().unwrap();

        // Create output Parquet filename
        let output_parquet_path = PathBuf::from(format!("{}.parquet", filename));

        // Read the CSV file directly
        //let file = OpenOptions::new().read(true).open(&csv_path)?;
        // let df = CsvReader::from_path(file)
        //     .has_header(true) // Set header row if your CSV has a header
        //     .finish()?;

        let df = CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(csv_path.into()))?
            .finish();

        // Write the DataFrame to Parquet file
        let parquet_file = OpenOptions::new().create(true).write(true).open(&output_parquet_path)?;
        let writer = ParquetWriter::new(parquet_file).with_compression(ParquetCompression::Snappy);

        writer.finish(&df)?; // Handle potential error here

        // // Write the DataFrame to a Parquet file
        // let file = File::create(&output_parquet_file)?;
        // ParquetWriter::new(file)
        //     .with_compression(ParquetCompression::Snappy)
        //     .finish(&df)?;

        println!("{:?}", df);
        processed_files += 1;
        bar.set_message(format!("Processed {} files", processed_files));
        bar.inc(1);
    }

    bar.finish();

    println!("CSV files successfully converted to Parquet!");

    Ok(())
}
