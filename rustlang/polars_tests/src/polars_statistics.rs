use std::string::String;
use polars::prelude::*;


use polars::prelude::*;

fn get_column_mean(df: &DataFrame, column_name: &str) -> Result<f64, PolarsError> {
    // Attempt to get the column
    let column = df.column(column_name)
        .map_err(|_| PolarsError::ComputeError(format!("Column '{}' not found", column_name).into()))?;

    // Calculate the mean
    let mean = column.mean().ok_or_else(|| PolarsError::ComputeError("Failed to compute mean".into()))?;

    Ok(mean)
}

pub fn main_test_get_column_mean() -> Result<(), PolarsError> {
    // Sample data
    let df = df![
        "A" => &[1.0, 2.0, 3.0, 4.0, 5.0],
        "B" => &[10, 20, 30, 40, 50],
        "C" => &["x", "y", "z", "w", "v"]
    ]?;

    // Get the mean of column "A"
    let column_name = "A";
    match get_column_mean(&df, column_name) {
        Ok(mean) => println!("The mean of column '{}' is {}", column_name, mean),
        Err(e) => println!("Error: {:?}", e),
    }

    Ok(())
}

fn get_means_of_all_numeric_columns(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    // Create a mutable vector to hold the mean series for each column
    let mut mean_series = Vec::new();

    // Iterate over each column in the DataFrame
    for column_name in df.get_column_names() {
        let column = df.column(column_name)?;

        // Check if the column data type is numeric
        match column.dtype() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => {
                // Compute the mean and append to the vector
                let mean_value = column.mean().unwrap_or(f64::NAN);
                mean_series.push(Series::new(&format!("{}_mean", column_name), &[mean_value]));
            },
            _ => {
                // Ignore non-numeric columns
            }
        }
    }

    // Create a DataFrame from the mean series
    let mean_df = DataFrame::new(mean_series)?;

    Ok(mean_df)
}

pub fn main_test_get_means_of_all_numeric_columns() -> Result<(), PolarsError> {
    // Sample data
    let df = df![
        "A" => &[1.0, 2.0, 3.0, 4.0, 5.0],
        "B" => &[10, 20, 30, 40, 50],
        "C" => &["x", "y", "z", "w", "v"]
    ]?;

    // Get the mean of all numeric columns
    let mean_df = get_means_of_all_numeric_columns(&df)?;

    // Print the results
    println!("Means of all numeric columns:\n{}", mean_df);

    Ok(())
}

/////////////////////////////////////////
//
//
// n Polars, you should first convert the AnyValue to f64 directly instead of using to_f64 on Option<AnyValue>.
//
// Here's an updated approach:
//
// Use unwrap_or(f64::NAN): For methods that return Option<f64>, handle None by using unwrap_or(f64::NAN).
// Convert AnyValue Directly: For methods that return Option<AnyValue>, use pattern matching to convert AnyValue to f64.

use polars::prelude::*;
//use polars::prelude::DataType::String;

pub fn get_statistics_of_all_numeric_columns(df: &DataFrame) -> Result<(DataFrame, DataFrame, DataFrame), PolarsError> {
    // Create vectors to hold the mean, min, and max series for each column
    let mut mean_series = Vec::new();
    let mut min_series = Vec::new();
    let mut max_series = Vec::new();

    // use std::collections::HashMap;
    // let mut rust_map = HashMap::new();
    // rust_map.insert("name", "Alice");
    // rust_map.insert("age", "30");

    // Iterate over each column in the DataFrame
    for column_name in df.get_column_names() {

        let column = df.column(column_name)?;

        // Check if the column data type is numeric
        match column.dtype() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => {
                // Compute the mean, min, and max
                let mean_value = column.mean().unwrap_or(f64::NAN);
                let min_value = column.min().unwrap_or(Some(f64::NAN));
                let max_value = column.max().unwrap_or(Some(f64::NAN));


                // Append to the respective vectors
                // mean_series.push(Series::new(&format!("{}_mean", column_name), &[mean_value]));
                // min_series.push(Series::new(&format!("{}_min", column_name), &[min_value]));
                // max_series.push(Series::new(&format!("{}_max", column_name), &[max_value]));

                mean_series.push(Series::new(&format!("{}", column_name), &[mean_value]));
                min_series.push(Series::new(&format!("{}", column_name), &[min_value]));
                max_series.push(Series::new(&format!("{}", column_name), &[max_value]));

                // mean_series.push(Series::new("mean", &[mean_value]));
                // min_series.push(Series::new("min", &[min_value]));
                // max_series.push(Series::new("max", &[max_value]));

            },
            _ => {
                // Ignore non-numeric columns
            }
        }
    }

    // Create DataFrames from the collected series
    let mean_df = DataFrame::new(mean_series)?;
    let min_df = DataFrame::new(min_series)?;
    let max_df = DataFrame::new(max_series)?;

    // // Concatenate the DataFrames along columns
    // let result_df = DataFrame::new(vec![
    //     mean_df.column("mean")?.clone(),
    //     min_df.column("min")?.clone(),
    //     max_df.column("max")?.clone(),
    // ])?;

    Ok ((mean_df, min_df, max_df))
    //Ok(result_df)
}

pub fn main_test_get_statistics_of_all_numeric_column() -> Result<(), PolarsError> {
    // Sample data
    let df = df![
        "A" => &[1.0, 2.0, 3.0, 4.0, 5.0],
        "B" => &[10, 20, 30, 40, 50],
        "C" => &["x", "y", "z", "w", "v"]
    ]?;

    // Get the statistics (mean, min, max) of all numeric columns
    let (mean_df, min_df, max_df) = get_statistics_of_all_numeric_columns(&df)?;

    // Print the results
    println!("means of all numeric columns:\n{}", mean_df);
    println!("max of all numeric columns:\n{}", max_df);
    println!("min of all numeric columns:\n{}", min_df);

    let key=String::from("A");

    let x= mean_df.column(&key)?;
    println!("means of column {}: {}", key, x);
    // println!("max of all numeric columns:\n{}", max_df);
    // println!("min of all numeric columns:\n{}", min_df);


    Ok(())
}
