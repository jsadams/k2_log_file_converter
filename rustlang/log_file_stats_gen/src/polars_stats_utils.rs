use polars::prelude::*;

use polars::error::PolarsError;
use polars::frame::DataFrame;


pub fn print_column_averages(df: &DataFrame) -> Result<(), PolarsError> {
    for col_name in df.get_column_names() {
        let col = df.column(col_name)?;

        // Check if the column is numeric
        if col.dtype().is_numeric() {
            let mean_value = col.mean().unwrap();
            println!("Average of column '{}': {}", col_name, mean_value);
        } else {
            println!("Column '{}' is not numeric and will be skipped.", col_name);
        }
    }

    Ok(())
}

pub fn ensure_less_than<T: PartialOrd>(a: T, b: T) -> (T, T)
{
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
}
#[allow(non_snake_case)]
pub fn filter_df_on_key_and_range(df: &DataFrame, column_name: &str, T1: f64, T2: f64) -> Result<DataFrame, PolarsError> {

    let (T1,T2) = ensure_less_than(T1,T2);


    // Filter the DataFrame based on the T_foo column
    let filtered_df = <polars::prelude::DataFrame as Clone>::clone(&df).lazy()
        .filter(col(column_name).gt(lit(T1)).and(col(column_name).lt(lit(T2))))
        .collect()?;

    return Ok(filtered_df);
}

/*
use polars::prelude::*;
use polars::series::ops::NullBehavior;

pub fn calculate_statistics(df: &DataFrame, column_name: &str, T1: f64, T2: f64) -> Result<(Series, Series, Series), PolarsError> {
    // Filter the DataFrame based on the T_foo column
    let filtered_df = df.lazy()
        .filter(col(column_name).gt(lit(T1)).and(col(column_name).lt(lit(T2))))
        .collect()?;

    // Initialize vectors to store the results
    let mut mean_series = Series::new_empty("mean", &DataType::Float64);
    let mut min_series = Series::new_empty("min", &DataType::Float64);
    let mut max_series = Series::new_empty("max", &DataType::Float64);

    // Iterate over columns and calculate statistics for each
    for col_name in filtered_df.get_column_names() {
        if col_name != column_name {
            let col = filtered_df.column(col_name)?;

            match col.dtype() {
                DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 => {
                    let mean_value = col.mean().unwrap_or(f64::NAN);

                    let min_value = match col.min() {
                        Ok(Some(AnyValue::Float64(val))) => val,
                        Ok(Some(AnyValue::Float32(val))) => val as f64,
                        Ok(Some(AnyValue::Int64(val))) => val as f64,
                        Ok(Some(AnyValue::Int32(val))) => val as f64,
                        _ => f64::NAN,
                    };

                    let max_value = match col.max() {
                        Some(AnyValue::Float64(val)) => val,
                        Some(AnyValue::Float32(val)) => val as f64,
                        Some(AnyValue::Int64(val)) => val as f64,
                        Some(AnyValue::Int32(val)) => val as f64,
                        _ => f64::NAN,
                    };

                    mean_series.append(&Series::new(&col_name, &[mean_value]))?;
                    min_series.append(&Series::new(&col_name, &[min_value]))?;
                    max_series.append(&Series::new(&col_name, &[max_value]))?;
                },
                _ => {
                    // Append dummy values for non-numeric columns
                    mean_series.append(&Series::new(&col_name, &[f64::NAN]))?;
                    min_series.append(&Series::new(&col_name, &[f64::NAN]))?;
                    max_series.append(&Series::new(&col_name, &[f64::NAN]))?;
                },
            }
        }
    }

    Ok((mean_series, min_series, max_series))
}

fn main() -> Result<(), PolarsError> {
    // Sample data
    let df = df![
        "T_foo" => &[1.0, 2.0, 3.0, 4.0, 5.0],
        "a" => &[10_i32, 20, 30, 40, 50],
        "b" => &[100_i32, 200, 300, 400, 500],
        "c" => &["x", "y", "z", "w", "v"]
    ]?;

    // Specify the range for T_foo
    let T1 = 2.0;
    let T2 = 4.0;

    // Calculate statistics
    let (mean_series, min_series, max_series) = calculate_statistics(&df, "T_foo", T1, T2)?;

    // Print the results
    println!("Means: {:?}", mean_series);
    println!("Mins: {:?}", min_series);
    println!("Maxs: {:?}", max_series);

    Ok(())
}
*/

// use polars::prelude::*;
//
// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // Sample DataFrame
//     let df = df!(
//         "T_foo" => &[1, 2, 3, 4, 5],
//         "col1" => &[1.0, 2.0, 3.0, 4.0, 5.0],
//         "col2" => &[1, 2, 3, 4, 5],
//         "col3" => &["a", "b", "c", "d", "e"]
//     )?;
//
//     // Define thresholds
//     let t1 = 2.0;
//     let t2 = 4.0;
//     let (t1, t2) = if t1 < t2 { (t1, t2) } else { (t2, t1) };
//
//     // Filter DataFrame
//     let filtered_df = df.filter(col("T_foo").gt(lit(t1)) & col("T_foo").lt(lit(t2)))?;
//
//     // Calculate mean, min, and max
//     let result = filtered_df
//         .select([
//             exclude("T_foo").mean().suffix("_mean"),
//             exclude("T_foo").min().suffix("_min"),
//             exclude("T_foo").max().suffix("_max"),
//         ])?;
//
//     println!("{:?}", result);
//
//     Ok(())
// }

fn mean_of_numeric_columns(df: &DataFrame) -> Result<Series, PolarsError> {
    //let mut means = Vec::new();

    //let mut means = Series::new_empty("mean", &DataType::Float64);

    for s in df.get_columns() {
        match s.dtype() {
            DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64 |
            DataType::Float32 | DataType::Float64 => {
                let mean_value = s.mean().ok_or("Error");
                //means.push(mean);
                let col_name=s.name();

                means.append(&Series::new(&col_name, &[mean_value]))?;
            },
            _ => {}
        }
    }

    return means;
    //Series::new("mean", &means)
}


// pub fn calculate_statistics(df: &DataFrame) -> Result<(Series, Series, Series), PolarsError> {
//
//     // Initialize vectors to store the results
//     let mut mean_series = Series::new_empty("mean", &DataType::Float64);
//     let mut min_series = Series::new_empty("min", &DataType::Float64);
//     let mut max_series = Series::new_empty("max", &DataType::Float64);
//
//     // Iterate over columns and calculate statistics for each
//     for col_name in df.get_column_names() {
//
//             let col = df.column(col_name)?;
//
//             match col.dtype() {
//                 DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 => {
//                     let mean_value = col.mean().unwrap_or(f64::NAN);
//
//                     let min_value = match col.min() {
//                         Ok(Some(AnyValue::Float64(val))) => val,
//                         Ok(Some(AnyValue::Float32(val))) => val as f64,
//                         Ok(Some(AnyValue::Int64(val))) => val as f64,
//                         Ok(Some(AnyValue::Int32(val))) => val as f64,
//                         _ => f64::NAN,
//                     };
//
//                     let max_value = match col.max() {
//                         Some(AnyValue::Float64(val)) => val,
//                         Some(AnyValue::Float32(val)) => val as f64,
//                         Some(AnyValue::Int64(val)) => val as f64,
//                         Some(AnyValue::Int32(val)) => val as f64,
//                         _ => f64::NAN,
//                     };
//
//                     mean_series.append(&Series::new(&col_name, &[mean_value]))?;
//                     min_series.append(&Series::new(&col_name, &[min_value]))?;
//                     max_series.append(&Series::new(&col_name, &[max_value]))?;
//                 },
//                 _ => {
//                     // Append dummy values for non-numeric columns
//                     mean_series.append(&Series::new(&col_name, &[f64::NAN]))?;
//                     min_series.append(&Series::new(&col_name, &[f64::NAN]))?;
//                     max_series.append(&Series::new(&col_name, &[f64::NAN]))?;
//                 },
//             }
//
//     }
//
//     Ok((mean_series, min_series, max_series))
// }
//
