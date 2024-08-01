use polars::prelude::*;
use chrono::{NaiveDateTime, TimeZone, Utc, Duration};
use std::collections::HashMap;
use chrono::DateTime;


use crate::polars_conversion_utils;

// fn calculate_mean(df: &DataFrame) -> Result<DataFrame, PolarsError> {
//     let lf = <polars::prelude::DataFrame as Clone>::clone(&(*df)).lazy();
//     let df_out = lf.group_by(["group_col"])
//         .agg([col("value").mean().alias("mean_value")])
//         .collect()?;
//
//     Ok(df_out)
// }

// fn group_by_and_mean_all_cols(df: &DataFrame, group_col: &str) -> Result<DataFrame, PolarsError> {
//     let lf = df.lazy();
//     let out = lf.group_by([col(group_col)])
//         .agg([
//             col("*")
//                 .filter(|s| s.dtype().is_numeric())
//                 .mean()
//                 .suffix("_mean"),
//         ])
//         .collect()?;
//
//     Ok(out)
// }
//
// fn group_by_and_mean_all_cols(df: &DataFrame, group_col: &str) -> Result<DataFrame, PolarsError> {
//     let lf = df.lazy();
//     let out = lf.group_by([col(group_col)])
//         .agg([
//             col("*")
//                 .filter(|s| s.dtype().is_numeric())
//                 .mean()
//                 .suffix("_mean"),
//         ])
//         .collect()?;
//
//     Ok(out)
// }

//
// fn group_by_and_mean_all_cols(df: &DataFrame, group_col: &str) -> Result<DataFrame, PolarsError> {
//     let lf = df.lazy();
//     let out = lf.group_by([col(group_col)])
//         .agg([
//             col("*")
//                 .filter(|col: &Schema| col.dtype().is_numeric()) // Specify type for closure argument
//                 .mean()
//                 .suffix("_mean"),
//         ])
//         .collect()?;
//
//     Ok(out)
// }
//use polars::prelude::*;

fn group_by_and_mean_all_cols(df: &DataFrame, group_col: &str) -> Result<DataFrame, PolarsError> {
    // Initialize an empty vector to collect expressions for aggregation
    let mut agg_exprs: Vec<Expr> = Vec::new();

    // Identify numeric columns and create mean aggregation expressions
    for col_name in df.get_column_names() {
        if col_name != group_col {
            let dtype = df.column(col_name)?.dtype();
            if dtype.is_numeric() {
                // Create an expression for the mean of the column and add an alias
                let expr = col(col_name).mean().alias(&format!("{}_mean", col_name));
                agg_exprs.push(expr);
            }
        }
    }

    // Perform the grouping and aggregation
    let lf = <polars::prelude::DataFrame as Clone>::clone(&df).lazy();
    let out = lf
        .group_by([col(group_col)])
        .agg(agg_exprs)
        .collect()?;

    Ok(out)
}

pub fn downsample_df_based_on_time(mut df: DataFrame, downsample_period_sec: i64) -> Result<DataFrame, PolarsError>
{
    // Create a new column for the rounded timestamps
    let rounded_timestamps: Vec<i64> = df
        .column("tv_sec")?
        .i64()?
        .into_iter()
        .zip(df.column("tv_usec")?.i64()?)
        .map(|(sec, usec)| {
            let sec = sec.unwrap();
            let usec = usec.unwrap();
            let timestamp = DateTime::from_timestamp(sec, (usec as u32) * 1000);
            let rounded_timestamp = (timestamp.expect("Failed to convert from timezone").timestamp() / downsample_period_sec) * downsample_period_sec;
            rounded_timestamp
        })
        .collect();

    // Add the new column to the DataFrame
    let rounded_series = Series::new("rounded_timestamp", rounded_timestamps);
    df.with_column(rounded_series)?;

    let df_out=group_by_and_mean_all_cols(&df, "rounded_timestamp");

    return df_out;

    //     // Group by the rounded timestamp and aggregate the values using mean
    // //let grouped_df = df.group_by("rounded_timestamp")?.mean()?;
    //
    // // Group by the rounded timestamp and aggregate the values using mean
    // let grouped_df = df.group_by(["rounded_timestamp"])?
    //     .select(&["value"])
    //     .mean()?;
    //
    // // // Group by the rounded timestamp and aggregate the values
    // // let grouped_df = df
    // //     .group_by(["rounded_timestamp"])?
    // //     .agg(&[
    // //         col("value").mean().alias("value_mean"),
    // //     ])?;
    //
    // // Print the downsampled DataFrame
    
    
    
    
    
    
    
}
pub fn main_downsample_df() -> Result<(), PolarsError>  {
    // Sample data
    let tv_sec = Series::new("tv_sec", &[1625077765, 1625077825, 1625077885, 1625077945, 1625078005]);
    let tv_usec = Series::new("tv_usec", &[0, 0, 0, 0, 0]);
    let value = Series::new("value", &[10, 20, 30, 40, 50]);
    let df1 = DataFrame::new(vec![tv_sec, tv_usec, value])?;



    println!("{:?}", df1);
    // Specify columns to convert to Int64

    let columns_to_convert = ["tv_sec", "tv_usec"];

    // Convert specified columns to Int64
    let df2 = polars_conversion_utils::convert_columns_to_int64(&df1, &columns_to_convert)?;
    //let df2=convert_i32_to_int64(&df1)?;

    println!("{:?}", df2);
    // Define the downsampling period (e.g., 1 minute)
    let downsample_period_sec = 60; // 1 minute in seconds
    let df3=downsample_df_based_on_time(df2,downsample_period_sec);



    println!("{:?}", df3);

    Ok(())
}
