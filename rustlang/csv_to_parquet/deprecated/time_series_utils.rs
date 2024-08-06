use chrono::FixedOffset;
use chrono::NaiveDateTime;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use std::str::FromStr;

// let mut df = /* your DataFrame */;
// create_timestamp_column(&mut df, "tv_sec", "tv_usec", Some("America/Los_Angeles"))?;
//
// fn create_timestamp_column(df: &mut DataFrame, tv_sec_col: &str, tv_usec_col: &str, timezone: Option<&str>) -> Result<(), PolarsError> {
//
//     let tv_sec = df.column(tv_sec_col)?.cast(&DataType::Int64)?;
//     let tv_usec = df.column(tv_usec_col)?.cast(&DataType::UInt32)?;
//
//     tz=Utc.
//     let timestamps = match timezone {
//         Some(tz) => {
//             let tz = Utc.with_timezone(&TimeZone::from_str(tz)?);
//             let timestamps = (tv_sec * 1_000_000 + tv_usec)
//                 .cast(DataType::Int64)?
//                 .into_series()
//                 .datetime(TimeUnit::Microseconds, Some(tz))?;
//             timestamps
//         }
//         None => {
//             let timestamps = (tv_sec * 1_000_000 + tv_usec)
//                 .cast(DataType::Int64)?
//                 .into_series()
//                 .datetime(TimeUnit::Microseconds, None)?;
//             timestamps
//         }
//     };
//
//     df.with_column(timestamps.alias("timestamp"))?;
//
//     Ok(())
// }

// fn convert_to_series(timestamps: &DatetimeChunked) -> Series {
//     Series::new("timestamp", timestamps.into_iter().map(|v| v.value()).collect())
// }

// fn convert_to_series(timestamps: &DatetimeChunked) -> Series {
//     Series::new(
//         "timestamp",
//         timestamps.iter().map(|opt_value| opt_value.map(|v| v.value())).collect(),
//     )
// }
//
// fn convert_to_series(timestamps: &DatetimeChunked) -> Series {
//     Series::new(
//         "timestamp",
//         timestamps.iter().map(|opt_value| opt_value.unwrap_or_default()).collect(),
//     )
// }

fn convert_to_series<Tz>(timestamps: &DatetimeChunked) -> Series {
    Series::new(
        "timestamp",
        timestamps.iter().map(|opt_value| opt_value.unwrap_or_default()).collect::<Vec<DateTime<Tz>>>(),
    )
}
fn create_timestamp_column(df: &mut DataFrame, tv_sec_col: &str, tv_usec_col: &str, timezone: Option<&str>) -> Result<(), PolarsError> {
    let tv_sec = df.column(tv_sec_col)?.cast(&DataType::Int64)?;
    let tv_usec = df.column(tv_usec_col)?.cast(&DataType::UInt32)?;

    let timestamps = (tv_sec * 1_000_000 + tv_usec).cast(&DataType::Int64)?.into_series();

    let timestamps = match timezone {
        Some(tz_str) => {
            // let offset = FixedOffset::from_str(tz_str)?;
            // timestamps.datetime(TimeUnit::Microseconds, Some(offset))?

            let offset = FixedOffset::from_str(tz_str).unwrap();
            timestamps.datetime()?
        }
        None => timestamps.datetime()?,
    };

    let timestamps_as_series = convert_to_series(timestamps);

    df.with_column("timestamp", timestamps_as_series)?;

    // The error message indicates that the type &Logical<DatetimeType, Int64Type> (a reference to a logical column) doesn't directly implement the IntoSeries trait, which is required by the with_column method in Polars.
    //
    //    Here's how you can address this issue:
    //
    //    Use cast to convert to ChunkedArray:
    //    Since the with_column method expects a type that implements IntoSeries, you can use cast to convert the Logical column to a ChunkedArray<DatetimeType, Int64Type>, which does implement IntoSeries.
    //
    //    Here's the updated code:
    //
    //df.with_column(timestamps.alias("timestamp"))?;
    //    df.with_column(timestamps, "timestamp")?; // Use with_column_name

    // Use cast to convert to ChunkedArray
    // let timestamps = timestamps.cast(DataType::Datetime)?;
    // df.with_column("timestamp", timestamps)?;

    //    df.with_column("timestamp", timestamps.as_series())?;

    //df.with_column("timestamp", Series::new("timestamp", &[timestamps]).into_series())?;

    Ok(())
}
