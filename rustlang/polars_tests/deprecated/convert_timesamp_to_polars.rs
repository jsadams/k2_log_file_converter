use polars::prelude::*;
use chrono::{NaiveDateTime, TimeZone, Utc};
use chrono::DateTime;

fn main_convert_timestamps_to_polars() -> Result<(), PolarsError> {

    // Sample data
    let tv_sec = Series::new("tv_sec", &[1625077765, 1625078765, 1625079765]);
    let tv_usec = Series::new("tv_usec", &[123456, 234567, 345678]);
    let df = DataFrame::new(vec![tv_sec, tv_usec])?;

    // Add the timestamp column
    let timestamps: Vec<String> = df
        .column("tv_sec")?
        .i64()?
        .into_iter()
        .zip(df.column("tv_usec")?.i64()?)
        .map(|(sec, usec)| {
            // Convert Unix time (sec) and microseconds (usec) to a NaiveDateTime
            let naive = NaiveDateTime::from_timestamp(sec.unwrap(), (usec.unwrap() as u32) * 1000);
            // Convert NaiveDateTime to DateTime<Utc>
            Utc.from_utc_datetime(&naive).to_rfc3339()
        })
        .collect();

    // Convert DateTime<Utc> to Series
    let timestamp_series = Series::new("timestamp", &timestamps);

    // Add the new column to the DataFrame
    let mut df = df.clone();
    df.with_column(timestamp_series)?;

    // Print the DataFrame
    println!("{:?}", df);

    Ok(())
}

