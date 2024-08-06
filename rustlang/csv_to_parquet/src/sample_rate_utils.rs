use polars::df;
use polars::prelude::*;

pub fn calculate_sample_time_statistics(df: &DataFrame) -> Result<(f64, f64, f64), PolarsError> {
    let tv_sec_key = String::from("t_tv_sec");
    let tv_usec_key = String::from("t_tv_usec");

    // Convert tv_sec and tv_usec to a single timestamp in nanoseconds
    let tv_sec: &Series = df.column(&tv_sec_key)?;
    let tv_usec: &Series = df.column(&tv_usec_key)?;

    // let tv_sec = tv_sec.cast(&DataType::Int64)?;
    // let tv_usec = tv_sec.cast(&DataType::Int64)?;

    let tv_sec = tv_sec.cast(&DataType::Float64)?;
    let tv_usec = tv_usec.cast(&DataType::Float64)?;

    let timestamps_ns: Vec<f64> = tv_sec
        .f64()?
        .into_iter()
        .zip(tv_usec.f64()?.into_iter())
        .map(|(sec_opt, usec_opt)| match (sec_opt, usec_opt) {
            (Some(sec), Some(usec)) => sec * 1_000_000_000.0 + usec * 1_000.0,
            _ => 0.0,
        })
        .collect();

    // Calculate time differences between successive timestamps
    let mut time_diffs_ns = Vec::with_capacity(timestamps_ns.len() - 1);
    for i in 1..timestamps_ns.len() {
        time_diffs_ns.push(timestamps_ns[i] - timestamps_ns[i - 1]);
    }

    // Convert time differences to seconds as f64
    let time_diffs_sec: Vec<f64> = time_diffs_ns.iter().map(|&diff| diff as f64 / 1_000_000_000.0).collect();

    // Create a Series from time_diffs_sec
    let time_diffs_series = Series::new("time_diffs", time_diffs_sec);

    // Compute average, min, and max of time differences
    // let mean_sample_time = time_diffs_sec.iter().copied().sum::<f64>() / time_diffs_sec.len() as f64;
    // let min_sample_time = time_diffs_sec.iter().copied().fold(f64::INFINITY, f64::min);
    // let max_sample_time = time_diffs_sec.iter().copied().fold(f64::NEG_INFINITY, f64::max);

    // Compute average, min, and max of time differences using Polars methods
    let mean_sample_time = time_diffs_series.mean().unwrap_or(f64::NAN);

    //min<T>(&self) -> PolarsResult<Option<T>>
    // https://stackoverflow.com/questions/42917566/what-is-this-question-mark-operator-about
    // It is a postfix operator that unwraps Result<T, E> and Option<T> values.
    //
    //     If applied to Result<T, E>, it unwraps the result and gives you the inner value, propagating the error to the calling function.
    //
    // let number = "42".parse::<i32>()?;
    // println!("{:?}", number); // 42
    // When applied to an Option<T>, it propagates None to the caller, leaving you the content of the Some branch to deal with.
    //
    // let val = Some(42)?;
    // println!("{:?}", val); // 42
    // The ? operator can only be used in a function that returns Result or Option like so:

    // // we can use expect if we want an error message
    let min_sample_time = time_diffs_series.min()?.unwrap_or(f64::NAN);
    let max_sample_time = time_diffs_series.max()?.unwrap_or(f64::NAN);

    // min gives Result<Option>> ? reduces each
    //let min_sample_time = time_diffs_series.min()??;
    //let max_sample_time = time_diffs_series.max()??;

    //let min_sample_time = time_diffs_series.min().unwrap_or(Some(f64::NAN))?;
    //let max_sample_time = time_diffs_series.max().unwrap_or(Some(f64::NAN))?;

    Ok((mean_sample_time, min_sample_time, max_sample_time))
}

pub fn main_calculate_time_statistics() -> Result<(), PolarsError> {
    // Sample data
    let df = df![
        "tv_sec" => &[1.0, 2.0, 3.0, 4.0, 5.0],
        "tv_usec" => &[100000.0, 200000.0, 300000.0, 400000.0, 500000.0]
    ]?;

    // Calculate statistics
    let (mean_time, max_time, min_time) = calculate_sample_time_statistics(&df)?;

    // Print the results
    println!("Mean sample time: {}", mean_time);
    println!("Max sample time: {}", max_time);
    println!("Min sample time: {}", min_time);

    Ok(())
}
