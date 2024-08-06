
use polars::prelude::*;

// fn get_every_nth_sample(df: &DataFrame, n: usize) -> Result<DataFrame, PolarsError> {
//     let len = df.height();
//     let indices: Vec<usize> = (0..len).step_by(n).collect();
//     let nth_sampled_df = df.select(&indices)?;
//
//     Ok(nth_sampled_df)
// }

pub fn get_every_nth_sample(df: &DataFrame, n: usize) -> Result<DataFrame, PolarsError> {
    let len = df.height();
    let mask: BooleanChunked = (0..len)
        .map(|i| i % n == 0)
        .collect::<BooleanChunked>()
        .into();

    let nth_sampled_df = df.filter(&mask)?;

    Ok(nth_sampled_df)
}
pub fn main_get_every_nth_sample() -> Result<(), PolarsError> {
    // Sample data
    let df = df![
        "A" => &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "B" => &[10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    ]?;

    // Get every 3rd sample
    let nth_sampled_df = get_every_nth_sample(&df, 3)?;

    // Print the results
    println!("Every 3rd sample:\n{}", nth_sampled_df);

    Ok(())
}
