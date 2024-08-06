#![allow(unused_imports)]
#![allow(dead_code)]

use polars::prelude::*;


// fn main() {
//
//
//     // read from path
//     let df = CsvReader::from_path("iris_csv")?
//         .infer_schema(None)
//         .has_header(true)
//         .finish()?;
//
//     println!("Hello, world!");
// }

mod downsample_df;
mod polars_statistics;
//mod convert_timesamp_to_polars;
mod sample_rate_utils;

use polars::prelude::*;
use chrono::{NaiveDateTime, TimeZone, Utc};

fn main() -> Result<(), PolarsError> {

    //downsample_df::main_downsample_df()
    //polars_statistics::main_test_get_statistics_of_all_numeric_column()

    sample_rate_utils::main_calculate_time_statistics()
}
