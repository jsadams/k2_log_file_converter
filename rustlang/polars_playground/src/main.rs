//#[warn(unused_imports)]
#![allow(unused_imports)]
#![allow(dead_code)]

// use std::io::Error;
// use std::fmt::Error;
use std::error::Error;
// use rand::Error;
// use core::fmt::Error;
// use core::error::Error;
use polars::df;
use rand::{thread_rng, Rng};

use polars::prelude::*;
use polars::prelude::*;//{CsvReader, DataType, DataFrame, Series};
//use polars::prelude::{Result as PolarResult};
//use polars_lazy::prelude::*;


use chrono::NaiveDate;
//use ::function_name::named;
use stdext::function_name;
fn series_example_1()
{
    // make a series and print it
    let s = Series::new("a", &[1, 2, 3, 4, 5]);
    print!("\nEntering function {}\n", function_name!());
    print!("{}\n", s);


}

fn data_frame_example_1()
{
    print!("\nEntering function {}\n", function_name!());

    let df: DataFrame = df!(
    "integer" => &[1, 2, 3, 4, 5],
    "date" => &[
        NaiveDate::from_ymd_opt(2025, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 2).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 3).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 4).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 5).unwrap().and_hms_opt(0, 0, 0).unwrap(),
    ],
    "float" => &[4.0, 5.0, 6.0, 7.0, 8.0]
                        ).unwrap();


    println!("{}", df);


}
fn data_frame_example_2() -> Result<(), PolarsError>
{
    print!("\nEntering function {}\n", function_name!());

    let mut arr = [0f64; 5];
        thread_rng().fill(&mut arr);

        let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
        "random" => &arr,
        "groups" => &["A", "A", "B", "C", "B"]).unwrap();


        println!("df={}", &df);

    return Ok(());
    }

// Use fn do_work() -> Result<(), WorkError>.
//
// Result<(), WorkError> means you want the work to be done, but it may fail.
//
// Option<WorkError> means you want to get an error, but it may be absent.
//
// You probably want the work to be done but not to get an error when you write do_work(), so Result<(), WorkError> is the better choice.
//
// I would expect Option<WorkError> only be used in cases like fn get_last_work_error() -> Option<WorkError>.
//

fn data_frame_example_3() -> Result<(), PolarsError>
{

    let df = df! (
    "integers"=> &[1, 2, 3, 4, 5],
    "big_integers"=> &[1, 10000002, 3, 10000004, 10000005],
    "floats"=> &[4.0, 5.0, 6.0, 7.0, 8.0],
    "floats_with_decimal"=> &[4.532, 5.5, 6.5, 7.5, 8.5],
).unwrap();

    println!("{}", &df);

    let out = df
        .clone()
        .lazy()
        .select([
            col("integers")
                .cast(DataType::Float32)
                .alias("integers_as_floats"),
            col("floats")
                .cast(DataType::Int32)
                .alias("floats_as_integers"),
            col("floats_with_decimal")
                .cast(DataType::Int32)
                .alias("floats_with_decimal_as_integers"),
        ])
        .collect()?;
    println!("{}", &out);

    return Ok(());
}




//fn main() -> Result<(), PolarsError>
fn main() -> Result<(), polars::prelude::PolarsError>
{
    series_example_1();
    data_frame_example_1();
    data_frame_example_2()?;
    data_frame_example_3()?;

    //     let mut arr = [0f64; 5];
    //     thread_rng().fill(&mut arr);
    //
    //     let df = df! (
    //     "nrs" => &[Some(1), Some(2), Some(3), None, Some(5)],
    //     "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), None],
    //     "random" => &arr,
    //     "groups" => &["A", "A", "B", "C", "B"])?;
    //
    //
    //     println!("{}", &df);
    //     //println!("Hello, world!");
    // }


    return Ok(());
}
