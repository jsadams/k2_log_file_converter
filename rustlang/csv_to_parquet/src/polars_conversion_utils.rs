use polars::datatypes::DataType;
use polars::error::PolarsError;
use polars::frame::DataFrame;
use polars::prelude::Series;

//
//
//
// let columns_to_convert = ["tv_sec", "tv_usec"];
//
// // Convert specified columns to Int64
// let df2 = polars_conversion_utils::convert_columns_to_int64(&df1, &columns_to_convert)?;
//
//
//
//
pub fn convert_columns_to_int64(df: &DataFrame, columns_to_convert: &[&str]) -> Result<DataFrame, PolarsError> {
    let mut new_columns: Vec<Series> = Vec::new();

    // Iterate over all columns
    for col_name in df.get_column_names() {
        let col = df.column(col_name)?;

        if columns_to_convert.contains(&col_name) {
            // Convert specified columns from Int32 to Int64
            let new_col = col.cast(&DataType::Int64)?.with_name(col_name);
            new_columns.push(new_col);
        } else {
            let new_col = col.clone().with_name(col_name);
            new_columns.push(new_col);
        }
    }

    // Create a new DataFrame with the converted columns
    let new_df = DataFrame::new(new_columns)?;

    Ok(new_df)
}

pub fn convert_all_i32_in_df_to_int64(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let mut new_columns: Vec<Series> = Vec::new();

    // Iterate over all columns
    for col_name in df.get_column_names() {
        let col = df.column(col_name)?;
        let dtype = col.dtype();

        if dtype == &DataType::Int32 {
            // Cast column from Int32 to Int64
            let new_col = col.cast(&DataType::Int64)?.with_name(col_name);
            new_columns.push(new_col);
        } else {
            // Keep column as is if not Int32
            let new_col = col.clone().with_name(col_name);
            new_columns.push(new_col);
        }
    }

    // Create a new DataFrame with the casted columns
    let new_df = DataFrame::new(new_columns)?;

    Ok(new_df)
}
