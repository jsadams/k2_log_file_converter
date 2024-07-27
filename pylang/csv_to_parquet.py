#!/usr/bin/env python3

import polars as pl
import argparse
import os
from tqdm import tqdm

def convert_csv_to_parquet(csv_file_path, output_dir):
    # Read CSV file
    df = pl.read_csv(csv_file_path)
    
    # Cast all columns to float
    df = df.with_columns([pl.col(col).cast(pl.Float64) for col in df.columns])
    
    # Create output file path
    base_name = os.path.basename(csv_file_path)
    output_file_name = os.path.splitext(base_name)[0] + '.parquet'
    output_file_path = os.path.join(output_dir, output_file_name)
    
    # Write to Parquet
    df.write_parquet(output_file_path)
    print(f"Converted {csv_file_path} to {output_file_path}")

def main():
    parser = argparse.ArgumentParser(description='Convert multiple CSV files to Parquet format.')
    parser.add_argument('csv_files', nargs='+', help='List of CSV files to convert.')
    parser.add_argument('--output_dir', default='.', help='Output directory for Parquet files. Default is current directory.')

    args = parser.parse_args()
    
    for csv_file in tqdm(args.csv_files, desc="Converting CSV files to Parquet"):
        convert_csv_to_parquet(csv_file, args.output_dir)

if __name__ == "__main__":
    main()
