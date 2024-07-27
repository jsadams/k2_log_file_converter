use std::path::{Path, PathBuf};
use std::io;
use std::fs;

pub fn prepend_output_dir_to_filename(output_dir: &str, old_filename: &str) -> String
{

    let old_filename_as_pathbuf = PathBuf::from(old_filename);
    let output_dir_as_pathbuf = PathBuf::from(output_dir);

    let new_filename_as_pathbuf=output_dir_as_pathbuf.join(old_filename_as_pathbuf);

    return new_filename_as_pathbuf.to_str().unwrap().to_string();


}
pub fn replace_file_extension(csv_filename: &str, new_extension: &str) -> String
{
    /*
        new extension should have a dot in it eg ".parquet"

    */

    let csv_path = PathBuf::from(csv_filename);

    // Extract filename without extension
    let basename = csv_path.file_stem().unwrap().to_str().unwrap();

    // Create output Parquet filename

    //et combined_path = path1.join(path2).join(path3);
    let new_filename = PathBuf::from(format!("{}{}", basename,new_extension));



    return new_filename.to_str().unwrap().to_string();


}


pub fn create_dir_if_not_exists(path: &str) -> io::Result<()> {
    let path = Path::new(path);
    if !path.exists() {
        fs::create_dir_all(path)?;

        println!("Created directory {}",path.to_str().unwrap());
    }
    Ok(())
}

pub fn get_file_size(file_path: &str) -> Result<u64,std::io::Error> {
    let path = Path::new(file_path);

    // Get the metadata of the file
    let metadata = path.metadata()?;

    // Get the file size in bytes
    let file_size_bytes = metadata.len();

    Ok(file_size_bytes)
    // // Convert bytes to megabytes
    // let file_size_mb = file_size_bytes as f64 / (1024.0 * 1024.0);
    //
    // // Print the file size
    // println!("File size: {} bytes ({:.2} MB)", file_size_bytes, file_size_mb);
    //
    // Ok(())
}