use glob::glob;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
// use glob::glob;
// use std::path::PathBuf;
use std::error::Error;
//
// pub fn os_path_join(A: &str, B: &str) -> String {
//
//     PathBuf::from(A).join(PathBuf::from(B)).to_str().unwrap().to_string()
//
//     // let A_as_pathbuf = PathBuf::from(A);
//     // let B_as_pathbuf = PathBuf::from(B);
//     //
//     // let new_filename_as_pathbuf = output_dir_as_pathbuf.join(old_filename_as_pathbuf);
//     //
//     // return new_filename_as_pathbuf.to_str().unwrap().to_string();
// }

pub fn os_path_join(output_dir: &str, old_filename: &str) -> String {
    let old_filename_as_pathbuf = PathBuf::from(old_filename);
    let output_dir_as_pathbuf = PathBuf::from(output_dir);

    let new_filename_as_pathbuf = output_dir_as_pathbuf.join(old_filename_as_pathbuf);

    return new_filename_as_pathbuf.to_str().unwrap().to_string();
}

pub fn replace_file_extension(csv_filename: &str, new_extension: &str) -> String {
    /*
        new extension should have a dot in it eg ".parquet"

    */

    let csv_path = PathBuf::from(csv_filename);

    // Extract filename without extension
    let basename = csv_path.file_stem().unwrap().to_str().unwrap();

    // Create output Parquet filename

    //et combined_path = path1.join(path2).join(path3);
    let new_filename = PathBuf::from(format!("{}{}", basename, new_extension));

    return new_filename.to_str().unwrap().to_string();
}

pub fn create_dir_if_not_exists(path: &str) -> io::Result<()> {
    let path = Path::new(path);
    if !path.exists() {
        fs::create_dir_all(path)?;

        println!("Created directory {}", path.to_str().unwrap());
    }
    Ok(())
}

pub fn get_file_size(file_path: &str) -> Result<u64, std::io::Error> {
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

// fn get_files_with_extension(dir: &str, extension: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
//     let pattern = PathBuf::from(dir).join(format!("*.{}", extension));
//
//
//     let paths = glob(&pattern.to_str().unwrap())?
//         .filter_map(|ok| ok.ok())
//         .map(|path| path.to_str().map(String::from))
//         .collect();
//     paths
// }
//
// pub fn get_glob_files(dir: &str, wildcard_pattern:  &str) -> Vec<String>
// {
// //-> Result<Vec<String>, Box<dyn std::error::Error>> {
//     //let pattern = format!("{}/*.bar", dir);
//     let pattern = format!("{}/{}", dir,wildcard_pattern);
//
//     // extern crate glob;
//     // use self::glob::glob;
//
//     let files : Vec<PathBuf>= glob("*").collect().unwrap();
//
//     paths_to_strings(files)
//
//     // let paths: Result<Vec<_>, _> = glob(&pattern)?
//     //     .filter_map(|ok| ok.ok())
//     //     .map(|path| path.to_str().map(String::from))
//     //     .collect();
//     // paths
// }
//
// fn paths_to_strings(paths: Vec<PathBuf>) -> Vec<String> {
//     paths.into_iter().map(|path| path.to_str().unwrap().to_string()).collect()
//

//     into_iter() converts the vector into an iterator.
//         map() applies the conversion logic to each element.
//         to_str().unwrap() converts PathBuf to &str.
//         to_string() converts &str to String.
//         collect() gathers the converted strings into a new vector.
//
//}

// fn paths_to_strings(paths: Vec<Result<PathBuf, glob::Error>>) -> Vec<String> {
//     paths
//         .into_iter()
//         .filter_map(|path| path.ok())
//         .map(|path| path.to_str().unwrap().to_string())
//         .collect()
// }


// use glob::glob;
// use std::path::PathBuf;

pub fn get_files_matching_pattern(pattern: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let paths = glob(pattern)?;
    let string_paths: Vec<String> = paths
        .into_iter()
        .filter_map(|path| path.ok())
        .map(|path| path.to_str().unwrap().to_string())
        .collect();
    Ok(string_paths)
}