use std::path::PathBuf;
use glob::glob;
//use rstest::fixture;
//
// #[fixture]
// fn test_dir() -> PathBuf {
//     // Create a temporary directory for testing
//     let tempdir = tempfile::tempdir().unwrap();
//     tempdir.path().to_path_buf()
// }
//
// #[fixture]
// fn create_test_files(test_dir: &PathBuf) {
//     // Create some test files
//     let file1 = test_dir.join("file1.bar");
//     let file2 = test_dir.join("file2.baz");
//     std::fs::write(&file1, b"content").unwrap();
//     std::fs::write(&file2, b"content").unwrap();
// }
//
// #[test]
// fn test_get_files_with_extension() {
//
//     test_dir=PathBuf(".out");
//     let files = get_files_with_extension(test_dir.to_str().unwrap(), "bar").unwrap();
//     assert_eq!(files.len(), 1);
//     assert_eq!(files[0], test_dir.join("file1.bar").to_str().unwrap().to_string());
// }

use crate::file_utils;
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_file_extension() {
        let csv_filename="foo.csv";
        let new_extension = ".dat";
        let new_filename=file_utils::replace_file_extension(csv_filename, new_extension);
        assert_eq!("foo.dat", new_filename);
    }
}