use std::path::PathBuf;
use std::{env, fs};

use dirs;
use std;

// Cargo automatically sets several environment variables when running build scripts, including OUT_DIR and CARGO_BIN_NAME. These variables are crucial for build scripts to interact with the build process correctly.
//
// OUT_DIR
// Purpose: Specifies the output directory for build script generated files. This is where you can place any files that need to be included in your final binary.
// Usage: You can use OUT_DIR to write generated code, assets, or other data that your build script produces. Cargo will automatically include these files in the final build process.
// CARGO_BIN_NAME
// Purpose: Contains the name of the binary being built. This is useful for determining the output filename when copying or renaming files.
// Usage: You can use CARGO_BIN_NAME to construct file paths or to identify the specific binary being built.
// These environment variables are set by Cargo itself during the build process and are available to your build script without any manual configuration.
//
// In summary, Cargo provides these variables to facilitate communication between the build script and the main build process. By using these variables correctly, you can effectively manage build artifacts and customize the build process.
//
fn main() {
    // Get the target directory
    let out_dir = env::var("OUT_DIR").expect("Issuing retrieving OUT_DIR env variable");
    let target_dir = PathBuf::from(out_dir);

    // Determine the binary name
    let binary_name = env::var("CARGO_BIN_NAME").expect("Issuing retrieving CARGO_BIN_NAME env variable");
    let binary_path = target_dir.join(binary_name.clone());

    // Get the user's home directory
    let home_dir = dirs::home_dir().expect("Could not get home directory");
    let bin_dir = home_dir.join("bin");

    // Create the bin directory if it doesn't exist
    fs::create_dir_all(&bin_dir).expect("Could not create bin directory");

    // Copy the binary to the bin directory
    fs::copy(&binary_path, bin_dir.join(&binary_name)).expect("Could not copy binary");
}
