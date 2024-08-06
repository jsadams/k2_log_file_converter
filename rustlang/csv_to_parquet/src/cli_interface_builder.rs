use clap::Arg;
use clap::Command;
//use clap::{Parser, Subcommand};

pub fn process_cli_via_builder_api() -> (std::string::String, bool, i32, Vec<String>, bool, f32) {
    let matches = Command::new("myapp")
        .version("2.0")
        .about("An example CLI app")
        .arg(
            Arg::new("force")
                .short('f')
                .long("force")
                .help("Forces the operation")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("output_dir")
                .short('o')
                .long("output_dir")
                .value_name("DIR")
                .help("Specifies the output directory")
                .value_parser(clap::value_parser!(String))
                .default_value("./output"),
        )
        .arg(
            Arg::new("args")
                .help("Filenames or Directory to process")
                .num_args(1..)
                .allow_hyphen_values(true),
        )
        .arg(
            Arg::new("default_input_extension")
                .short('i')
                .long("default_input_extenion")
                //.value_name("output_dir")
                .help("Specifies the default extension mask for input files in directory mode")
                .value_parser(clap::value_parser!(String))
                .default_value("*.dat"),
        )
        .arg(
            Arg::new("verbosity")
                .short('v')
                .long("verbosity")
                .value_name("LEVEL")
                .help("Set verbosity level (0-10)")
                .default_value("2")
                .value_parser(clap::value_parser!(i32).range(0..=10)),
        )
        .arg(
            Arg::new("downsample_period_sec")
                .short('T')
                .long("downsample_period_sec")
                .value_name("VALUE")
                .help("Set the period over which to downsample")
                .default_value("10.0")
                .value_parser(clap::value_parser!(f32)),

        )
        .arg(
            Arg::new("do_downsampling")
                .short('d')
                .long("downsample")
                .help("Downsample the data")
                .action(clap::ArgAction::SetTrue),
        )

        .get_matches();

    let do_downsampling= matches.get_flag("do_downsampling");
    let downsample_period_sec = matches.get_one::<f32>("downsample_period_sec").unwrap().to_owned();
    let force = matches.get_flag("force");
    let output_dir = matches.get_one::<String>("output_dir").unwrap().to_string();
    let verbosity: i32 = matches.get_one::<i32>("verbosity").unwrap().to_owned();
    //let gain: f32 = matches.get_one::<f32>("gain").unwrap().to_owned();

    //let args: Vec<&str> = matches.get_many::<String>("paths").unwrap().collect();
    let args: Vec<String> = matches
        .get_many::<String>("args")
        .unwrap()
        .map(|s| s.to_string())
        .collect();

    (output_dir, force, verbosity, args, do_downsampling, downsample_period_sec)
}
