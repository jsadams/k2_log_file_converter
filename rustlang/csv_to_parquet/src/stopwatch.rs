use humantime::{format_duration, FormattedDuration};
use std::time::Duration;
use std::time::Instant;

pub struct Stopwatch {
    start_time: Instant,
}

impl Stopwatch {
    pub fn new() -> Stopwatch {
        Stopwatch { start_time: Instant::now() }
    }

    pub fn elapsed(&self) -> Duration {
        let duration = self.start_time.elapsed();
        return duration;
    }

    // pub fn elapsed_formatted_human(&self) -> FormattedDuration {
    //     return format_duration(self.elapsed());
    // }

    pub fn elapsed_formatted_human(&self) -> String {
        return format_duration(self.elapsed()).to_string();
    }
}

//
// fn main() {
//     let start = Instant::now();
//
//     foo(); // Call the function you want to time.
//
//     let duration = start.elapsed();
//
//     let secs = duration.as_secs();
//     let millis = duration.subsec_millis();
//
//     if secs >= 3600 {
//         let hours = secs / 3600;
//         let minutes = (secs % 3600) / 60;
//         let seconds = secs % 60;
//         println!("Time elapsed in foo() is: {} hours, {} minutes, and {}.{:03} seconds", hours, minutes, seconds, millis);
//     } else if secs >= 60 {
//         let minutes = secs / 60;
//         let seconds = secs % 60;
//         println!("Time elapsed in foo() is: {} minutes and {}.{:03} seconds", minutes, seconds, millis);
//     } else {
//         println!("Time elapsed in foo() is: {}.{:03} seconds", secs, millis);
//     }
// }
