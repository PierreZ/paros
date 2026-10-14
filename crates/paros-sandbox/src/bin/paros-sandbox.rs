//! `paros-sandbox`: read a request `{"config": …, "actions": […]}` on
//! stdin and print the run as JSON, the same bytes the browser gets.

use std::io::Read;

fn main() {
    let mut request = String::new();
    if let Err(error) = std::io::stdin().read_to_string(&mut request) {
        eprintln!("cannot read stdin: {error}");
        std::process::exit(2);
    }
    println!("{}", paros_sandbox::run_json(&request));
}
