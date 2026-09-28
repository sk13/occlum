#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate elf;
extern crate env_logger;
extern crate regex;
extern crate shellexpand;
extern crate walkdir;
use bom::Bom;
use clap::Parser;
use env_logger::Env;
use util::check_rsync;

mod bom;
mod error;
mod util;

/// copy files described in a bom file to a given dest root dir
#[derive(Debug, Clone, Parser)]
#[command(version)]
struct CopyBomOption {
    /// Set the bom file to copy
    #[arg(short = 'f', long = "file")]
    bom_file: String,
    /// The dest root dir
    #[arg(long = "root")]
    root_dir: String,
    /// Dry run mode
    #[arg(long = "dry-run")]
    dry_run: bool,
    /// Set the paths where to find included bom files
    #[arg(long = "include-dir", num_args = 1..)]
    included_dirs: Vec<String>,
}

impl CopyBomOption {
    fn copy_files(&self) {
        let CopyBomOption {
            bom_file,
            root_dir,
            dry_run,
            included_dirs,
        } = self;
        let image = Bom::from_yaml_file(bom_file);
        image.manage_top_bom(bom_file, root_dir, *dry_run, included_dirs);
    }
}

fn main() {
    // the copy_bom log environmental variable
    let env = Env::new().filter("OCCLUM_LOG_LEVEL");
    env_logger::init_from_env(env);
    check_rsync();

    let copy_bom_option = CopyBomOption::parse();
    copy_bom_option.copy_files();
}
