#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::error::Error;
use std::path::PathBuf;
use std::str;
use std::time::Instant;

use fst::{IntoStreamer, Streamer};
use structopt::StructOpt;

use meilidb_core::Store;
use meilidb_data::Database;

#[derive(Debug, StructOpt)]
pub struct Opt {
    /// The destination where the database must be created
    #[structopt(parse(from_os_str))]
    pub database_path: PathBuf,

    #[structopt(short = "n", long = "number-results", default_value = "10")]
    pub number_results: usize,

    /// Retrieve only words with a frequency over this number.
    #[structopt(long = "frequency-over", default_value = "10000")]
    pub frequency_over: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let _ = env_logger::init();
    let opt = Opt::from_args();

    let start = Instant::now();
    let database = Database::start_default(&opt.database_path)?;
    let index = database.open_index("test")?.unwrap();
    println!("database prepared for you in {:.2?}", start.elapsed());

    let store = index.index_lease();
    let words = store.words()?;

    let mut common_words = BinaryHeap::new();
    let mut total_frequency = 0;

    let mut stream = words.into_stream();
    while let Some(input) = stream.next() {
        let text = str::from_utf8(input)?;

        let doc_indexes = store.word_indexes(input)?;
        let freq = doc_indexes.map_or(0, |i| i.len());

        total_frequency += freq;

        if freq > opt.frequency_over {
            common_words.push((Reverse(freq), text.to_owned()));
        }
    }

    let mut common_words = common_words.into_sorted_vec();
    common_words.truncate(opt.number_results);

    println!("total number of matches: {}", total_frequency);
    for (Reverse(freq), word) in common_words {
        let prec = (freq as f64 / total_frequency as f64) * 100.0;
        println!("{:<8} {:.2}%   {}", freq, prec, word);
    }

    Ok(())
}
