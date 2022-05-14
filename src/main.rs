use aho_corasick::AhoCorasick;
use cdc::{Polynom64, Rabin64, RollingHash64, SeparatorIter};
use clap::{App, Arg};
use indicatif::ProgressBar;
use indicatif::{ParallelProgressIterator, ProgressIterator};
use itertools::Itertools;
use log::{info, warn};
use rayon::{iter::IntoParallelIterator, iter::ParallelIterator, slice::ParallelSlice};
use rug::{
    integer::{IsPrime, Order},
    Integer,
};
use simplelog::{ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode};
use std::{collections::HashMap, collections::HashSet, convert::TryInto, fs::read};

const PRIMES_WARNING_THRESHOLD: usize = 1_000;

// ./target/release/prime-finder -f 4 -s 128 core.ssh-agent.15  82.91s user 22.79s system 130% cpu 1:20.71 total
fn finder_sliding_window<'a>(
    pqn_tuples: &'a HashMap<Vec<u8>, (&Integer, &Integer)>,
    file_contents: &[u8],
    prime_size: usize,
) -> Vec<&'a (&'a Integer, &'a Integer)> {
    let bar_size = (file_contents.len() - prime_size).try_into().unwrap();

    let pb = ProgressBar::new(bar_size);
    pb.set_draw_rate(4_000_000);

    info!("Search for composites in file");
    file_contents
        .par_windows(prime_size * 2)
        //.progress_count(bar_size)
        .progress_with(pb)
        .filter_map(|window| pqn_tuples.get(window))
        .collect()
}

fn finder_aho_corasick<'a>(
    pqn_tuples: &'a HashMap<Vec<u8>, (&Integer, &Integer)>,
    file_contents: &[u8],
    prime_size: usize,
) -> Vec<&'a (&'a Integer, &'a Integer)> {
    let composites = pqn_tuples.keys();
    let ac = AhoCorasick::new(composites);

    let bar_size = (file_contents.len() - prime_size).try_into().unwrap();

    let pb = ProgressBar::new(bar_size);
    pb.set_draw_rate(4_000_000);

    info!("Search for composites in file");
    ac.find_iter(file_contents)
        .progress_with(pb)
        .flat_map(|m| pqn_tuples.get(&file_contents[m.start()..m.end()]))
        .collect()
}

// ./target/release/prime-finder -f 4 -s 128 core.ssh-agent.15  70.59s user 11.57s system 137% cpu 59.732 total
fn finder_rabin_karp<'a>(
    pqn_tuples: &'a HashMap<Vec<u8>, (&Integer, &Integer)>,
    file_contents: &[u8],
    prime_size: usize,
) -> Vec<&'a (&'a Integer, &'a Integer)> {
    let bit_size = 8; /*(2*prime_size*8).try_into().unwrap();*/
    let mut hasher = Rabin64::new(bit_size);

    let bar_size = (file_contents.len() - prime_size).try_into().unwrap();

    let pb = ProgressBar::new(bar_size);
    pb.set_draw_rate(4_000_000);

    let rabin_pqn_tuples: HashMap<Polynom64, &(&Integer, &Integer)> = pqn_tuples
        .iter()
        .progress_with(pb)
        .map(|(k, v)| {
            hasher.reset();
            for b in k.iter() {
                hasher.slide(b);
            }
            (*hasher.get_hash(), v)
        })
        .collect();

    SeparatorIter::custom_new(file_contents.iter().cloned(), bit_size, |candidate_hash| {
        rabin_pqn_tuples.contains_key(&candidate_hash)
    })
    .flat_map(|separator| {
        pqn_tuples.get(
            &file_contents[separator.index as usize - 2 * prime_size..(separator.index as usize)],
        )
    })
    .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])?;

    //TODO: add start/end command line arguments
    let matches = App::new("prime-finder")
        .version("0.1")
        .about("Finds RSA primes in files")
        .author("Calle Svensson <calle.svensson@zeta-two.com>")
        .arg(
            Arg::with_name("prime_size")
                .short("s")
                .long("prime-size")
                .value_name("SIZE")
                .help("Sets the size in bytes of the prime numbers to search for")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dump_primes")
                .short("p")
                .long("dump-primes")
                .help("Prints all primes without verifying P*Q"),
        )
        .arg(
            Arg::with_name("null_filter_length")
                .short("f")
                .long("null-filter-length")
                .value_name("LENGTH")
                .help("Filters out any primes with a sequence of null bytes this long")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("FILE")
                .help("Sets the input file to use")
                .required(true)
                .index(1),
        )
        .get_matches();

    let dump_primes = matches.is_present("dump_primes");
    let prime_size = matches
        .value_of("prime_size")
        .unwrap_or("128")
        .parse::<usize>()?;
    let file_name = matches.value_of("FILE").unwrap();

    let null_filter_length = matches
        .value_of("null_filter_length")
        .unwrap_or("2")
        .parse::<usize>()?;
    let file_contents = read(file_name)?;

    let bar_size = (file_contents.len() - prime_size).try_into().unwrap();

    let pb = ProgressBar::new(bar_size);
    pb.set_draw_rate(4);

    info!("Finding candidate primes");
    let probably_primes = file_contents
        .par_windows(prime_size)
        // Discard candidates containing too long streaks of 0 bits
        .progress_with(pb)
        .filter(|window| {
            !window
            .windows(null_filter_length)
            .any(|sub_window| sub_window.iter().all(|&b| b == 0))
        })
        .flat_map(|window| {
            vec![
                Integer::from_digits(window, Order::Msf),
                Integer::from_digits(window, Order::Lsf),
                ]
                .into_par_iter()
            })
        .filter_map(|number| match number.is_probably_prime(20) {
            IsPrime::Yes | IsPrime::Probably => Some(number),
            IsPrime::No => None,
        });
  
        
    let primes: HashSet<_> = probably_primes.collect();
    //let primes: Vec<_> = Vec::with_capacity(1000);
    //probably_primes.collect_into(primes);
    info!("Found {} prime candidates", primes.len());
    if primes.len() > PRIMES_WARNING_THRESHOLD {
        warn!("A large number of candidate primes found. This will consume a large amount of memory. Consider lowering the -f parameter")
    }

    if dump_primes {
        println!("Primes in file");
        for prime in primes {
            println!("{}", prime);
        }
    } else {
        info!("Construct N candidates");
        let pq_tuples = primes.iter().cartesian_product(primes.iter());

        let pb = ProgressBar::new(0);
        pb.set_draw_rate(4);

        let pqn_tuples: HashMap<_, _> = pq_tuples
            .progress_with(pb)
            .filter(|(p, q)| p <= q)
            .flat_map(|(p, q)| {
                vec![
                    (Integer::from(p * q).to_digits::<u8>(Order::Lsf), (p, q)),
                    (Integer::from(p * q).to_digits::<u8>(Order::Msf), (p, q)),
                ]
                .into_iter()
            })
            .collect();

        //let valid_primes = finder_sliding_window(&pqn_tuples, &file_contents, prime_size); // Simple
        //let valid_primes = finder_aho_corasick(&pqn_tuples, &file_contents, prime_size); // Memory expensive
        let valid_primes = finder_rabin_karp(&pqn_tuples, &file_contents, prime_size); // Slightly faster

        println!("Validated primes in file");
        for (p, q) in valid_primes {
            let n = Integer::from(*p * *q);
            println!("P:{} Q:{} N:{}", p, q, n);
        }
    }
    Ok(())
}
