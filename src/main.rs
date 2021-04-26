use {
    aho_corasick::AhoCorasick,
    clap::{App, Arg},
    indicatif::{ParallelProgressIterator, ProgressIterator},
    itertools::Itertools,
    log::info,
    rayon::{iter::IntoParallelIterator, iter::ParallelIterator, slice::ParallelSlice},
    rug::{
        integer::{IsPrime, Order},
        Integer,
    },
    simplelog::{ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode},
    std::{collections::HashMap, convert::TryInto, fs::read},
};

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

    info!("Finding candidate primes");
    let probably_primes = file_contents
        .par_windows(prime_size)
        .progress_count(bar_size)
        .filter(|window| {
            match window
                .windows(null_filter_length)
                .find(|&sub_window| sub_window.iter().all(|&b| b == 0))
            {
                None => true,
                Some(_) => false,
            }
        })
        .flat_map(|window| {
            vec![
                Integer::from_digits(window, Order::Msf),
                Integer::from_digits(window, Order::Lsf),
            ]
            .into_par_iter()
        })
        .filter_map(|number| match number.is_probably_prime(1) {
            IsPrime::Yes | IsPrime::Probably => Some(number),
            IsPrime::No => None,
        })
        .filter_map(|number| match number.is_probably_prime(20) {
            IsPrime::Yes | IsPrime::Probably => Some(number),
            IsPrime::No => None,
        });

    let primes: Vec<_> = probably_primes.collect();
    info!("Found {} prime candidates", primes.len());

    if dump_primes {
        println!("Primes in file");
        for prime in primes {
            println!("{}", prime);
        }
    } else {
        info!("Construct N candidates");
        //TODO: Warning: consumes a lot of memory
        let pqn_tuples: HashMap<_, _> = primes
            .iter()
            .cartesian_product(primes.iter())
            .filter(|(p, q)| p <= q)
            .flat_map(|(p, q)| {
                vec![
                    (Integer::from(p * q).to_digits::<u8>(Order::Lsf), (p, q)),
                    (Integer::from(p * q).to_digits::<u8>(Order::Msf), (p, q)),
                ]
                .into_iter()
            })
            .collect();

        info!("Building search");
        let ac = AhoCorasick::new(pqn_tuples.keys().progress());
        info!("Validating candidates");
        let valid_primes: Vec<_> = ac
            .find_iter(&file_contents)
            .filter_map(|mat| {
                let nbytes = &file_contents[mat.start()..mat.end()];
                match pqn_tuples.get::<[u8]>(nbytes) {
                    None => None,
                    Some(pq) => Some(pq),
                }
            })
            .collect();

        println!("Validated primes in file");
        for (p, q) in valid_primes {
            let n = Integer::from(*p * *q);
            println!("P:{} Q:{} N:{}", p, q, n);
        }
    }
    Ok(())
}
