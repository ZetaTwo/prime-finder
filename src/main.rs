#![feature(try_trait)]

use {
    clap::{App, Arg},
    indicatif::{ParallelProgressIterator, ProgressIterator},
    itertools::Itertools,
    rayon::{iter::ParallelIterator, slice::ParallelSlice},
    rug::{
        integer::{IsPrime, Order},
        Integer,
    },
    std::{convert::TryInto, fmt, fs::read},
};

#[derive(Debug, Clone)]
struct FindPrimeError;

type Result<T> = std::result::Result<T, FindPrimeError>;

impl fmt::Display for FindPrimeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error while executing program")
    }
}

//TODO: Specify error message from errors
impl From<std::io::Error> for FindPrimeError {
    fn from(_err: std::io::Error) -> FindPrimeError {
        FindPrimeError {}
    }
}
impl From<std::num::ParseIntError> for FindPrimeError {
    fn from(_err: std::num::ParseIntError) -> FindPrimeError {
        FindPrimeError {}
    }
}
impl From<std::option::NoneError> for FindPrimeError {
    fn from(_err: std::option::NoneError) -> FindPrimeError {
        FindPrimeError {}
    }
}

fn main() -> Result<()> {
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
    let file_name = matches.value_of("FILE")?;

    //let file_contents = &read("core.ssh-agent.15")?[..200000]; // Faster testing
    let null_filter_length = matches
        .value_of("null_filter_length")
        .unwrap_or("2")
        .parse::<usize>()?;
    let file_contents = read(file_name)?; //TODO: input file as argument

    let bar_size = (file_contents.len() - prime_size).try_into().unwrap();

    println!("Finding candidate primes");
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
        }) /*.flat_map( | window | {
            [Integer::from_digits(window, Order::Msf), Integer::from_digits(window, Order::Lsf)].iter() //TODO: Add both LE and BE
        })*/
        .map(|window| Integer::from_digits(window, Order::Lsf))
        .filter_map(|number| match number.is_probably_prime(1) {
            IsPrime::Yes | IsPrime::Probably => Some(number),
            IsPrime::No => None,
        })
        .filter_map(|number| match number.is_probably_prime(20) {
            IsPrime::Yes | IsPrime::Probably => Some(number),
            IsPrime::No => None,
        });

    let primes: Vec<_> = probably_primes.collect();

    if dump_primes {
        primes.iter().for_each(|prime| println!("{}", prime));
    } else {
        println!("Validating candidates");
        // TODO: Warning, this is O(n^2) algorithmic sin
        let valid_primes: Vec<_> = primes
            .iter()
            .cartesian_product(primes.iter())
            .progress()
            .map(|(p, q)| (p, q, Integer::from(p * q)))
            .filter(|(p, q, n)| {
                if q >= p {
                    return false;
                }
                let nbytes = n.to_digits::<u8>(Order::LsfBe);
                match file_contents
                    .windows(nbytes.len())
                    .find(|&window| nbytes == window || nbytes.iter().rev().eq(window))
                {
                    None => false,
                    Some(_) => true,
                }
            })
            .collect();

        println!("Primes in file");
        valid_primes.iter().for_each(|(p, q, n)| {
            println!("P:{} Q:{} N:{}", p, q, n);
        });
    }
    Ok(())
}
