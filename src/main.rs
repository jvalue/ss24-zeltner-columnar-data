#![feature(iterator_try_collect)]
#![feature(array_windows)]

use std::{
    convert::Infallible,
    fmt::Display,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

mod runcfg;

use clap::Parser;
use itertools::iproduct;
use runcfg::Runcfg;

#[derive(Debug, clap::ValueEnum, Clone, Copy)]
enum Backend {
    Typescript,
    Polars,
    PolarsOneBlock,
    PolarsRusqlite,
    PolarsOneBlockRusqlite,
}
impl Backend {
    const VARIANTS: [Self; 5] = [
        Self::Typescript,
        Self::Polars,
        Self::PolarsOneBlock,
        Self::PolarsRusqlite,
        Self::PolarsOneBlockRusqlite,
    ];

    fn _use_polars_flag(&self) -> bool {
        match self {
            Self::Typescript => false,
            Self::Polars
            | Self::PolarsOneBlock
            | Self::PolarsRusqlite
            | Self::PolarsOneBlockRusqlite => true,
        }
    }

    fn example(&self) -> &'static str {
        match self {
            Self::Typescript => "ts",
            Self::Polars => "pl",
            Self::PolarsOneBlock => "plob",
            Self::PolarsRusqlite => "plrs",
            Self::PolarsOneBlockRusqlite => "plobrs",
        }
    }
}

#[derive(Debug, clap::ValueEnum, Clone, Copy)]
enum TransformAmount {
    None,
    Some,
    Many,
}
impl TransformAmount {
    const VARIANTS: [Self; 3] = [Self::None, Self::Some, Self::Many];

    fn example(&self) -> &'static str {
        match self {
            Self::None => "no",
            Self::Some => "so",
            Self::Many => "ma",
        }
    }
}

#[derive(Debug, Parser)]
struct Cli {
    #[arg(short, long)]
    /// Show the models own output
    show_output: bool,

    #[arg(long)]
    /// Hide the models own errors
    hide_errors: bool,

    #[arg(short, long)]
    /// A list of sizes for the dataset input.
    /// If omitted, all will be used.
    datasizes: Vec<usize>,

    #[arg(short, long)]
    /// A list of backends to use.
    /// If omitted, all will be used.
    backends: Vec<Backend>,

    #[arg(short, long)]
    /// A how many transformations to do.
    /// If omitted, all will be used.
    transformations: Vec<TransformAmount>,

    /// The location of the repository
    #[clap(default_value = "/home/jonas/Code/uni/bachelor/ss24-zeltner-columnar-data")]
    repo: PathBuf,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Sqldiff {
    changes: usize,
    inserts: usize,
    deletes: usize,
    unchanged: usize,
}
impl FromStr for Sqldiff {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::default());
        }
        let [changes, inserts, deletes, unchanged] = s
            .split_once(':')
            .unwrap()
            .1
            .trim()
            .split(',')
            .map(|s| s.split_whitespace().next()?.parse::<usize>().ok())
            .try_collect::<Vec<_>>()
            .unwrap()
            .try_into()
            .unwrap();
        Ok(Self {
            changes,
            inserts,
            deletes,
            unchanged,
        })
    }
}
impl Sqldiff {
    pub fn compare(d1: &Path, d2: &Path) -> Self {
        let mut cmd = Command::new("sqldiff");
        cmd.args([d1, d2]).arg("--summary");

        let out = cmd.output().expect("it should work");
        assert!(
            out.stderr.is_empty(),
            "{}",
            String::from_utf8(out.stderr).unwrap()
        );
        if out.stdout.is_empty() {
            return Self::default();
        }
        String::from_utf8(out.stderr)
            .unwrap()
            .parse::<Sqldiff>()
            .unwrap()
    }

    pub fn equal(&self) -> bool {
        self.changes == 0 && self.inserts == 0 && self.deletes == 0
    }
}
impl Display for Sqldiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}~ {}+ {}- {}=]",
            self.changes, self.inserts, self.deletes, self.unchanged
        )
    }
}

fn main() {
    let Cli {
        show_output,
        hide_errors,
        datasizes,
        backends,
        transformations,
        repo,
    } = Cli::parse();

    let datasizes = if datasizes.is_empty() {
        vec![0]
    } else {
        datasizes
    };

    let backends = if backends.is_empty() {
        Backend::VARIANTS.to_vec()
    } else {
        backends
    };

    let transformations = if transformations.is_empty() {
        TransformAmount::VARIANTS.to_vec()
    } else {
        transformations
    };

    iproduct!(datasizes, transformations)
        .map(|(d, t)| {
            let d = (d != 0).then_some(d);
            backends
                .iter()
                .map(|b| Runcfg::new(&repo, t, *b, d, show_output, hide_errors))
                .map(|cfg| {
                    let (duration, success) = cfg.run();
                    (cfg, duration, success)
                })
                .collect::<Vec<_>>()
        })
        .for_each(|cfgs| {
            cfgs.array_windows::<2>()
                .for_each(|[(c1, _, s1), (c2, _, s2)]| {
                    if !s1 || !s2 {
                        return;
                    }
                    let diff = Sqldiff::compare(c1.destination(), c2.destination());
                    if !diff.equal() {
                        eprintln!("{c1} differs from {c2}: {diff}");
                    }
                });
            println!();
            cfgs.into_iter().for_each(|(cfg, dur, suc)| {
                if suc {
                    println!("{cfg} took {} seconds", dur.as_secs_f64());
                } else {
                    eprintln!("FAIL: {cfg}");
                }
            })
        });
}
