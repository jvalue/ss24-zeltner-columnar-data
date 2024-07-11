use std::{
    fmt::Display,
    fs::OpenOptions,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{self, Duration},
};

use crate::{Backend, TransformAmount};

fn source(repo: &Path, n_lines: usize) -> PathBuf {
    let mut target_file = std::env::current_dir().expect("Cwd should be available");
    target_file.push(format!("data/l-{n_lines}.csv"));

    if target_file.exists() {
        return target_file;
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&target_file)
        .unwrap();

    let mut src = repo.to_path_buf();
    src.push("example/data/brewey_data_all.csv");
    let lines = format!("--lines={}", n_lines + 1);
    Command::new("head")
        .arg(lines)
        .arg(src)
        .stdout(file)
        .output()
        .expect("Could not execute command");
    target_file
    // HINT: returns something like example/data/l.csv
}

fn example(transformations: TransformAmount, backend: Backend) -> String {
    "example:".to_string() + &[backend.example(), transformations.example()].join("-")
    // HINT: returns something like example:plobrs-ma
}

fn destination(transformations: TransformAmount, backend: Backend, n_lines: usize) -> PathBuf {
    let mut dst = std::env::current_dir().expect("Cwd should be available");
    let sqlite_name = [
        backend.example(),
        transformations.example(),
        &n_lines.to_string(),
    ]
    .join("-")
        + ".sqlite";
    dst.push(sqlite_name);
    dst
    // HINT: returns something like plobrs-ma-l.sqlite
}

#[derive(Debug, Clone)]
pub struct Runcfg {
    repo: PathBuf,
    transformations: TransformAmount,
    backend: Backend,
    n_lines: usize,
    example: String,
    source: PathBuf,
    destination: PathBuf,
    show_output: bool,
}
impl Runcfg {
    pub fn new(
        repo: &Path,
        transformations: TransformAmount,
        backend: Backend,
        n_lines: usize,
        show_output: bool,
    ) -> Self {
        Self {
            repo: repo.to_path_buf(),
            transformations,
            backend,
            n_lines,
            example: example(transformations, backend),
            source: source(repo, n_lines),
            destination: destination(transformations, backend, n_lines),
            show_output,
        }
    }

    pub fn example(&self) -> &str {
        &self.example
    }

    pub fn source(&self) -> &Path {
        &self.source
    }

    pub fn destination(&self) -> &Path {
        &self.destination
    }

    pub fn repo(&self) -> &PathBuf {
        &self.repo
    }

    pub fn transformations(&self) -> TransformAmount {
        self.transformations
    }

    pub fn backend(&self) -> Backend {
        self.backend
    }

    pub fn n_lines(&self) -> usize {
        self.n_lines
    }

    pub fn show_output(&self) -> bool {
        self.show_output
    }

    pub fn run(&self) -> Duration {
        let src = format!("SRC={}", self.source.to_string_lossy());
        let dst = format!("DST={}", self.destination.to_string_lossy());
        let mut cmd = Command::new("npm");
        cmd.current_dir(&self.repo)
            .args(["run", &self.example, "--", "-e", &src, "-e", &dst]);
        if !self.show_output {
            cmd.stdout(Stdio::null());
        }

        // NOTE: https://stackoverflow.com/a/40953863
        let now = time::Instant::now();
        let mut child = cmd.spawn().expect("it should work");
        child.wait().expect("no interrupts");
        now.elapsed()
    }
}

impl Display for Runcfg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.backend().example(),
            self.transformations().example(),
            self.n_lines()
        )
    }
}
