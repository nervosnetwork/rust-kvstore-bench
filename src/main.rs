use clap::{App, Arg, ArgMatches, SubCommand};
use rust_kvstore_bench::lmdb_zero::Store as Lmdb;
use rust_kvstore_bench::rocksdb::Store as Rocksdb;
use rust_kvstore_bench::sled::Store as Sled;
use rust_kvstore_bench::workload::{
    generate_report, generate_workload, run_workload, sample_workload, TaskGenerator, Workload,
    WorkloadResult,
};
use rust_kvstore_bench::KeyValueStore;
use serde_json;
use std::io::{stdin, stdout};

fn main() {
    let matches = App::new("Rust Key Value Store Benchmark")
        .version("0.1.0")
        .subcommand(
            SubCommand::with_name("generate_workload")
                .about("Generates a benchmark workload")
                .arg(Arg::with_name("task_generator").required(true))
                .arg(Arg::with_name("nums_task").required(true)),
        )
        .subcommand(
            SubCommand::with_name("sample_workload")
                .about("Take samples of generated workload")
                .arg(Arg::with_name("task_generator").required(true))
                .arg(Arg::with_name("nums_task").required(true)),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a workload on the database")
                .arg(Arg::with_name("db_type").required(true))
                .arg(Arg::with_name("path").required(true)),
        )
        .subcommand(SubCommand::with_name("report").about("Generate report"))
        .get_matches();

    match matches.subcommand() {
        ("generate_workload", Some(matches)) => execute_generate_workload(&matches),
        ("sample_workload", Some(matches)) => execute_sample_workload(&matches),
        ("run", Some(matches)) => execute_run(&matches),
        ("report", _) => execute_report(),
        _ => {}
    }
}

fn execute_generate_workload(matches: &ArgMatches) {
    let task_generator: TaskGenerator =
        serde_json::from_str(&matches.value_of("task_generator").unwrap()).expect("invalid json");
    let nums_task: usize = matches
        .value_of("nums_task")
        .unwrap()
        .parse()
        .expect("invalid num");
    let workload = generate_workload(&task_generator, nums_task);
    serde_json::to_writer(stdout(), &workload).expect("failed to write workload");
}

fn execute_sample_workload(matches: &ArgMatches) {
    let workload: Workload = serde_json::from_reader(stdin()).expect("failed to read workload");
    let task_generator: TaskGenerator =
        serde_json::from_str(&matches.value_of("task_generator").unwrap()).expect("invalid json");
    let nums_task: usize = matches
        .value_of("nums_task")
        .unwrap()
        .parse()
        .expect("invalid num");
    let workload = sample_workload(&workload, &task_generator, nums_task);
    serde_json::to_writer(stdout(), &workload).expect("failed to write workload");
}

fn execute_run(matches: &ArgMatches) {
    let db_type = matches.value_of("db_type").unwrap();
    let path = matches.value_of("path").unwrap();
    match db_type {
        "rocksdb" => run::<Rocksdb>(&path),
        "lmdb" => run::<Lmdb>(&path),
        "sled" => run::<Sled>(&path),
        _ => {}
    }
}

fn execute_report() {
    let result: WorkloadResult =
        serde_json::from_reader(stdin()).expect("failed to read workload result");
    let report = generate_report(&result);
    serde_json::to_writer_pretty(stdout(), &report).expect("failed to write report");
}

fn run<'a, T: KeyValueStore<'a>>(path: &str) {
    let store = T::new(path);
    let workload: Workload = serde_json::from_reader(stdin()).expect("failed to read workload");
    let result = run_workload(&workload, &store);
    serde_json::to_writer(stdout(), &result).expect("failed to write workload result");
}
