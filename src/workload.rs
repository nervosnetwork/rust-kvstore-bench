use crate::{Batch, KeyValueStore};
use rand::distributions::Standard;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use statrs::statistics::OrderStatistics;
use std::time::Instant;

pub fn generate_workload(task_generator: &TaskGenerator, nums_task: usize) -> Workload {
    let mut rng = thread_rng();
    Workload(
        (0..nums_task)
            .map(|_| match task_generator {
                TaskGenerator::Get(key_size) => Task::Get(rand_vec(&mut rng, *key_size)),
                TaskGenerator::Exists(key_size) => Task::Exists(rand_vec(&mut rng, *key_size)),
                TaskGenerator::Batch(ogs) => Task::Batch(
                    ogs.iter()
                        .map(|og| match og {
                            BatchOperationGenerator::Put(key_size, value_size) => {
                                BatchOperation::Put(rand_vec(&mut rng, *key_size), *value_size)
                            }
                            BatchOperationGenerator::Delete(key_size) => {
                                BatchOperation::Delete(rand_vec(&mut rng, *key_size))
                            }
                        })
                        .collect(),
                ),
            })
            .collect(),
    )
}

pub fn sample_workload(
    workload: &Workload,
    task_generator: &TaskGenerator,
    nums_task: usize,
) -> Workload {
    let mut rng = thread_rng();
    let mut keys = Vec::new();
    workload.0.iter().for_each(|task| match task {
        Task::Batch(ops) => ops.iter().for_each(|op| match op {
            BatchOperation::Put(key, _) => {
                keys.push(key);
            }
            _ => {}
        }),
        _ => {}
    });
    Workload(
        (0..nums_task)
            .map(|_| match task_generator {
                TaskGenerator::Get(_) => Task::Get(keys.choose(&mut rng).unwrap().to_vec()),
                TaskGenerator::Exists(_) => Task::Exists(keys.choose(&mut rng).unwrap().to_vec()),
                TaskGenerator::Batch(ogs) => Task::Batch(
                    ogs.iter()
                        .map(|og| match og {
                            BatchOperationGenerator::Put(key_size, value_size) => {
                                BatchOperation::Put(rand_vec(&mut rng, *key_size), *value_size)
                            }
                            BatchOperationGenerator::Delete(_) => {
                                BatchOperation::Delete(keys.choose(&mut rng).unwrap().to_vec())
                            }
                        })
                        .collect(),
                ),
            })
            .collect(),
    )
}

pub fn run_workload<'a, T: KeyValueStore<'a>>(workload: &Workload, store: &T) -> WorkloadResult {
    let mut rng = thread_rng();
    WorkloadResult(
        workload
            .0
            .iter()
            .map(|task| match task {
                Task::Get(key) => {
                    let now = Instant::now();
                    store.get(key).expect("store get failed");
                    let elapsed = now.elapsed().as_nanos();
                    TaskResult(TaskType::Get, elapsed)
                }
                Task::Exists(key) => {
                    let now = Instant::now();
                    store.exists(key).expect("store exists failed");
                    let elapsed = now.elapsed().as_nanos();
                    TaskResult(TaskType::Exists, elapsed)
                }
                Task::Batch(operations) => {
                    let mut batch = store.batch().expect("failed to create batch");
                    operations.iter().for_each(|op| match op {
                        BatchOperation::Put(key, value_size) => {
                            let value = rand_vec(&mut rng, *value_size);
                            batch.put(key, &value).expect("batch put failed");
                        }
                        BatchOperation::Delete(key) => {
                            batch.delete(key).expect("batch delete failed");
                        }
                    });
                    let now = Instant::now();
                    batch.commit().expect("failed to commit");
                    let elapsed = now.elapsed().as_nanos();
                    TaskResult(TaskType::Batch, elapsed)
                }
            })
            .collect(),
    )
}

pub fn generate_report(result: &WorkloadResult) -> WorkloadReport {
    let data = &mut result
        .0
        .iter()
        .map(|tr| (tr.1 as f64) / 1000.0)
        .collect::<Vec<_>>()[..];
    WorkloadReport {
        total: data.iter().sum(),
        median: data.median(),
        lower_quartile: data.lower_quartile(),
        upper_quartile: data.upper_quartile(),
    }
}

fn rand_vec<R: Rng>(rng: &mut R, len: usize) -> Vec<u8> {
    rng.sample_iter(&Standard).take(len).collect()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskGenerator {
    // key_size
    Get(usize),
    // key_size
    Exists(usize),
    Batch(Vec<BatchOperationGenerator>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchOperationGenerator {
    // key_size, value_size
    Put(usize, usize),
    // key_size
    Delete(usize),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Workload(pub Vec<Task>);

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Task {
    Get(Vec<u8>),
    Exists(Vec<u8>),
    Batch(Vec<BatchOperation>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchOperation {
    Put(Vec<u8>, usize),
    Delete(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkloadResult(pub Vec<TaskResult>);

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskResult(TaskType, u128);

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    Get,
    Exists,
    Batch,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkloadReport {
    total: f64,
    median: f64,
    lower_quartile: f64,
    upper_quartile: f64,
}
