pub mod lmdb_zero;
pub mod rocksdb;
pub mod sled;
pub mod workload;

#[derive(Debug)]
pub enum Error {
    DBError(String),
}

pub trait KeyValueStore<'a> {
    type Batch: Batch;
    fn new(path: &str) -> Self;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn exists(&self, key: &[u8]) -> Result<bool, Error>;
    fn batch(&self) -> Result<Self::Batch, Error>;
}

pub trait Batch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn delete(&mut self, key: &[u8]) -> Result<(), Error>;
    fn commit(self) -> Result<(), Error>;
}
