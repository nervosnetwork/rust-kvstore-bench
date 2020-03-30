use crate::{Batch, Error, KeyValueStore};
use lmdb_zero::{
    self, Database, DatabaseOptions, EnvBuilder, Environment, Ignore, LmdbResultExt,
    ReadTransaction, WriteTransaction,
};
use std::sync::Arc;

pub struct Store {
    env: Arc<Environment>,
    db: Arc<Database<'static>>,
}

impl<'a> KeyValueStore<'a> for Store {
    type Batch = LmdbBatch<'a>;

    fn new(path: &str) -> Self {
        let mut env_builder = EnvBuilder::new().unwrap();
        env_builder.set_maxdbs(1).unwrap();
        // max 1TB
        env_builder
            .set_mapsize(1_099_511_627_776)
            .unwrap_or_else(|e| {
                panic!("Unable to allocate LMDB space: {:?}", e);
            });
        // By default, each write to rocksdb is asynchronous: it returns after pushing the write from the process into the operating system
        // Using NOSYNC here to keep same behaviour with rocksdb.
        let env = unsafe {
            Arc::new(
                env_builder
                    .open(&path, lmdb_zero::open::NOSYNC, 0o600)
                    .unwrap(),
            )
        };
        let db = Arc::new(
            Database::open(
                Arc::clone(&env),
                Some("lmdb"),
                &DatabaseOptions::new(lmdb_zero::db::CREATE),
            )
            .unwrap(),
        );
        Self { env, db }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let tx = ReadTransaction::new(Arc::clone(&self.env))?;
        let access = tx.access();
        access
            .get(&self.db, key)
            .map(|res: &[u8]| res.to_vec())
            .to_opt()
            .map_err(Into::into)
    }

    fn exists(&self, key: &[u8]) -> Result<bool, Error> {
        let tx = ReadTransaction::new(Arc::clone(&self.env))?;
        let access = tx.access();
        let result: lmdb_zero::error::Result<&Ignore> = access.get(&self.db, key);
        result.to_opt().map(|r| r.is_some()).map_err(Into::into)
    }

    fn batch(&self) -> Result<Self::Batch, Error> {
        let tx = WriteTransaction::new(Arc::clone(&self.env))?;
        Ok(Self::Batch {
            db: Arc::clone(&self.db),
            tx,
        })
    }
}

pub struct LmdbBatch<'a> {
    db: Arc<Database<'static>>,
    tx: WriteTransaction<'a>,
}

impl<'a> Batch for LmdbBatch<'a> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.tx
            .access()
            .put(&self.db, key, value, lmdb_zero::put::Flags::empty())?;
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.tx.access().del_key(&self.db, key)?;
        Ok(())
    }

    fn commit(self) -> Result<(), Error> {
        self.tx.commit()?;
        Ok(())
    }
}

impl From<lmdb_zero::error::Error> for Error {
    fn from(e: lmdb_zero::error::Error) -> Error {
        Error::DBError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distributions::Standard;
    use rand::{thread_rng, Rng};
    use tempfile;

    #[test]
    fn put_and_get() {
        let tmp_dir = tempfile::Builder::new()
            .prefix("put_and_get")
            .tempdir()
            .unwrap();
        let store = Store::new(tmp_dir.path().to_str().unwrap());
        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0], &[0, 0, 0]).unwrap();
        batch.put(&[1, 1], &[1, 1, 1]).unwrap();
        let mut rng = thread_rng();
        // max key size 511 bytes
        let key: Vec<u8> = rng.sample_iter(&Standard).take(511).collect();
        let value: Vec<u8> = rng.sample_iter(&Standard).take(1024 * 1024).collect();
        batch.put(&key, &value).unwrap();
        batch.commit().unwrap();

        assert_eq!(Some(vec![0, 0, 0]), store.get(&[0, 0]).unwrap());
        assert_eq!(Some(vec![1, 1, 1]), store.get(&[1, 1]).unwrap());
        assert_eq!(Some(value), store.get(&key).unwrap());
        assert_eq!(None, store.get(&[2, 2]).unwrap());
    }

    #[test]
    fn exists() {
        let tmp_dir = tempfile::Builder::new().prefix("exists").tempdir().unwrap();
        let store = Store::new(tmp_dir.path().to_str().unwrap());
        assert!(!store.exists(&[0, 0]).unwrap());

        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0], &[0, 0, 0]).unwrap();
        batch.commit().unwrap();

        assert!(store.exists(&[0, 0]).unwrap());
    }

    #[test]
    fn delete() {
        let tmp_dir = tempfile::Builder::new().prefix("delete").tempdir().unwrap();
        let store = Store::new(tmp_dir.path().to_str().unwrap());
        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0], &[0, 0, 0]).unwrap();
        batch.commit().unwrap();
        assert_eq!(Some(vec![0, 0, 0]), store.get(&[0, 0]).unwrap());

        let mut batch = store.batch().unwrap();
        batch.delete(&[0, 0]).unwrap();
        batch.commit().unwrap();
        assert_eq!(None, store.get(&[0, 0]).unwrap());
    }
}
