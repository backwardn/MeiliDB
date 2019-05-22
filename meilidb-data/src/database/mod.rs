use std::collections::hash_map::Entry;
use std::collections::{HashSet, HashMap};
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::Schema;

mod custom_settings;
mod docs_words_index;
mod documents_addition;
mod documents_deletion;
mod documents_index;
mod error;
mod index;
mod main_index;
mod raw_index;
mod words_index;

pub use self::error::Error;
pub use self::index::Index;
pub use self::custom_settings::CustomSettings;

use self::docs_words_index::DocsWordsIndex;
use self::documents_addition::DocumentsAddition;
use self::documents_deletion::DocumentsDeletion;
use self::documents_index::DocumentsIndex;
use self::index::InnerIndex;
use self::main_index::MainIndex;
use self::raw_index::RawIndex;
use self::words_index::WordsIndex;

use lmdb_zero::error::LmdbResultExt;

pub struct Database {
    cache: RwLock<HashMap<String, Arc<Index>>>,
    default_db: Arc<lmdb_zero::Database<'static>>,
    environment: Arc<lmdb_zero::Environment>,
}

impl Database {
    pub fn start_default<P: AsRef<Path>>(path: P) -> Result<Database, Error> {
        let path = path.as_ref().to_str().unwrap();
        let environment = unsafe {
            let mut builder = lmdb_zero::EnvBuilder::new()?;
            builder.set_mapsize(10 * 2560 * 4096)?;
            builder.set_maxdbs(2000)?;
            builder.open(path, lmdb_zero::open::Flags::empty(), 0o600)?
        };
        let environment = Arc::new(environment);

        let cache = RwLock::new(HashMap::new());

        let options = lmdb_zero::DatabaseOptions::defaults();
        let default_db = Arc::new(lmdb_zero::Database::open(environment.clone(), None, &options)?);

        Ok(Database { cache, default_db, environment })
    }

    pub fn indexes(&self) -> Result<Option<HashSet<String>>, Error> {
        let txn = lmdb_zero::ReadTransaction::new(self.environment.as_ref())?;
        let access = txn.access();

        let bytes = match access.get(&self.default_db, "indexes").to_opt()? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let indexes = bincode::deserialize(&bytes)?;
        Ok(Some(indexes))
    }

    fn set_indexes(&self, value: &HashSet<String>) -> Result<(), Error> {
        let txn = lmdb_zero::WriteTransaction::new(self.environment.as_ref())?;

        {
            let mut access = txn.access();
            let bytes = bincode::serialize(value)?;
            access.put(&self.default_db, "indexes", &bytes, lmdb_zero::put::Flags::empty())?;
        }

        txn.commit()?;

        Ok(())
    }

    pub fn open_index(&self, name: &str) -> Result<Option<Arc<Index>>, Error> {
        {
            let cache = self.cache.read().unwrap();
            if let Some(index) = cache.get(name).cloned() {
                return Ok(Some(index))
            }
        }

        let mut cache = self.cache.write().unwrap();
        let index = match cache.entry(name.to_string()) {
            Entry::Occupied(occupied) => {
                occupied.get().clone()
            },
            Entry::Vacant(vacant) => {
                if !self.indexes()?.map_or(false, |x| x.contains(name)) {
                    return Ok(None)
                }

                let main = {
                    let options = lmdb_zero::DatabaseOptions::defaults();
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(name), &options)?;
                    let database = Arc::new(database);
                    MainIndex(database)
                };

                let words = {
                    let db_name = format!("{}-words", name);
                    let options = lmdb_zero::DatabaseOptions::defaults();
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(&db_name), &options)?;
                    let database = Arc::new(database);
                    WordsIndex(database)
                };

                let docs_words = {
                    let db_name = format!("{}-docs-words", name);
                    let options = lmdb_zero::DatabaseOptions::defaults();
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(&db_name), &options)?;
                    let database = Arc::new(database);
                    DocsWordsIndex(database)
                };

                let documents = {
                    let db_name = format!("{}-documents", name);
                    let options = lmdb_zero::DatabaseOptions::defaults();
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(&db_name), &options)?;
                    let database = Arc::new(database);

                    let schema = main.schema()?.unwrap();
                    let (first_attr, last_attr) = schema.min_max_attr().unwrap();
                    DocumentsIndex { first_attr, last_attr, database }
                };

                let custom = {
                    let db_name = format!("{}-custom", name);
                    let options = lmdb_zero::DatabaseOptions::defaults();
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(&db_name), &options)?;
                    let database = Arc::new(database);
                    CustomSettings(database)
                };

                let raw_index = RawIndex { main, words, docs_words, documents, custom };
                let index = Index::from_raw(raw_index)?;

                vacant.insert(Arc::new(index)).clone()
            },
        };

        Ok(Some(index))
    }

    pub fn create_index(&self, name: &str, schema: Schema) -> Result<Arc<Index>, Error> {
        let mut cache = self.cache.write().unwrap();

        let index = match cache.entry(name.to_string()) {
            Entry::Occupied(occupied) => {
                occupied.get().clone()
            },
            Entry::Vacant(vacant) => {
                let main = {
                    let options = lmdb_zero::DatabaseOptions::new(lmdb_zero::db::CREATE);
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(name), &options)?;
                    let database = Arc::new(database);
                    MainIndex(database)
                };

                if let Some(prev_schema) = main.schema()? {
                    if prev_schema != schema {
                        return Err(Error::SchemaDiffer)
                    }
                }

                main.set_schema(&schema)?;

                let words = {
                    let db_name = format!("{}-words", name);
                    let options = lmdb_zero::DatabaseOptions::new(lmdb_zero::db::CREATE);
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(&db_name), &options)?;
                    let database = Arc::new(database);
                    WordsIndex(database)
                };

                let docs_words = {
                    let db_name = format!("{}-docs-words", name);
                    let options = lmdb_zero::DatabaseOptions::new(lmdb_zero::db::CREATE);
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(&db_name), &options)?;
                    let database = Arc::new(database);
                    DocsWordsIndex(database)
                };

                let documents = {
                    let db_name = format!("{}-documents", name);
                    let options = lmdb_zero::DatabaseOptions::new(lmdb_zero::db::CREATE);
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(&db_name), &options)?;
                    let database = Arc::new(database);
                    let (first_attr, last_attr) = schema.min_max_attr().unwrap();
                    DocumentsIndex { first_attr, last_attr, database }
                };

                let custom = {
                    let db_name = format!("{}-custom", name);
                    let options = lmdb_zero::DatabaseOptions::new(lmdb_zero::db::CREATE);
                    let database = lmdb_zero::Database::open(self.environment.clone(), Some(&db_name), &options)?;
                    let database = Arc::new(database);
                    CustomSettings(database)
                };

                let mut indexes = self.indexes()?.unwrap_or_else(HashSet::new);
                indexes.insert(name.to_string());
                self.set_indexes(&indexes)?;

                let raw_index = RawIndex { main, words, docs_words, documents, custom };
                let index = Index::from_raw(raw_index)?;

                vacant.insert(Arc::new(index)).clone()
            },
        };

        Ok(index)
    }
}
