use std::collections::hash_map::Entry;
use std::collections::{HashSet, HashMap};
use std::path::Path;
use std::sync::RwLock;
use meilidb_schema::Schema;

mod error;

mod index;
mod main_index;
mod custom_settings_index;
mod docs_words_index;
mod documents_index;
mod synonyms_index;
mod words_index;

mod documents_addition;
mod documents_deletion;
mod synonyms_addition;
mod synonyms_deletion;

pub use self::error::Error;

pub use self::index::Index;
use self::main_index::MainIndex;
pub use self::custom_settings_index::CustomSettingsIndex;
use self::docs_words_index::DocsWordsIndex;
use self::documents_index::DocumentsIndex;
use self::synonyms_index::SynonymsIndex;
use self::words_index::WordsIndex;

use self::documents_addition::DocumentsAddition;
use self::documents_deletion::DocumentsDeletion;
use self::synonyms_addition::SynonymsAddition;
use self::synonyms_deletion::SynonymsDeletion;

fn load_indexes(tree: &sled::Tree) -> Result<HashSet<String>, Error> {
    match tree.get("indexes")? {
        Some(bytes) => Ok(bincode::deserialize(&bytes)?),
        None => Ok(HashSet::new())
    }
}

pub struct Database {
    cache: RwLock<HashMap<String, Index>>,
    inner: sled::Db,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Database, Error> {
        let cache = RwLock::new(HashMap::new());
        let inner = sled::Db::open(path)?;

        let indexes = load_indexes(&inner)?;
        let database = Database { cache, inner };

        for index in indexes {
            database.open_index(&index)?;
        }

        Ok(database)
    }

    pub fn indexes(&self) -> Result<HashSet<String>, Error> {
        load_indexes(&self.inner)
    }

    fn set_indexes(&self, value: &HashSet<String>) -> Result<(), Error> {
        let bytes = bincode::serialize(value)?;
        self.inner.insert("indexes", bytes)?;
        Ok(())
    }

    pub fn open_index(&self, name: &str) -> Result<Option<Index>, Error> {
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
                if !self.indexes()?.contains(name) {
                    return Ok(None)
                }

                let index = Index::new(&self.inner, name)?;
                vacant.insert(index).clone()
            },
        };

        Ok(Some(index))
    }

    pub fn create_index(&self, name: &str, schema: Schema) -> Result<Index, Error> {
        let mut cache = self.cache.write().unwrap();

        let index = match cache.entry(name.to_string()) {
            Entry::Occupied(occupied) => {
                occupied.get().clone()
            },
            Entry::Vacant(vacant) => {
                let index = Index::with_schema(&self.inner, name, schema)?;

                let mut indexes = self.indexes()?;
                indexes.insert(name.to_string());
                self.set_indexes(&indexes)?;

                vacant.insert(index).clone()
            },
        };

        Ok(index)
    }
}
