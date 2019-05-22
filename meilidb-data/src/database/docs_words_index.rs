use std::sync::Arc;

use lmdb_zero::error::LmdbResultExt;
use meilidb_core::DocumentId;

use super::Error;

#[derive(Clone)]
pub struct DocsWordsIndex(pub Arc<lmdb_zero::Database<'static>>);

impl DocsWordsIndex {
    pub fn doc_words(&self, id: DocumentId) -> Result<Option<fst::Set>, Error> {
        let txn = lmdb_zero::ReadTransaction::new(self.0.env())?;
        let access = txn.access();

        let key = id.0.to_be_bytes();
        match access.get::<_, [u8]>(&self.0, &key).to_opt()? {
            Some(bytes) => {
                let len = bytes.len();
                let value = bytes.into();
                let fst = fst::raw::Fst::from_shared_bytes(value, 0, len)?;
                Ok(Some(fst::Set::from(fst)))
            },
            None => Ok(None)
        }
    }

    pub fn set_doc_words(&self, id: DocumentId, words: &fst::Set) -> Result<(), Error> {
        let txn = lmdb_zero::WriteTransaction::new(self.0.env())?;

        {
            let mut access = txn.access();
            let key = id.0.to_be_bytes();
            access.put(&self.0, &key, words.as_fst().as_bytes(), lmdb_zero::put::Flags::empty())?;
        }

        txn.commit()?;

        Ok(())
    }

    pub fn del_doc_words(&self, id: DocumentId) -> Result<(), Error> {
        let txn = lmdb_zero::WriteTransaction::new(self.0.env())?;

        {
            let mut access = txn.access();
            let key = id.0.to_be_bytes();
            access.del_key(&self.0, &key)?;
        }

        txn.commit()?;

        Ok(())
    }
}
