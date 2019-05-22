use std::sync::Arc;

use lmdb_zero::error::LmdbResultExt;
use meilidb_core::DocIndex;
use sdset::{Set, SetBuf};
use zerocopy::{LayoutVerified, AsBytes};

#[derive(Clone)]
pub struct WordsIndex(pub Arc<lmdb_zero::Database<'static>>);

impl WordsIndex {
    pub fn doc_indexes(&self, word: &[u8]) -> lmdb_zero::Result<Option<SetBuf<DocIndex>>> {
        let txn = lmdb_zero::ReadTransaction::new(self.0.env())?;
        let access = txn.access();

        match access.get::<_, [u8]>(&self.0, word).to_opt()? {
            Some(bytes) => {
                let layout = LayoutVerified::new_slice(bytes).expect("invalid layout");
                let slice = layout.into_slice();
                let setbuf = SetBuf::new_unchecked(slice.to_vec());
                Ok(Some(setbuf))
            },
            None => Ok(None),
        }
    }

    pub fn set_doc_indexes(&self, word: &[u8], set: &Set<DocIndex>) -> lmdb_zero::Result<()> {
        let txn = lmdb_zero::WriteTransaction::new(self.0.env())?;

        {
            let mut access = txn.access();
            access.put(&self.0, word, set.as_bytes(), lmdb_zero::put::Flags::empty())?;
        }

        txn.commit()?;

        Ok(())
    }

    pub fn del_doc_indexes(&self, word: &[u8]) -> lmdb_zero::Result<()> {
        let txn = lmdb_zero::WriteTransaction::new(self.0.env())?;

        {
            let mut access = txn.access();
            access.del_key(&self.0, word)?;
        }

        txn.commit()?;

        Ok(())
    }
}
