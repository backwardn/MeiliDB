use std::sync::Arc;

use lmdb_zero::error::LmdbResultExt;
use crate::ranked_map::RankedMap;
use crate::schema::Schema;

use super::Error;

#[derive(Clone)]
pub struct MainIndex(pub Arc<lmdb_zero::Database<'static>>);

impl MainIndex {
    pub fn schema(&self) -> Result<Option<Schema>, Error> {
        let txn = lmdb_zero::ReadTransaction::new(self.0.env())?;
        let access = txn.access();

        match access.get::<_, [u8]>(&self.0, "schema").to_opt()? {
            Some(bytes) => {
                let schema = Schema::read_from_bin(bytes)?;
                Ok(Some(schema))
            },
            None => Ok(None),
        }
    }

    pub fn set_schema(&self, schema: &Schema) -> Result<(), Error> {
        let txn = lmdb_zero::WriteTransaction::new(self.0.env())?;

        {
            let mut access = txn.access();
            let mut bytes = Vec::new();
            schema.write_to_bin(&mut bytes)?;
            access.put(&self.0, "schema", &bytes, lmdb_zero::put::Flags::empty())?;
        }

        txn.commit()?;

        Ok(())
    }

    pub fn words_set(&self) -> Result<Option<fst::Set>, Error> {
        let txn = lmdb_zero::ReadTransaction::new(self.0.env())?;
        let access = txn.access();

        match access.get::<_, [u8]>(&self.0, "words").to_opt()? {
            Some(bytes) => {
                let len = bytes.len();
                let value = bytes.into();
                let fst = fst::raw::Fst::from_shared_bytes(value, 0, len)?;
                Ok(Some(fst::Set::from(fst)))
            },
            None => Ok(None),
        }
    }

    pub fn set_words_set(&self, value: &fst::Set) -> Result<(), Error> {
        let txn = lmdb_zero::WriteTransaction::new(self.0.env())?;

        {
            let mut access = txn.access();
            access.put(&self.0, "words", value.as_fst().as_bytes(), lmdb_zero::put::Flags::empty())?;
        }

        txn.commit()?;

        Ok(())
    }

    pub fn ranked_map(&self) -> Result<Option<RankedMap>, Error> {
        let txn = lmdb_zero::ReadTransaction::new(self.0.env())?;
        let access = txn.access();

        match access.get::<_, [u8]>(&self.0, "ranked-map").to_opt()? {
            Some(bytes) => {
                let ranked_map = RankedMap::read_from_bin(bytes.as_ref())?;
                Ok(Some(ranked_map))
            },
            None => Ok(None),
        }
    }

    pub fn set_ranked_map(&self, value: &RankedMap) -> Result<(), Error> {
        let txn = lmdb_zero::WriteTransaction::new(self.0.env())?;

        {
            let mut access = txn.access();
            let mut bytes = Vec::new();
            value.write_to_bin(&mut bytes)?;
            access.put(&self.0, "ranked_map", &bytes, lmdb_zero::put::Flags::empty())?;
        }

        txn.commit()?;

        Ok(())
    }
}
