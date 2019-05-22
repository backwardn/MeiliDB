use std::sync::Arc;
use std::ops::RangeInclusive;

use meilidb_core::DocumentId;
use lmdb_zero::error::LmdbResultExt;

use crate::document_attr_key::DocumentAttrKey;
use crate::schema::SchemaAttr;

#[derive(Clone)]
pub struct DocumentsIndex {
    pub first_attr: SchemaAttr,
    pub last_attr: SchemaAttr,
    pub database: Arc<lmdb_zero::Database<'static>>,
}

impl DocumentsIndex {
    pub fn document_field(&self, id: DocumentId, attr: SchemaAttr) -> lmdb_zero::Result<Option<Vec<u8>>> {
        let txn = lmdb_zero::ReadTransaction::new(self.database.env())?;
        let access = txn.access();

        let key = DocumentAttrKey::new(id, attr).to_be_bytes();
        access.get::<_, [u8]>(&self.database, &key).to_opt().map(|s| s.map(Vec::from))
    }

    pub fn set_document_field(&self, id: DocumentId, attr: SchemaAttr, value: &[u8]) -> lmdb_zero::Result<()> {
        let txn = lmdb_zero::WriteTransaction::new(self.database.env())?;

        {
            let mut access = txn.access();
            let key = DocumentAttrKey::new(id, attr).to_be_bytes();
            access.put(&self.database, &key, value, lmdb_zero::put::Flags::empty())?;
        }

        txn.commit()?;

        Ok(())
    }

    pub fn del_document_field(&self, id: DocumentId, attr: SchemaAttr) -> lmdb_zero::Result<()> {
        let txn = lmdb_zero::WriteTransaction::new(self.database.env())?;

        {
            let mut access = txn.access();
            let key = DocumentAttrKey::new(id, attr).to_be_bytes();
            access.del_key(&self.database, &key)?;
        }

        txn.commit()?;

        Ok(())
    }

    pub fn del_all_document_fields(&self, id: DocumentId) -> lmdb_zero::Result<()> {

        let txn = lmdb_zero::WriteTransaction::new(self.database.env())?;

        {
            let mut access = txn.access();
            for attr in self.first_attr.0..self.last_attr.0 {
                let key = DocumentAttrKey::new(id, SchemaAttr(attr)).to_be_bytes();
                access.del_key(&self.database, &key)?;
            }
        }

        txn.commit()?;

        Ok(())
    }

    pub fn document_fields(&self, id: DocumentId) -> DocumentFieldsIter {
        DocumentFieldsIter {
            document_id: id,
            raw_iter: RangeInclusive::new(self.first_attr.0, self.last_attr.0),
            database: &self.database,
        }
    }
}

pub struct DocumentFieldsIter<'a> {
    document_id: DocumentId,
    raw_iter: RangeInclusive<u16>,
    database: &'a lmdb_zero::Database<'static>,
}

impl<'a> Iterator for DocumentFieldsIter<'a> {
    type Item = lmdb_zero::Result<(SchemaAttr, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let txn = match lmdb_zero::ReadTransaction::new(self.database.env()) {
            Ok(txn) => txn,
            Err(e) => return Some(Err(e)),
        };

        // TODO create a transaction for the whole iterator life
        let access = txn.access();

        loop {
            match self.raw_iter.next() {
                Some(attr) => {
                    let attr = SchemaAttr(attr);
                    let key = DocumentAttrKey::new(self.document_id, attr);
                    match access.get::<_, [u8]>(&self.database, &key.to_be_bytes()).to_opt() {
                        Ok(Some(value)) => return Some(Ok((key.attribute, value.to_vec()))),
                        Ok(None) => continue,
                        Err(e) => return Some(Err(e.into()))
                    }
                },
                None => return None,
            }
        }
    }
}
