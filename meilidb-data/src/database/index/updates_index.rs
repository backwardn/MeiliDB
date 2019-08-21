use std::sync::Arc;
use std::thread;

use sled::Event;

use crate::database::{
    DocumentsAddition, DocumentsDeletion, SynonymsAddition, SynonymsDeletion
};

fn event_is_set(event: &Event) -> bool {
    match event {
        Event::Set(_, _) => true,
        _ => false,
    }
}

#[derive(Clone)]
pub struct UpdatesIndex {
    db: sled::Db,
    tree: Arc<sled::Tree>,
}

impl UpdatesIndex {
    pub fn new(db: sled::Db, tree: Arc<sled::Tree>) -> UpdatesIndex {
        let tree_clone = tree.clone();
        let _handle = thread::spawn(move || {
            loop {
                let mut subscription = tree_clone.watch_prefix(vec![]);

                while let Some((id, update)) = tree_clone.pop_min().unwrap() {
                    // ...
                }

                // this subscription is just used to block
                // the loop until a new update is inserted
                subscription.filter(event_is_set).next();
            }
        });

        UpdatesIndex { db, tree }
    }

    pub fn push_documents_addition(&self, addition: DocumentsAddition) -> sled::Result<u64> {
        let update_id = self.db.generate_id()?;
        unimplemented!()
    }

    pub fn push_documents_deletion(&self, deletion: DocumentsDeletion) -> sled::Result<u64> {
        let update_id = self.db.generate_id()?;
        unimplemented!()
    }

    pub fn push_synonyms_addition(&self, addition: SynonymsAddition) -> sled::Result<u64> {
        let update_id = self.db.generate_id()?;
        unimplemented!()
    }

    pub fn push_synonyms_deletion(&self, deletion: SynonymsDeletion) -> sled::Result<u64> {
        let update_id = self.db.generate_id()?;
        unimplemented!()
    }
}
