use std::sync::Arc;
use std::ops::Deref;

#[derive(Clone)]
pub struct CustomSettings(pub Arc<lmdb_zero::Database<'static>>);

impl Deref for CustomSettings {
    type Target = lmdb_zero::Database<'static>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
