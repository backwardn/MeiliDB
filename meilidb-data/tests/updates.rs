use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;

use serde_json::json;
use meilidb_data::Database;
use meilidb_schema::{Schema, SchemaBuilder, STORED, INDEXED};

fn simple_schema() -> Schema {
    let mut builder = SchemaBuilder::with_identifier("objectId");
    builder.new_attribute("objectId", STORED | INDEXED);
    builder.new_attribute("title", STORED | INDEXED);
    builder.build()
}

#[test]
fn insert_delete_document() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let database = Database::open(&tmp_dir).unwrap();

    let as_been_updated = Arc::new(AtomicBool::new(false));

    let schema = simple_schema();
    let index = database.create_index("hello", schema).unwrap();

    let as_been_updated_clone = as_been_updated.clone();
    index.set_update_callback(Box::new(move |_| as_been_updated_clone.store(true, Relaxed)));

    let doc1 = json!({ "objectId": 123, "title": "hello" });

    let mut addition = index.documents_addition();
    addition.update_document(&doc1);
    let update_id = addition.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());

    let docs = index.query_builder().query("hello", 0..10).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(index.document(None, docs[0].id).unwrap().as_ref(), Some(&doc1));

    let mut deletion = index.documents_deletion();
    deletion.delete_document(&doc1).unwrap();
    let update_id = deletion.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());

    let docs = index.query_builder().query("hello", 0..10).unwrap();
    assert_eq!(docs.len(), 0);
}

#[test]
fn replace_document() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let database = Database::open(&tmp_dir).unwrap();

    let as_been_updated = Arc::new(AtomicBool::new(false));

    let schema = simple_schema();
    let index = database.create_index("hello", schema).unwrap();

    let as_been_updated_clone = as_been_updated.clone();
    index.set_update_callback(Box::new(move |_| as_been_updated_clone.store(true, Relaxed)));

    let doc1 = json!({ "objectId": 123, "title": "hello" });
    let doc2 = json!({ "objectId": 123, "title": "coucou" });

    let mut addition = index.documents_addition();
    addition.update_document(&doc1);
    let update_id = addition.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());

    let docs = index.query_builder().query("hello", 0..10).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(index.document(None, docs[0].id).unwrap().as_ref(), Some(&doc1));

    let mut deletion = index.documents_addition();
    deletion.update_document(&doc2);
    let update_id = deletion.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());

    let docs = index.query_builder().query("hello", 0..10).unwrap();
    assert_eq!(docs.len(), 0);

    let docs = index.query_builder().query("coucou", 0..10).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(index.document(None, docs[0].id).unwrap().as_ref(), Some(&doc2));
}
