use std::{error, fmt};
use crate::serde::SerializerError;

#[derive(Debug)]
pub enum Error {
    SchemaDiffer,
    SchemaMissing,
    WordIndexMissing,
    MissingDocumentId,
    LmdbError(lmdb_zero::Error),
    FstError(fst::Error),
    BincodeError(bincode::Error),
    SerializerError(SerializerError),
}

impl From<lmdb_zero::Error> for Error {
    fn from(error: lmdb_zero::Error) -> Error {
        Error::LmdbError(error)
    }
}

impl From<fst::Error> for Error {
    fn from(error: fst::Error) -> Error {
        Error::FstError(error)
    }
}

impl From<bincode::Error> for Error {
    fn from(error: bincode::Error) -> Error {
        Error::BincodeError(error)
    }
}

impl From<SerializerError> for Error {
    fn from(error: SerializerError) -> Error {
        Error::SerializerError(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match self {
            SchemaDiffer => write!(f, "schemas differ"),
            SchemaMissing => write!(f, "this index does not have a schema"),
            WordIndexMissing => write!(f, "this index does not have a word index"),
            MissingDocumentId => write!(f, "document id is missing"),
            LmdbError(e) => write!(f, "lmdb error; {}", e),
            FstError(e) => write!(f, "fst error; {}", e),
            BincodeError(e) => write!(f, "bincode error; {}", e),
            SerializerError(e) => write!(f, "serializer error; {}", e),
        }
    }
}

impl error::Error for Error { }

