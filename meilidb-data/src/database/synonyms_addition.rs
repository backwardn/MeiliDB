use std::collections::BTreeMap;
use std::sync::Arc;

use fst::{SetBuilder, set::OpBuilder};
use meilidb_core::normalize_str;
use sdset::SetBuf;

use super::{Error, Index};
use super::index::Cache;

pub struct SynonymsAddition<'a> {
    index: &'a Index,
    synonyms: BTreeMap<String, Vec<String>>,
}

impl<'a> SynonymsAddition<'a> {
    pub fn new(index: &'a Index) -> SynonymsAddition<'a> {
        SynonymsAddition { index, synonyms: BTreeMap::new() }
    }

    pub fn add_synonym<S, T, I>(&mut self, synonym: S, alternatives: I)
    where S: AsRef<str>,
          T: AsRef<str>,
          I: Iterator<Item=T>,
    {
        let synonym = normalize_str(synonym.as_ref());
        let alternatives = alternatives.map(|s| s.as_ref().to_lowercase());
        self.synonyms.entry(synonym).or_insert_with(Vec::new).extend(alternatives);
    }

    pub fn finalize(self) -> Result<u64, Error> {
        self.index.push_synonyms_addition(self.synonyms)
    }
}

pub struct FinalSynonymsAddition<'a> {
    inner: &'a Index,
    synonyms: BTreeMap<String, Vec<String>>,
}

impl<'a> FinalSynonymsAddition<'a> {
    pub fn new(inner: &'a Index) -> FinalSynonymsAddition<'a> {
        FinalSynonymsAddition { inner, synonyms: BTreeMap::new() }
    }

    pub fn from_map(
        inner: &'a Index,
        synonyms: BTreeMap<String, Vec<String>>,
    ) -> FinalSynonymsAddition<'a>
    {
        FinalSynonymsAddition { inner, synonyms }
    }

    pub fn add_synonym<S, T, I>(&mut self, synonym: S, alternatives: I)
    where S: AsRef<str>,
          T: AsRef<str>,
          I: Iterator<Item=T>,
    {
        let synonym = normalize_str(synonym.as_ref());
        let alternatives = alternatives.map(|s| s.as_ref().to_lowercase());
        self.synonyms.entry(synonym).or_insert_with(Vec::new).extend(alternatives);
    }

    pub fn finalize(self) -> Result<(), Error> {
        let ref_index = self.inner.as_ref();
        let synonyms = ref_index.synonyms_index;
        let main = ref_index.main_index;

        let mut synonyms_builder = SetBuilder::memory();

        for (synonym, alternatives) in self.synonyms {
            synonyms_builder.insert(&synonym).unwrap();

            let alternatives = {
                let alternatives = SetBuf::from_dirty(alternatives);
                let mut alternatives_builder = SetBuilder::memory();
                alternatives_builder.extend_iter(alternatives).unwrap();
                alternatives_builder.into_inner().unwrap()
            };
            synonyms.set_alternatives_to(synonym.as_bytes(), alternatives)?;
        }

        let delta_synonyms = synonyms_builder
            .into_inner()
            .and_then(fst::Set::from_bytes)
            .unwrap();

        let synonyms = match main.synonyms_set()? {
            Some(synonyms) => {
                let op = OpBuilder::new()
                    .add(synonyms.stream())
                    .add(delta_synonyms.stream())
                    .r#union();

                let mut synonyms_builder = SetBuilder::memory();
                synonyms_builder.extend_stream(op).unwrap();
                synonyms_builder
                    .into_inner()
                    .and_then(fst::Set::from_bytes)
                    .unwrap()
            },
            None => delta_synonyms,
        };

        main.set_synonyms_set(&synonyms)?;

        // update the "consistent" view of the Index
        let cache = ref_index.cache;
        let words = main.words_set()?.unwrap_or_default();
        let ranked_map = cache.ranked_map.clone();
        let schema = cache.schema.clone();

        let cache = Cache { words, synonyms, schema, ranked_map };
        self.inner.cache.store(Arc::new(cache));

        Ok(())
    }
}
