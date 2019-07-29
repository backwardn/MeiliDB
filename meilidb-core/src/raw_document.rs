use std::fmt;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Instant;

use log::info;
use rayon::slice::ParallelSliceMut;
use sdset::SetBuf;
use slice_group_by::GroupBy;

use crate::{TmpMatch, DocumentId, Highlight};

pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;

#[derive(Debug, Clone)]
pub struct RawDocument {
    pub id: DocumentId,

    pub query_index: SmallVec32<u32>,
    pub distance: SmallVec32<u8>,
    pub attribute: SmallVec32<u16>,
    pub word_index: SmallVec32<u16>,
    pub is_exact: SmallVec32<bool>,

    pub highlights: SmallVec32<Highlight>,
}

impl RawDocument {
    pub fn query_index(&self) -> &[u32] {
        &self.query_index
    }

    pub fn distance(&self) -> &[u8] {
        &self.distance
    }

    pub fn attribute(&self) -> &[u16] {
        &self.attribute
    }

    pub fn word_index(&self) -> &[u16] {
        &self.word_index
    }

    pub fn is_exact(&self) -> &[bool] {
        &self.is_exact
    }
}

pub fn raw_documents_from(
    mut matches: Vec<(DocumentId, TmpMatch)>,
    mut highlights: Vec<(DocumentId, Highlight)>,
) -> Vec<RawDocument>
{
    let start = Instant::now();
    matches.par_sort_unstable_by_key(|(id, _)| *id);
    info!("sorting matches took {:.2?}", start.elapsed());

    let start = Instant::now();
    highlights.par_sort_unstable_by_key(|(id, _)| *id);
    info!("sorting highlights took {:.2?}", start.elapsed());

    // TODO check if using highlights is faster (there are fewer)
    let number_of_documents = matches.linear_group_by_key(|(id, _)| *id).count();
    let mut documents = Vec::with_capacity(number_of_documents);

    let matches = matches.linear_group_by_key(|(id, _)| *id);
    let highlights = highlights.linear_group_by_key(|(id, _)| *id);

    for (mgroup, hgroup) in matches.zip(highlights) {
        // check if documents id are the same in these two groups
        debug_assert_eq!(mgroup[0].0, hgroup[0].0);

        // linear_group_by will never yield an empty array
        // so it is safe to do so
        let id = unsafe { mgroup.get_unchecked(0).0 };
        let highlights = SmallVec32::from_iter(hgroup.iter().map(|(_, h)| *h));

        let len = mgroup.len();
        let mut query_index = SmallVec32::with_capacity(len);
        let mut distance = SmallVec32::with_capacity(len);
        let mut attribute = SmallVec32::with_capacity(len);
        let mut word_index = SmallVec32::with_capacity(len);
        let mut is_exact = SmallVec32::with_capacity(len);

        for (_, match_) in mgroup {
            query_index.push(match_.query_index);
            distance.push(match_.distance);
            attribute.push(match_.attribute);
            word_index.push(match_.word_index);
            is_exact.push(match_.is_exact);
        }

        let document = RawDocument {
            id,
            query_index,
            distance,
            attribute,
            word_index,
            is_exact,
            highlights,
        };

        documents.push(document);
    }

    documents
}

pub fn permutations_unstable_by_key<F, K>(len: usize, mut f: F) -> Vec<usize>
where F: FnMut(usize) -> K,
      K: Ord,
{
    let mut permutations: Vec<usize> = (0..len).collect();
    permutations.sort_unstable_by_key(|&i| f(i));
    permutations
}

// this function is O(N) in term of memory but it could be O(1)
// by following this blog post
// https://devblogs.microsoft.com/oldnewthing/20170102-00/?p=95095
pub fn apply_permutations<T: Clone>(permutations: &[usize], vec: &mut SmallVec32<T>) {
    debug_assert_eq!(permutations.len(), vec.len());

    // it is not necessary to restrict items to be Clone,
    // we could ptr::read and, after having copied everything,
    // set_len to 0 and drop the "empty" vec.
    let mut new = SmallVec32::with_capacity(permutations.len());
    for &i in permutations {
        new.push(vec[i].clone());
    }
    std::mem::replace(vec, new);
}
