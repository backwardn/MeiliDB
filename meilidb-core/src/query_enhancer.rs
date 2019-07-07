use std::ops::Range;
use std::iter::FromIterator;
use intervaltree::{IntervalTree, Element};

/// Return `true` if the specified range can accept the given replacements words.
/// Returns `false` if the replacements words are already present in the original query
/// or if there is fewer replacement words than the range to replace.
//
//
// ## Ignored because already present in original
//
//     new york city subway
//     -------- ^^^^
//   /          \
//  [new york city]
//
//
// ## Ignored because smaller than the original
//
//   new york city subway
//   -------------
//   \          /
//    [new york]
//
//
// ## Accepted because bigger than the original
//
//        NYC subway
//        ---
//       /   \
//      /     \
//     /       \
//    /         \
//   /           \
//  [new york city]
//
pub fn rewrite_range_with<S, T>(query: &[S], range: Range<usize>, words: &[T]) -> bool
where S: AsRef<str>,
      T: AsRef<str>,
{
    if words.len() <= range.len() {
        // there is fewer or equal replacement words
        // than there is already in the replaced range
        return false
    }

    // retrieve the part to rewrite but with the length
    // of the replacement part
    let original = query.iter().skip(range.start).take(words.len());

    // check if the original query doesn't already contain
    // the replacement words
    !original.map(AsRef::as_ref).eq(words.iter().map(AsRef::as_ref))
}

pub struct QueryEnhancerBuilder<'a, S> {
    query: &'a [S],
    origins: Vec<usize>,
    real_to_origin: Vec<(Range<usize>, usize)>,
}

impl<S: AsRef<str>> QueryEnhancerBuilder<'_, S> {
    pub fn new(query: &[S]) -> QueryEnhancerBuilder<S> {
        // we initialize origins query indices based on their positions
        let origins: Vec<_> = (0..query.len()).collect();
        let real_to_origin = origins.iter().map(|&o| (o..o+1, o)).collect();

        QueryEnhancerBuilder { query, origins, real_to_origin }
    }

    /// Update the final real to origin query indices mapping.
    ///
    /// `range` is the original words range that this `replacement` words replace
    /// and `real` is the first real query index of these replacement words.
    pub fn declare<T>(&mut self, range: Range<usize>, real: usize, replacement: &[T])
    where T: AsRef<str>,
    {
        // check if the range of original words
        // can be rewritten with the replacement words
        if rewrite_range_with(self.query, range.clone(), replacement) {

            // this range can be replaced so we need to
            // modify the origins accordingly
            let offset = replacement.len() - range.len();
            for (o, r) in self.origins.iter_mut().enumerate().skip(range.end) {
                // we add the offset but don't forget to remove
                // the already possibly added offsets
                *r += offset.saturating_sub(*r - o);
            }
        }

        // we need to store the real number and origins relations
        // this way it will be possible to know by how many
        // we need to pad real query indices
        let real_range = real..real + replacement.len();
        self.real_to_origin.push((real_range, range.start));
    }

    pub fn build(self) -> QueryEnhancer {
        QueryEnhancer {
            origins: self.origins,
            real_to_origin: IntervalTree::from_iter(self.real_to_origin),
        }
    }
}

pub struct QueryEnhancer {
    origins: Vec<usize>,
    // TODO the ranges do not overlap so it is possible
    //      to replace this by a simpler type
    real_to_origin: IntervalTree<usize, usize>,
}

impl QueryEnhancer {
    /// Returns the query indices to use to replace this real query index.
    pub fn replacement(&self, real: usize) -> Range<usize> {

        // query the interval tree with the real query index
        let mut iter = self.real_to_origin.query_point(real);
        let element = iter.next().expect("real has never been declared");
        debug_assert!(iter.next().is_none(), "there must not be another range containing a real");

        let Element { range, value } = element.clone();
        let origin = value;

        // if `real` is the end bound of the range
        if range.end == real {
            // compute the padding and return the range with the padding
            let n = real - range.start;
            let padding = self.origins[origin] - origin;

            debug_assert!(n <= range.end);
            debug_assert!(padding <= range.len());
            debug_assert!(n <= padding);

            Range { start: origin + n, end: origin + padding + 1 }

        } else {
            // just return the origin along with
            // the real position of the word
            let n = real - range.start;
            let origin = self.origins[origin];

            debug_assert!(n <= range.end);

            Range { start: origin + n, end: origin + n + 1 }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_unmodified() {
        let query = ["new", "york", "city", "subway"];
        //             0       1       2        3
        let mut builder = QueryEnhancerBuilder::new(&query);

        // new york = new york city
        builder.declare(0..2, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // new
        assert_eq!(enhancer.replacement(1), 1..2); // york
        assert_eq!(enhancer.replacement(2), 2..3); // city
        assert_eq!(enhancer.replacement(3), 3..4); // subway
        assert_eq!(enhancer.replacement(4), 0..1); // new
        assert_eq!(enhancer.replacement(5), 1..2); // york
        assert_eq!(enhancer.replacement(6), 2..3); // city
    }

    #[test]
    fn simple_growing() {
        let query = ["new", "york", "subway"];
        //             0       1        2
        let mut builder = QueryEnhancerBuilder::new(&query);

        // new york = new york city
        builder.declare(0..2, 3, &["new", "york", "city"]);
        //                    ^      3       4       5

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // new
        assert_eq!(enhancer.replacement(1), 1..2); // york
        assert_eq!(enhancer.replacement(2), 3..4); // subway
        assert_eq!(enhancer.replacement(3), 0..1); // new
        assert_eq!(enhancer.replacement(4), 1..2); // york
        assert_eq!(enhancer.replacement(5), 2..3); // city
    }

    #[test]
    fn bigger_growing() {
        let query = ["NYC", "subway"];
        //             0        1
        let mut builder = QueryEnhancerBuilder::new(&query);

        // NYC = new york city
        builder.declare(0..1, 2, &["new", "york", "city"]);
        //                    ^      2       3       4

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // NYC
        assert_eq!(enhancer.replacement(1), 3..4); // subway
        assert_eq!(enhancer.replacement(2), 0..1); // new
        assert_eq!(enhancer.replacement(3), 1..2); // york
        assert_eq!(enhancer.replacement(4), 2..3); // city
    }

    #[test]
    fn middle_query_growing() {
        let query = ["great", "awesome", "NYC", "subway"];
        //              0         1        2        3
        let mut builder = QueryEnhancerBuilder::new(&query);

        // NYC = new york city
        builder.declare(2..3, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // great
        assert_eq!(enhancer.replacement(1), 1..2); // awesome
        assert_eq!(enhancer.replacement(2), 2..3); // NYC
        assert_eq!(enhancer.replacement(3), 5..6); // subway
        assert_eq!(enhancer.replacement(4), 2..3); // new
        assert_eq!(enhancer.replacement(5), 3..4); // york
        assert_eq!(enhancer.replacement(6), 4..5); // city
    }

    #[test]
    fn multiple_growings() {
        let query = ["great", "awesome", "NYC", "subway"];
        //              0         1        2        3
        let mut builder = QueryEnhancerBuilder::new(&query);

        // NYC = new york city
        builder.declare(2..3, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        // subway = underground train
        builder.declare(3..4, 7, &["underground", "train"]);
        //                    ^          7           8

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // great
        assert_eq!(enhancer.replacement(1), 1..2); // awesome
        assert_eq!(enhancer.replacement(2), 2..3); // NYC
        assert_eq!(enhancer.replacement(3), 5..6); // subway
        assert_eq!(enhancer.replacement(4), 2..3); // new
        assert_eq!(enhancer.replacement(5), 3..4); // york
        assert_eq!(enhancer.replacement(6), 4..5); // city
        assert_eq!(enhancer.replacement(7), 5..6); // underground
        assert_eq!(enhancer.replacement(8), 6..7); // train
    }

    #[test]
    fn multiple_probable_growings() {
        let query = ["great", "awesome", "NYC", "subway"];
        //              0         1        2        3
        let mut builder = QueryEnhancerBuilder::new(&query);

        // NYC = new york city
        builder.declare(2..3, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        // subway = underground train
        builder.declare(3..4, 7, &["underground", "train"]);
        //                    ^          7           8

        // great awesome = good
        builder.declare(0..2, 9, &["good"]);
        //                    ^       9

        // awesome NYC = NY
        builder.declare(1..3, 10, &["NY"]);
        //                    ^^     10

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0),  0..1); // great
        assert_eq!(enhancer.replacement(1),  1..2); // awesome
        assert_eq!(enhancer.replacement(2),  2..3); // NYC
        assert_eq!(enhancer.replacement(3),  5..6); // subway
        assert_eq!(enhancer.replacement(4),  2..3); // new
        assert_eq!(enhancer.replacement(5),  3..4); // york
        assert_eq!(enhancer.replacement(6),  4..5); // city
        assert_eq!(enhancer.replacement(7),  5..6); // underground
        assert_eq!(enhancer.replacement(8),  6..7); // train
        assert_eq!(enhancer.replacement(9),  0..1); // good
        assert_eq!(enhancer.replacement(10), 1..2); // NY
    }
}
