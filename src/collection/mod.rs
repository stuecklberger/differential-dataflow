//! A time-varying multiset of records.
//!
//! The core datastructure representing a dataset in differential dataflow is `collection::Trace`.
//! It represents a multiset that can be indexed by partially ordered times, and is stored in a
//! compressed incremental representation.

pub mod least_upper_bound;
pub mod lookup;
pub mod trace;
pub mod compact;
pub mod tier;
pub mod count;
pub mod robin_hood;

pub use collection::lookup::Lookup;
pub use collection::least_upper_bound::LeastUpperBound;
pub use collection::least_upper_bound::close_under_lub;
pub use collection::trace::Trace;
pub use collection::trace::Offset;
