//! Rendezvous (HRW) hashing over a node set with a pluggable hasher.
//!
//! # Example
//!
//! This example demonstrates creating a Rendezvous set, adding and removing nodes, and picking a node for a key.
//!
//! ```
//! use hrw::Rendezvous;
//! let mut r = Rendezvous::from_nodes(["A", "B"]);
//! assert_eq!(r.len(), 2);
//! r.add_node("C");
//! assert_eq!(r.len(), 3);
//! r.remove_node(&"B");
//! assert_eq!(r.len(), 2);
//! let chosen = r.pick_top(&"my-key");
//! assert!(chosen.is_some());
//! ```
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};

#[derive(Clone, Debug)]
pub struct Rendezvous<N, S = RandomState> {
    nodes: Vec<N>,
    build: S,
}

impl<N> Rendezvous<N, RandomState>
where
    N: Hash + Eq,
{
    /// Default: uses `RandomState` (SipHash-like) for decent DoS-resistance.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            build: RandomState::new(),
        }
    }

    pub fn from_nodes(nodes: impl IntoIterator<Item = N>) -> Self {
        Self {
            nodes: nodes.into_iter().collect(),
            build: RandomState::new(),
        }
    }
}

impl<N> Default for Rendezvous<N, RandomState>
where
    N: Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<N, S> Rendezvous<N, S>
where
    N: Hash + Eq + PartialOrd + std::fmt::Debug,
    S: BuildHasher + Clone,
{
    /// Construct with a custom hasher builder (e.g., ahash::RandomState).
    pub fn from_nodes_and_hasher(nodes: impl IntoIterator<Item = N>, build: S) -> Self {
        Self {
            nodes: nodes.into_iter().collect(),
            build,
        }
    }

    pub fn add_node(&mut self, node: N) -> bool {
        if !self.nodes.iter().any(|n| n == &node) {
            self.nodes.push(node);
            true
        } else {
            false
        }
    }

    pub fn remove_node(&mut self, node: &N) -> bool {
        if let Some(i) = self.nodes.iter().position(|n| n == node) {
            self.nodes.swap_remove(i);
            true
        } else {
            false
        }
    }

    #[inline]
    fn hrw_score<K: Hash>(key: &K, node: &N, build: &S) -> u64 {
        let mut h = build.build_hasher();
        key.hash(&mut h);
        node.hash(&mut h);
        h.finish()
    }

    /// Pick the single best node (O(N) max scan).
    pub fn pick_top<K: Hash>(&self, key: &K) -> Option<&N> {
        self.nodes
            .iter()
            .max_by_key(|n| Self::hrw_score(key, *n, &self.build))
    }

    /// Pick the top-k nodes with partial selection (O(N) + O(k log k))
    pub fn pick_top_k<K: Hash>(&self, key: &K, k: usize) -> Vec<&N> {
        if self.nodes.is_empty() || k == 0 {
            return Vec::new();
        }
        let k = k.min(self.nodes.len());

        let mut scored: Vec<_> = self
            .nodes
            .iter()
            .map(|n| (Self::hrw_score(key, n, &self.build), n))
            .collect();

        let k = k.min(scored.len());
        let nth = k - 1;

        // After this, the top-k elements are in scored[..k] in arbitrary order
        scored.select_nth_unstable_by(nth, |a, b| b.0.cmp(&a.0));

        // Now sort only the top-k slice to get deterministic replica order
        scored[..k].sort_unstable_by(|a, b| b.0.cmp(&a.0));

        // Return the top-k nodes
        scored[..k].iter().map(|&(_, n)| n).collect()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_pick_default_hasher() {
        let r = Rendezvous::from_nodes(["A", "B", "C"]);
        assert!(r.pick_top(&"key").is_some());
    }

    #[test]
    fn distribution_stability_on_node_change() {
        use std::collections::HashMap;

        let mut r = Rendezvous::from_nodes(["a", "b", "c", "d"]);
        let mut map1 = HashMap::new();
        for i in 0..1000 {
            map1.insert(i, r.pick_top(&i).unwrap().to_string());
        }

        r.add_node("e");
        let mut map2 = HashMap::new();
        let mut moved1_2 = 0;
        for i in 0..1000 {
            let n = r.pick_top(&i).unwrap().to_string();
            if *map1.get(&i).unwrap() != n {
                moved1_2 += 1;
            }
            map2.insert(i, n);
        }

        r.remove_node(&"c");
        let mut map3 = HashMap::new();
        let mut moved2_3 = 0;
        for i in 0..1000 {
            let n = r.pick_top(&i).unwrap().to_string();
            if *map2.get(&i).unwrap() != n {
                moved2_3 += 1;
            }
            map3.insert(i, n);
        }

        assert!(moved1_2 >= 100 && moved1_2 <= 300, "{moved1_2}");
        assert!(moved2_3 >= 100 && moved2_3 <= 300, "{moved2_3}");
    }

    #[test]
    fn ahash_pick_top_and_top_k() {
        use ahash::RandomState as AHash;
        let r = Rendezvous::from_nodes_and_hasher(["X", "Y", "Z"], AHash::new());
        let key = "alpha";
        let one = r.pick_top(&key).unwrap();
        let top2 = r.pick_top_k(&key, 2);
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0], one);
    }

    #[test]
    fn top_k_deterministic_order() {
        let r = Rendezvous::from_nodes(["A", "B", "C", "D"]);
        let p1 = r.pick_top_k(&"k42", 3);
        let p2 = r.pick_top_k(&"k42", 3);
        assert_eq!(p1, p2);
    }

    #[test]
    fn custom_hasher_construction_compiles() {
        // Example with the default again; swap to ahash/fxhash if you like:
        let r: Rendezvous<&'static str, RandomState> =
            Rendezvous::from_nodes_and_hasher(["n1", "n2", "n3"], RandomState::new());
        assert!(r.pick_top(&"abc").is_some());
    }

    #[test]
    fn add_and_remove_nodes() {
        let mut r = Rendezvous::new();
        assert!(r.is_empty());
        r.add_node("A");
        r.add_node("B");
        assert_eq!(r.len(), 2);
        assert!(!r.is_empty());
        assert!(r.remove_node(&"A"));
        assert_eq!(r.len(), 1);
        assert!(!r.remove_node(&"A")); // Already removed
        assert!(r.remove_node(&"B"));
        assert!(r.is_empty());
    }

    #[test]
    fn no_duplicate_nodes() {
        let mut r = Rendezvous::from_nodes(["A"]);
        r.add_node("A");
        assert_eq!(r.len(), 1);
        r.add_node("B");
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn pick_top_and_top_k_consistency() {
        let r = Rendezvous::from_nodes(["A", "B", "C"]);
        let key = "mykey";
        let one = r.pick_top(&key);
        let mut top1 = r.pick_top_k(&key, 1);
        let top1_opt = top1.pop();
        if one != top1_opt {
            eprintln!("pick_top: {:?}, pick_top_k(1): {:?}", one, top1_opt);
        }
        assert!(
            one.is_none() && top1_opt.is_none() || one == top1_opt,
            "pick_top: {:?}, pick_top_k(1): {:?}",
            one,
            top1_opt
        );
    }

    #[test]
    fn pick_top_k_edge_cases() {
        let r = Rendezvous::from_nodes(["A", "B"]);
        let empty = Rendezvous::<&str>::new();
        assert!(empty.pick_top_k(&"k", 1).is_empty());
        assert!(r.pick_top_k(&"k", 0).is_empty());
        let all = r.pick_top_k(&"k", 10);
        // Should return at most all nodes, and all should be from the original set
        assert!(all.len() <= r.len());
        for n in &all {
            assert!(r.nodes.contains(n));
        }
        // Should not panic if k > nodes.len()
    }
}
