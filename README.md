# hrw

A simple, generic implementation of Highest Random Weight (HRW or Rendezvous) hashing in Rust.

## Features
- Deterministic node selection for any key
- Pluggable hashers (e.g., `ahash`, `std`)
- Add/remove nodes at runtime
- No external dependencies for core logic

## Example

```rust
use hrw::Rendezvous;

let mut r = Rendezvous::from_nodes(["A", "B"]);
assert_eq!(r.len(), 2);
r.add_node("C");
assert_eq!(r.len(), 3);
r.remove_node(&"B");
assert_eq!(r.len(), 2);
let chosen = r.pick_top(&"my-key");
assert!(chosen.is_some());
```

## License
MIT
