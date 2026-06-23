//! `paros-wasm-demo` — a browser/wasm demo animating a deterministic paros
//! simulation and the impact of network chaos.
//!
//! Mirrors `moonpool-wasm-demo`. The real demo (wasm-bindgen glue, a canvas
//! renderer, `?seed=`/`?still=`/`?dump` query params) and the GitHub Pages
//! pipeline arrive in Stage 1 (#14); the workloads it animates are defined once
//! in `paros-sim` and reused here. Stage 0 is an empty scaffold that keeps the
//! crate building (it depends only on the wasm-safe `paros-core`).
