//! The per-iteration [`StateHandle`] as the registry of the harness's shared
//! singletons: the storage world, the shape registry, the scripted lifecycle
//! queue, the corpus's scripted-crash flag. Each is published under its own
//! well-known key by whoever asks first and found by everyone after — fresh
//! per seed, stable across a process's reboots.

use std::sync::{Arc, Mutex};

use moonpool_sim::StateHandle;

/// Get-or-publish the `Arc<Mutex<T>>` under `key`, creating it with `init`
/// on the first ask. Get-then-publish is race-free: the sim executor is
/// single-threaded and this runs synchronously (no `.await` between the get
/// and the publish).
pub(crate) fn published<T: Send + Sync + 'static>(
    state: &StateHandle,
    key: &'static str,
    init: impl FnOnce() -> T,
) -> Arc<Mutex<T>> {
    if let Some(value) = state.get::<Arc<Mutex<T>>>(key) {
        return value;
    }
    let value = Arc::new(Mutex::new(init()));
    state.publish(key, value.clone());
    value
}
