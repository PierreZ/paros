//! The journal fold lab of the site's `/journal/calls/` page (#1363
//! (journal fold lab)): the reader makes the calls of three writers on a
//! journal, and `paros-journal` answers.
//!
//! # What this module is, and is not
//!
//! It is a **driver** of `paros-journal`, and depends on nothing else. Every
//! answer comes from [`paros_journal::JournalState::apply`] or
//! [`paros_journal::resolve()`], the code the journal's replicas run. The
//! module builds the commands and keeps what a client and a replica keep
//! around the fold (the writer's last view, the records of each segment, the
//! seal). It holds no journal rule of its own.
//!
//! No consensus runs here: each call is one decided slot, applied at once.
//! It draws no randomness and reads no clock, so a list of moves is the same
//! run every time.
//!
//! - [`world`] — the writers and the chain of segments.
//! - [`lab`] — the exercises, the reader's moves and the page's view.

pub mod lab;
pub mod world;

// ---- the wasm surface -------------------------------------------------------

#[cfg(target_arch = "wasm32")]
mod wasm {
    use wasm_bindgen::prelude::*;

    /// An exercise of the journal fold: three writers the reader drives by
    /// hand, with one goal.
    #[wasm_bindgen(js_name = JournalLab)]
    pub struct WasmJournalLab {
        inner: crate::journal::lab::Lab,
    }

    #[wasm_bindgen(js_class = JournalLab)]
    impl WasmJournalLab {
        /// Start the exercise `id`.
        ///
        /// # Errors
        ///
        /// A JS exception when no exercise has that id.
        #[wasm_bindgen(constructor)]
        pub fn new(id: &str) -> Result<WasmJournalLab, JsError> {
            console_error_panic_hook::set_once();
            crate::journal::lab::Lab::new(id)
                .map(|inner| WasmJournalLab { inner })
                .map_err(|why| JsError::new(&why))
        }

        /// Play one JSON `Move`; answers the `LabView` as JSON.
        pub fn play(&mut self, chosen: &str) -> String {
            match serde_json::from_str::<crate::journal::lab::Move>(chosen) {
                Ok(chosen) => self.inner.play(&chosen),
                Err(error) => {
                    return serde_json::json!({ "error": error.to_string() }).to_string();
                }
            }
            self.view()
        }

        /// The `LabView` as JSON.
        #[must_use]
        pub fn view(&self) -> String {
            serde_json::to_string(&self.inner.view()).unwrap_or_else(|err| {
                serde_json::json!({ "error": format!("the engine cannot encode the view: {err}") })
                    .to_string()
            })
        }
    }
}
