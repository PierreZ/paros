//! The journal labs of the site's Journal pages: the fold lab of
//! `/journal/calls/` (#1363 (journal fold lab)), where the reader makes the
//! calls of three writers, and the register and record labs of
//! `/journal/matchmakers/` and `/journal/segments/` (#1364 (register and
//! segment record lab)), where the reader runs the matchmaker register and
//! the moves of the segment records. `paros-journal` answers every move.
//!
//! # What this module is, and is not
//!
//! It is a **driver** of `paros-journal`, and depends on nothing else. Every
//! answer comes from [`paros_journal::JournalState::apply`],
//! [`paros_journal::resolve()`] or `paros_journal::register`, the code the
//! journal's replicas and matchmakers run. The
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
//! - [`keyed`] — the shell of an exercise whose moves are named by a key.
//! - [`register`] — two controllers and three matchmakers of the register.
//! - [`records`] — one register value and the moves of its records.
//! - [`seal`] — the step-through of a reshape and a seal on the journal
//!   roles, for `/journal/segments/` (#1365).

pub mod keyed;
pub mod lab;
pub mod records;
pub mod register;
pub mod seal;
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

    /// The step-through trace of a Journal page (#1365 (reshape and seal
    /// step-through)), recorded now from the journal roles, as JSON: the page
    /// draws this live run in place of the recorded file.
    ///
    /// # Errors
    ///
    /// A JS exception when no page has that name, or when the trace does
    /// not encode.
    #[wasm_bindgen]
    pub fn journal_trace(page: &str) -> Result<String, JsError> {
        console_error_panic_hook::set_once();
        let trace = crate::journal::seal::trace_of(page)
            .ok_or_else(|| JsError::new(&format!("no Journal trace {page:?}")))?;
        serde_json::to_string(&trace).map_err(|err| JsError::new(&err.to_string()))
    }

    /// The JSON of a keyed lab's view.
    fn keyed_view<W: crate::journal::keyed::World>(lab: &crate::journal::keyed::Lab<W>) -> String {
        serde_json::to_string(&lab.view()).unwrap_or_else(|err| {
            serde_json::json!({ "error": format!("the engine cannot encode the view: {err}") })
                .to_string()
        })
    }

    /// Play one JSON keyed `Move` on `lab`; answers its view as JSON.
    fn keyed_play<W: crate::journal::keyed::World>(
        lab: &mut crate::journal::keyed::Lab<W>,
        chosen: &str,
    ) -> String {
        match serde_json::from_str::<crate::journal::keyed::Move>(chosen) {
            Ok(chosen) => lab.play(&chosen),
            Err(error) => {
                return serde_json::json!({ "error": error.to_string() }).to_string();
            }
        }
        keyed_view(lab)
    }

    /// An exercise of the matchmaker register: two controllers and three
    /// matchmakers, each read and write delivered or dropped by hand.
    #[wasm_bindgen(js_name = RegisterLab)]
    pub struct WasmRegisterLab {
        inner: crate::journal::keyed::Lab<crate::journal::register::Register>,
    }

    #[wasm_bindgen(js_class = RegisterLab)]
    impl WasmRegisterLab {
        /// Start the exercise `id`.
        ///
        /// # Errors
        ///
        /// A JS exception when no exercise has that id.
        #[wasm_bindgen(constructor)]
        pub fn new(id: &str) -> Result<WasmRegisterLab, JsError> {
            console_error_panic_hook::set_once();
            crate::journal::keyed::Lab::new(crate::journal::register::exercises(), id)
                .map(|inner| WasmRegisterLab { inner })
                .map_err(|why| JsError::new(&why))
        }

        /// Play one JSON `Move`; answers the `LabView` as JSON.
        pub fn play(&mut self, chosen: &str) -> String {
            keyed_play(&mut self.inner, chosen)
        }

        /// The `LabView` as JSON.
        #[must_use]
        pub fn view(&self) -> String {
            keyed_view(&self.inner)
        }
    }

    /// An exercise of the segment records: one register value, each move a
    /// compare-and-set with its guard.
    #[wasm_bindgen(js_name = RecordLab)]
    pub struct WasmRecordLab {
        inner: crate::journal::keyed::Lab<crate::journal::records::Records>,
    }

    #[wasm_bindgen(js_class = RecordLab)]
    impl WasmRecordLab {
        /// Start the exercise `id`.
        ///
        /// # Errors
        ///
        /// A JS exception when no exercise has that id.
        #[wasm_bindgen(constructor)]
        pub fn new(id: &str) -> Result<WasmRecordLab, JsError> {
            console_error_panic_hook::set_once();
            crate::journal::keyed::Lab::new(crate::journal::records::exercises(), id)
                .map(|inner| WasmRecordLab { inner })
                .map_err(|why| JsError::new(&why))
        }

        /// Play one JSON `Move`; answers the `LabView` as JSON.
        pub fn play(&mut self, chosen: &str) -> String {
            keyed_play(&mut self.inner, chosen)
        }

        /// The `LabView` as JSON.
        #[must_use]
        pub fn view(&self) -> String {
            keyed_view(&self.inner)
        }
    }
}
