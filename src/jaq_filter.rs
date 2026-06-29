//! Experimental `?jaq=` support: run a user-supplied jq program over each
//! multiplex frame to filter and/or reshape it.
//!
//! Semantics follow jq: the program runs once per frame with the frame's JSON
//! projection as input. If it produces no output, the frame is dropped (so
//! `select(...)` acts as a filter); each value it does produce is sent as its
//! own JSON text message (so `{a, b}` reshapes, `.payload` projects a column).
//!
//! jaq-json is built with its `sync` feature, making `Val` (and therefore the
//! compiled [`Program`]) `Send`, so we can keep the compiled program in the
//! axum websocket task and run it inline — no separate executor thread. The
//! per-run interpreter state (`Ctx`, intermediate values) is created and fully
//! drained within [`Program::run`], never held across an `.await`.

use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Compiler, Ctx, Vars, data, unwrap_valr};
use jaq_json::Val;

/// A compiled jq program plus the lookup table its execution context needs.
pub struct Program {
    filter: jaq_core::compile::Filter<jaq_core::Native<data::JustLut<Val>>>,
}

impl Program {
    /// Compile a jq program. Returns a human-readable error string suitable for
    /// a `400` response when the program fails to lex, parse, or compile.
    pub fn compile(program: &str) -> Result<Self, String> {
        let file = File {
            code: program,
            path: (),
        };
        let defs = jaq_core::defs()
            .chain(jaq_std::defs())
            .chain(jaq_json::defs());
        let funs = jaq_core::funs::<data::JustLut<Val>>()
            .chain(jaq_std::funs())
            .chain(jaq_json::funs());

        let loader = Loader::new(defs);
        let arena = Arena::default();
        let modules = loader
            .load(&arena, file)
            .map_err(|errs| format_errors("load", &errs))?;
        let filter = Compiler::default()
            .with_funs(funs)
            .compile(modules)
            .map_err(|errs| format_errors("compile", &errs))?;

        Ok(Self { filter })
    }

    /// Run the program against one frame's JSON projection, returning each
    /// output value rendered as a compact JSON string ready to send.
    ///
    /// An empty `Vec` means "drop this frame" (e.g. `select(...)` rejected it).
    /// Keeping the parse-run-render entirely inside this method means the
    /// non-`Send` interpreter values never escape to the caller's async task.
    pub fn run_json(&self, input_json: &str) -> Result<Vec<String>, String> {
        let input: Val = serde_json::from_str(input_json).map_err(|e| e.to_string())?;
        Ok(self.run(input)?.into_iter().map(|v| v.to_string()).collect())
    }

    /// Run the program against one input value, collecting every output value.
    ///
    /// An empty result means "drop this frame". `Err` carries a runtime error
    /// (e.g. a type error in the program) for the caller to log; we don't tear
    /// the client down over one bad frame.
    pub fn run(&self, input: Val) -> Result<Vec<Val>, String> {
        let ctx = Ctx::<data::JustLut<Val>>::new(&self.filter.lut, Vars::new([]));
        self.filter
            .id
            .run((ctx, input))
            .map(unwrap_valr)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    }
}

/// Render jaq's `Vec<(File, Error)>` error lists into one short string.
fn format_errors<S: core::fmt::Display, P, E: core::fmt::Debug>(
    stage: &str,
    errors: &[(File<S, P>, E)],
) -> String {
    use core::fmt::Write as _;
    let mut out = format!("jaq {stage} error");
    for (_, err) in errors {
        let _ = write!(out, ": {err:?}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_str(program: &str, input: &str) -> Result<Vec<String>, String> {
        let prog = Program::compile(program)?;
        let val: Val = serde_json::from_str(input).map_err(|e| e.to_string())?;
        Ok(prog.run(val)?.into_iter().map(|v| v.to_string()).collect())
    }

    #[test]
    fn select_filters_frames() {
        // Matching predicate -> one output (keep).
        let out = run_str(r#"select(.type == "packet")"#, r#"{"type":"packet"}"#).unwrap();
        assert_eq!(out.len(), 1);
        // Non-matching predicate -> no output (drop).
        let out = run_str(r#"select(.type == "packet")"#, r#"{"type":"stats"}"#).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn projects_a_column() {
        let out = run_str(".payload.hash", r#"{"payload":{"hash":"abc"}}"#).unwrap();
        assert_eq!(out, vec!["\"abc\""]);
    }

    #[test]
    fn reshapes_object() {
        let out = run_str("{h: .hash}", r#"{"hash":"x","extra":1}"#).unwrap();
        assert_eq!(out, vec![r#"{"h":"x"}"#]);
    }

    #[test]
    fn invalid_program_is_rejected() {
        assert!(Program::compile("this is not (((valid").is_err());
    }
}
