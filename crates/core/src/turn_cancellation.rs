use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::oneshot;

struct ActiveTurn {
    turn_id: String,
    cancel_tx: oneshot::Sender<()>,
}

/// Handed to the background task spawned by `submit_turn`. `tokio::select!`
/// it against the turn's real work — resolving means `cancel` was called for
/// this exact `(runtime_id, turn_id)` and the task must stop and emit a
/// `Completed { outcome: Cancelled }` (or equivalent) terminal event instead
/// of continuing.
pub type CancelSignal = oneshot::Receiver<()>;

/// One registry instance is meant to be shared (behind `Arc`) between an
/// adapter's `submit_turn` (which calls [`Self::begin`]) and its `cancel`
/// (which calls [`Self::fire`]). Tracks at most one in-flight turn per
/// `runtime_id` — enough for this codebase's single-active-turn-per-session
/// discipline (`SessionApplication`'s `TurnGate`), which is enforced one
/// layer up and guarantees a prior turn has already reached a terminal state
/// before a new one is submitted for the same `runtime_id`.
#[derive(Default)]
pub struct TurnCancellationRegistry {
    active: Mutex<HashMap<String, ActiveTurn>>,
}

impl TurnCancellationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `turn_id` as `runtime_id`'s in-flight turn and return the
    /// signal the background task should race against. Call this before
    /// spawning that task (not from within it), so a `cancel` that arrives
    /// the instant `submit_turn` returns can never miss the registration.
    pub fn begin(&self, runtime_id: &str, turn_id: &str) -> CancelSignal {
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let mut guard = self
            .active
            .lock()
            .expect("turn cancellation registry lock poisoned");
        guard.insert(
            runtime_id.to_string(),
            ActiveTurn {
                turn_id: turn_id.to_string(),
                cancel_tx,
            },
        );
        cancel_rx
    }

    pub fn fire(&self, runtime_id: &str, turn_id: Option<&str>) -> bool {
        let mut guard = self
            .active
            .lock()
            .expect("turn cancellation registry lock poisoned");
        let matches = guard
            .get(runtime_id)
            .map(|active| turn_id.map_or(true, |expected| expected == active.turn_id))
            .unwrap_or(false);
        if !matches {
            return false;
        }
        match guard.remove(runtime_id) {
            Some(active) => active.cancel_tx.send(()).is_ok(),
            None => false,
        }
    }

    /// The background task must call this once it reaches a terminal state
    /// on its own (whether by finishing normally or by observing its own
    /// [`CancelSignal`] fire), so that a `cancel` racing in just after has
    /// nothing left to signal instead of firing into a dropped receiver or —
    /// worse — clearing a *newer* turn's entry. Only clears the registry
    /// entry if it still matches `turn_id`.
    pub fn end(&self, runtime_id: &str, turn_id: &str) {
        let mut guard = self
            .active
            .lock()
            .expect("turn cancellation registry lock poisoned");
        if guard
            .get(runtime_id)
            .map(|active| active.turn_id == turn_id)
            .unwrap_or(false)
        {
            guard.remove(runtime_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fire_resolves_the_matching_cancel_signal() {
        let registry = TurnCancellationRegistry::new();
        let mut signal = registry.begin("runtime-1", "turn-1");

        assert!(registry.fire("runtime-1", Some("turn-1")));
        signal
            .try_recv()
            .expect("cancel signal should have resolved");
    }

    #[tokio::test]
    async fn fire_with_a_mismatched_turn_id_is_a_no_op() {
        let registry = TurnCancellationRegistry::new();
        let mut signal = registry.begin("runtime-1", "turn-1");

        assert!(!registry.fire("runtime-1", Some("turn-2")));
        assert!(signal.try_recv().is_err(), "signal must not have fired");
    }

    #[tokio::test]
    async fn fire_with_no_turn_id_cancels_whatever_is_active() {
        let registry = TurnCancellationRegistry::new();
        let mut signal = registry.begin("runtime-1", "turn-1");

        assert!(registry.fire("runtime-1", None));
        signal
            .try_recv()
            .expect("cancel signal should have resolved");
    }

    #[tokio::test]
    async fn end_prevents_a_racing_cancel_from_firing_into_a_dropped_receiver() {
        let registry = TurnCancellationRegistry::new();
        let _signal = registry.begin("runtime-1", "turn-1");

        registry.end("runtime-1", "turn-1");

        assert!(!registry.fire("runtime-1", Some("turn-1")));
    }

    #[tokio::test]
    async fn end_does_not_clear_a_newer_turn_registered_under_the_same_runtime_id() {
        let registry = TurnCancellationRegistry::new();
        let _stale_signal = registry.begin("runtime-1", "turn-1");
        let mut current_signal = registry.begin("runtime-1", "turn-2");

        // A late `end("turn-1")` call (e.g. a background task that raced with
        // a new submission somehow) must not clobber turn-2's entry.
        registry.end("runtime-1", "turn-1");

        assert!(registry.fire("runtime-1", Some("turn-2")));
        current_signal
            .try_recv()
            .expect("turn-2's signal should still be reachable");
    }
}
