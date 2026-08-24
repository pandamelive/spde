use std::sync::atomic::{AtomicBool, Ordering};

static EXIT_SIGNAL: AtomicBool = AtomicBool::new(false);

pub fn setup_signal_handler() {
    ctrlc::set_handler(move || {
        eprintln!("\nreceived exit signal");
        EXIT_SIGNAL.store(true, Ordering::SeqCst);
    })
    .expect("failed set ctrl‑c handler");
}

pub fn is_exit_triggered() -> bool {
    EXIT_SIGNAL.load(Ordering::SeqCst)
}
