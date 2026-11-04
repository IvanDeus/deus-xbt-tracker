//! Global PRNG replacing C `rand()`/`srand()` used by the original
//! (secret generation, peer selection, random strings). A xorshift64*
//! generator is used; like the original it is seeded from the clock.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static STATE: AtomicU64 = AtomicU64::new(0);

/// Port of `srand(time(NULL))` (call once at start-up).
pub fn srand(seed: u64) {
    let mut s = seed;
    if s == 0 {
        s = 0x2545F4914F6CDD1D;
    }
    // warm up the state
    s ^= s >> 12;
    s ^= s << 25;
    s ^= s >> 27;
    STATE.store(s, Ordering::Relaxed);
}

/// Port of `rand()`: returns a value in [0, 2^31) like glibc rand.
pub fn rand() -> u64 {
    loop {
        let old = STATE.load(Ordering::Relaxed);
        let mut s = old;
        s ^= s >> 12;
        s ^= s << 25;
        s ^= s >> 27;
        if STATE
            .compare_exchange_weak(old, s, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return (s.wrapping_mul(0x2545F4914F6CDD1D) >> 33) & 0x7FFF_FFFF;
        }
    }
}

/// Seed from clock + pid (used by main, mirroring `srand(time(NULL))`).
pub fn seed_from_time() {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    srand(t ^ ((std::process::id() as u64) << 32) ^ 0x9E3779B97F4A7C15);
}
