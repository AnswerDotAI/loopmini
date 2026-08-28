//! The same reactor hosted on a Tokio current-thread runtime: Tokio waits on the
//! kqueue's own fd (a kqueue is pollable), then a zero-timeout drain preserves the
//! kqueue-native level/oneshot semantics that asyncio's add_reader contract needs.
//! Rust futures spawned on the runtime advance during every poll phase, with the
//! GIL released, which is the shared-reactor story in one file.
use crate::reactor::Reactor as ReactorCore;
use polling::Events;
use std::io;
use std::os::fd::RawFd;
use std::time::Duration;
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::runtime::{Builder, Handle, Runtime};
use std::ops::Deref;

enum Rt { Owned(Runtime), Borrowed(Handle) }

impl Rt {
    fn handle(&self) -> Handle {
        match self { Rt::Owned(r) => r.handle().clone(), Rt::Borrowed(h) => h.clone() }
    }
    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        match self { Rt::Owned(r) => r.block_on(f), Rt::Borrowed(h) => h.block_on(f) }
    }
}

pub struct TokioCore<H> {
    afd: AsyncFd<RawFd>, // declared before rt: must deregister while an owned driver lives
    rt: Rt,
    core: ReactorCore<H>,
}

impl<H> Deref for TokioCore<H> {
    type Target = ReactorCore<H>;
    fn deref(&self) -> &ReactorCore<H> { &self.core }
}

impl<H: Clone> TokioCore<H> {
    /// A reactor on its own current-thread runtime, for standalone use.
    pub fn new() -> io::Result<Self> {
        Self::build(Rt::Owned(Builder::new_current_thread().enable_io().enable_time().build()?))
    }

    /// A reactor on an existing runtime. Call `poll` only from a thread outside that
    /// runtime: Tokio panics on `block_on` from one of its own workers. The runtime
    /// must outlive this reactor.
    pub fn with_handle(handle: Handle) -> io::Result<Self> { Self::build(Rt::Borrowed(handle)) }

    fn build(rt: Rt) -> io::Result<Self> {
        let core = ReactorCore::new()?;
        let handle = rt.handle();
        let afd = {
            let _g = handle.enter();
            AsyncFd::with_interest(core.poller_fd(), Interest::READABLE)?
        };
        Ok(Self { afd, rt, core })
    }


    /// Wait in Tokio, then drain without blocking. Readiness is cleared *before*
    /// the drain: an event arriving after the clear is either picked up by this
    /// drain or arrives as a fresh edge. A drain capped by Events capacity leaves
    /// the ready queue non-empty, so the next turn polls with a zero timeout and
    /// drains again: no burst is lost.
    pub fn poll(&self, timeout: Option<Duration>) -> io::Result<Events> {
        // A zero timeout means "drain, don't wait": entering the runtime would round
        // the wait up to the timer driver's ~1ms tick, at 1ms per busy loop turn
        if timeout == Some(Duration::ZERO) { return self.core.poll(timeout) }
        self.rt.block_on(async {
            let ready = self.afd.readable();
            match timeout {
                Some(t) => {
                    if let Ok(Ok(mut guard)) = tokio::time::timeout(t, ready).await { guard.clear_ready() }
                }
                None => {
                    if let Ok(mut guard) = ready.await { guard.clear_ready() }
                }
            }
        });
        self.core.poll(Some(Duration::ZERO))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::ControlFlow;
    use std::time::Instant;

    #[test]
    fn borrowed_handle_drives_waits() {
        let rt = Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let core: TokioCore<u32> = TokioCore::with_handle(rt.handle().clone()).unwrap();
        core.schedule_at(core.time() + 0.01, 7);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let ControlFlow::Continue(t) = core.next_timeout() else { panic!("unexpected stop") };
            let ev = core.poll(t).unwrap();
            core.process(&ev);
            if core.take_batch().into_iter().any(|h| h == 7) { break }
            assert!(Instant::now() < deadline, "timer never fired through the borrowed runtime");
        }
    }

    #[test]
    fn borrowed_handle_wakes_from_another_thread() {
        let rt = Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let core = std::sync::Arc::new(TokioCore::with_handle(rt.handle().clone()).unwrap());
        let sender = core.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            sender.schedule_ts(7).unwrap();
        });
        let ev = core.poll(Some(Duration::from_secs(5))).unwrap();
        core.process(&ev);
        assert_eq!(core.take_batch().into_iter().collect::<Vec<_>>(), [7]);
    }
}
