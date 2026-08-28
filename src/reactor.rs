//! PyO3-free reactor core: ready queue, timer map, fd interest map, and poll
//! phases, generic over the handle type so Rust and Python drivers share it.
use polling::{Event, Events, Poller};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::ops::ControlFlow;
use std::os::fd::BorrowedFd;
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct FdEntry<H> { reader: Option<H>, writer: Option<H> }
impl<H> Default for FdEntry<H> { fn default() -> Self { Self { reader: None, writer: None } } }

fn interest<H>(key: usize, e: &FdEntry<H>) -> Event {
    match (e.reader.is_some(), e.writer.is_some()) {
        (true, true) => Event::all(key),
        (true, false) => Event::readable(key),
        (false, true) => Event::writable(key),
        (false, false) => Event::none(key),
    }
}

fn borrowed(fd: usize) -> BorrowedFd<'static> { unsafe { BorrowedFd::borrow_raw(fd as i32) } }

struct Inner<H> {
    ready: VecDeque<H>,
    timers: BTreeMap<(u64, u64), H>, // keyed on (deadline µs, submission seq): first entry is next due
    fds: HashMap<usize, FdEntry<H>>,
    seq: u64,
    stop: bool,
}

pub struct Reactor<H> {
    inner: Mutex<Inner<H>>,
    tsq: Mutex<VecDeque<H>>,
    poller: Poller,
    start: Instant,
}

impl<H: Clone> Reactor<H> {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            inner: Mutex::new(Inner { ready: VecDeque::new(), timers: BTreeMap::new(), fds: HashMap::new(), seq: 0, stop: false }),
            tsq: Mutex::new(VecDeque::new()),
            poller: Poller::new()?,
            start: Instant::now(),
        })
    }

    fn now_us(&self) -> u64 { self.start.elapsed().as_micros() as u64 }

    pub fn time(&self) -> f64 { self.start.elapsed().as_secs_f64() }

    /// The poller's own fd (a kqueue/epoll fd is itself pollable), for embedding in another reactor.
    pub fn poller_fd(&self) -> std::os::fd::RawFd { std::os::fd::AsRawFd::as_raw_fd(&self.poller) }

    pub fn schedule(&self, h: H) { self.inner.lock().unwrap().ready.push_back(h) }

    /// Safe from any thread: queues the handle and wakes a blocked `poll`.
    pub fn schedule_ts(&self, h: H) -> io::Result<()> {
        self.tsq.lock().unwrap().push_back(h);
        self.poller.notify()
    }

    /// `when` is in seconds on this reactor's `time()` clock. Returns the timer's
    /// key, which `cancel_timer` accepts until the timer fires.
    pub fn schedule_at(&self, when: f64, h: H) -> (u64, u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.seq += 1;
        let key = ((when.max(0.) * 1e6) as u64, inner.seq);
        inner.timers.insert(key, h);
        key
    }

    /// Drop a scheduled timer. False when it already fired (or was already removed):
    /// the promoted handle then carries its own cancelled flag, which dispatch skips.
    pub fn cancel_timer(&self, key: (u64, u64)) -> bool {
        self.inner.lock().unwrap().timers.remove(&key).is_some()
    }

    pub fn timer_count(&self) -> usize { self.inner.lock().unwrap().timers.len() }

    fn add_side(&self, fd: usize, h: H, write: bool) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let fresh = !inner.fds.contains_key(&fd);
        let e = inner.fds.entry(fd).or_default();
        if write { e.writer = Some(h) } else { e.reader = Some(h) }
        let ev = interest(fd, e);
        if fresh { unsafe { self.poller.add(fd as i32, ev) } } else { self.poller.modify(borrowed(fd), ev) }
    }

    fn rm_side(&self, fd: usize, write: bool) -> io::Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        let Some(e) = inner.fds.get_mut(&fd) else { return Ok(false) };
        let had = if write { e.writer.take().is_some() } else { e.reader.take().is_some() };
        let empty = e.reader.is_none() && e.writer.is_none();
        let ev = interest(fd, e);
        if empty {
            inner.fds.remove(&fd);
            let _ = self.poller.delete(borrowed(fd));
        } else { self.poller.modify(borrowed(fd), ev)? }
        Ok(had)
    }

    pub fn add_reader(&self, fd: usize, h: H) -> io::Result<()> { self.add_side(fd, h, false) }
    pub fn add_writer(&self, fd: usize, h: H) -> io::Result<()> { self.add_side(fd, h, true) }
    pub fn remove_reader(&self, fd: usize) -> io::Result<bool> { self.rm_side(fd, false) }
    pub fn remove_writer(&self, fd: usize) -> io::Result<bool> { self.rm_side(fd, true) }

    pub fn stop(&self) -> io::Result<()> {
        self.inner.lock().unwrap().stop = true;
        self.poller.notify()
    }

    pub fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        let fds: Vec<usize> = inner.fds.drain().map(|(fd, _)| fd).collect();
        for fd in fds { let _ = self.poller.delete(borrowed(fd)); }
        inner.ready.clear();
        inner.timers.clear();
        self.tsq.lock().unwrap().clear();
    }

    fn drain_tsq(&self, ready: &mut VecDeque<H>) {
        let mut q = self.tsq.lock().unwrap();
        while let Some(h) = q.pop_front() { ready.push_back(h) }
    }

    /// One turn's first phase: consume a pending stop, promote due timers and
    /// cross-thread handles, and say how long the driver may block in `poll`.
    pub fn next_timeout(&self) -> ControlFlow<(), Option<Duration>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.stop { inner.stop = false; return ControlFlow::Break(()) }
        let Inner { ready, timers, .. } = &mut *inner;
        self.drain_tsq(ready);
        let now = self.now_us();
        while timers.first_key_value().is_some_and(|((when, _), _)| *when <= now) {
            let (_, h) = timers.pop_first().unwrap();
            ready.push_back(h);
        }
        // Clamped like CPython's MAXIMUM_SELECT_TIMEOUT: a sleep(inf) timer saturates
        // to u64::MAX and kqueue rejects such a timespec with EINVAL
        const MAX_POLL_US: u64 = 86_400_000_000;
        ControlFlow::Continue(if !ready.is_empty() { Some(Duration::ZERO) }
            else { timers.first_key_value().map(|((when, _), _)| Duration::from_micros((when - now + 1).min(MAX_POLL_US))) })
    }

    /// Blocking wait; holds no locks, so drivers may run it with the GIL released.
    pub fn poll(&self, timeout: Option<Duration>) -> io::Result<Events> {
        let mut events = Events::new();
        self.poller.wait(&mut events, timeout)?;
        Ok(events)
    }

    /// Queue handles for fired fd events (re-arming the oneshot sources) and
    /// any cross-thread handles that arrived during `poll`.
    pub fn process(&self, events: &Events) {
        let mut inner = self.inner.lock().unwrap();
        let Inner { ready, fds, .. } = &mut *inner;
        for ev in events.iter() {
            if let Some(e) = fds.get(&ev.key) {
                if ev.readable { if let Some(h) = &e.reader { ready.push_back(h.clone()) } }
                if ev.writable { if let Some(h) = &e.writer { ready.push_back(h.clone()) } }
                let _ = self.poller.modify(borrowed(ev.key), interest(ev.key, e));
            }
        }
        self.drain_tsq(ready);
    }

    pub fn take_batch(&self) -> VecDeque<H> { std::mem::take(&mut self.inner.lock().unwrap().ready) }

    /// Put back unrun handles (in their original order) after an aborted batch.
    pub fn requeue_front(&self, items: impl DoubleEndedIterator<Item = H>) {
        let mut inner = self.inner.lock().unwrap();
        for h in items.rev() { inner.ready.push_front(h) }
    }
}
