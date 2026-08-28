//! The Python-facing reactor: scheduling and readiness methods plus the canonical
//! dispatch loop, as one pyclass both extensions compile in. `loopmini._core`
//! registers it with an owned runtime; an embedding extension (`kernmini._native`)
//! constructs it with `with_handle` on its own runtime. The dispatch loop carries
//! the injected-exception requeue rule, which must exist exactly once.
use crate::tokio_core::TokioCore;
use polling::Events;
use pyo3::intern;
use pyo3::prelude::*;
use std::ops::ControlFlow;
use tokio::runtime::Handle;

#[pyclass(name = "Reactor")]
pub struct PyReactor { core: TokioCore<Py<PyAny>> }

impl PyReactor {
    /// A reactor whose blocking waits run on `handle`'s runtime. Call `run` only
    /// from a thread outside that runtime (a Python main or session thread):
    /// Tokio panics on `block_on` from one of the runtime's own workers.
    pub fn with_handle(handle: Handle) -> PyResult<Self> {
        Ok(Self { core: TokioCore::with_handle(handle)? })
    }
}

#[pymethods]
impl PyReactor {
    #[new]
    fn new() -> PyResult<Self> { Ok(Self { core: TokioCore::new()? }) }

    fn time(&self) -> f64 { self.core.time() }
    fn schedule(&self, h: Py<PyAny>) { self.core.schedule(h) }
    fn schedule_ts(&self, h: Py<PyAny>) -> PyResult<()> { Ok(self.core.schedule_ts(h)?) }
    fn schedule_at(&self, when: f64, h: Py<PyAny>) -> (u64, u64) { self.core.schedule_at(when, h) }
    fn cancel_timer(&self, key: (u64, u64)) -> bool { self.core.cancel_timer(key) }
    fn timer_count(&self) -> usize { self.core.timer_count() }
    fn add_reader(&self, fd: usize, h: Py<PyAny>) -> PyResult<()> { Ok(self.core.add_reader(fd, h)?) }
    fn add_writer(&self, fd: usize, h: Py<PyAny>) -> PyResult<()> { Ok(self.core.add_writer(fd, h)?) }
    fn remove_reader(&self, fd: usize) -> PyResult<bool> { Ok(self.core.remove_reader(fd)?) }
    fn remove_writer(&self, fd: usize) -> PyResult<bool> { Ok(self.core.remove_writer(fd)?) }
    fn stop(&self) -> PyResult<()> { Ok(self.core.stop()?) }
    fn close(&self) { self.core.close() }

    fn run(&self, py: Python) -> PyResult<()> {
        loop {
            py.check_signals()?;
            let ControlFlow::Continue(timeout) = self.core.next_timeout() else { return Ok(()) };
            let events = match py.detach(|| self.core.poll(timeout)) {
                Ok(ev) => ev,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => Events::new(),
                Err(e) => return Err(e.into()),
            };
            self.core.process(&events);
            let batch = self.core.take_batch();
            let mut it = batch.into_iter();
            while let Some(h) = it.next() {
                // An injected async exception (e.g. KeyboardInterrupt) can surface at either
                // Python call; only a handle whose _run began may be dropped without requeue.
                let cancelled = h.bind(py).getattr(intern!(py, "_cancelled")).and_then(|v| v.extract::<bool>());
                match cancelled {
                    Err(e) => {
                        self.core.requeue_front(std::iter::once(h).chain(it));
                        return Err(e);
                    }
                    Ok(true) => continue,
                    Ok(false) => {}
                }
                if let Err(e) = h.bind(py).call_method0(intern!(py, "_run")) {
                    // An injected exception surfacing at `_run`'s entry leaves a single-frame
                    // traceback: the callback never ran, so requeue the handle - dropping it
                    // would lose e.g. a task wakeup and orphan the task. A deeper traceback
                    // means the callback began, so the handle is consumed (CPython semantics).
                    let entry_only = e.traceback(py)
                        .map_or(true, |tb| tb.getattr("tb_next").ok().is_none_or(|n| n.is_none()));
                    if entry_only { self.core.requeue_front(std::iter::once(h).chain(it)) }
                    else { self.core.requeue_front(it) }
                    return Err(e);
                }
            }
        }
    }
}
