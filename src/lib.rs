pub mod reactor;
pub mod tokio_core;
mod pyreactor;

use pyo3::prelude::*;

pub use pyreactor::PyReactor;
pub use reactor::Reactor as ReactorCore;
pub use tokio_core::TokioCore;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyReactor>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
