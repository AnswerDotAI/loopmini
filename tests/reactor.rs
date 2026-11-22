use std::sync::Arc;
use std::thread;
use std::time::Duration;

use loopmini::reactor::Reactor;

#[test]
fn cross_thread_schedule_wakes_poll() {
    let reactor = Arc::new(Reactor::new().unwrap());
    let sender = reactor.clone();
    let thread = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        sender.schedule_ts("ready").unwrap();
    });
    let events = reactor.poll(None).unwrap();
    reactor.process(&events);
    thread.join().unwrap();
    assert_eq!(reactor.take_batch().into_iter().collect::<Vec<_>>(), vec!["ready"]);
}
