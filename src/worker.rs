use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle};

use crate::Admission;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerEvent {
    Started,
    DrainStarted,
    DrainCompleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerCadence {
    Interval,
    EagerBacklog,
}

pub trait FlushCycle: Send {
    fn flush(&mut self) -> impl Future<Output = ()> + Send;
    fn is_empty(&self) -> bool;
    fn has_ready_queue(&self) -> bool;
    fn on_event(&self, _event: WorkerEvent) {}
}

pub struct Worker {
    admission: Admission,
    shutdown: Arc<Notify>,
    join: JoinHandle<()>,
}

struct CloseOnExit(Admission);

impl Drop for CloseOnExit {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl Worker {
    pub fn spawn<C: FlushCycle + 'static>(admission: Admission, cycle: C) -> Self {
        Self::spawn_with_cadence(admission, cycle, WorkerCadence::EagerBacklog)
    }

    pub fn spawn_with_cadence<C: FlushCycle + 'static>(
        admission: Admission,
        cycle: C,
        cadence: WorkerCadence,
    ) -> Self {
        let shutdown = Arc::new(Notify::new());
        let join = tokio::spawn(run(cycle, admission.clone(), shutdown.clone(), cadence));
        Self {
            admission,
            shutdown,
            join,
        }
    }

    /// Closes admission and waits for acknowledgement of every accepted batch.
    pub fn shutdown(self) -> impl Future<Output = Result<(), JoinError>> + Send {
        self.admission.close();
        self.shutdown.notify_one();
        self.join
    }
}

async fn run<C: FlushCycle>(
    mut cycle: C,
    admission: Admission,
    shutdown: Arc<Notify>,
    cadence: WorkerCadence,
) {
    let _close_on_exit = CloseOnExit(admission.clone());
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut draining = false;
    let mut backlog = false;
    cycle.on_event(WorkerEvent::Started);
    loop {
        if !draining {
            let eager = backlog && cadence == WorkerCadence::EagerBacklog;
            tokio::select! {
                _ = ticker.tick(), if !eager => {},
                _ = tokio::task::yield_now(), if eager => {},
                _ = shutdown.notified() => {
                    admission.close_and_wait().await;
                    draining = true;
                    cycle.on_event(WorkerEvent::DrainStarted);
                }
            }
        }
        cycle.flush().await;
        backlog = cycle.has_ready_queue();
        if draining && cycle.is_empty() {
            cycle.on_event(WorkerEvent::DrainCompleted);
            return;
        }
        if draining && !backlog {
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else if draining {
            tokio::task::yield_now().await;
        }
    }
}
