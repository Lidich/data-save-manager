use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const CLOSED: usize = 1 << (usize::BITS - 1);

#[derive(Clone, Default)]
pub struct Admission {
    state: Arc<AtomicUsize>,
}

pub struct EnqueueGuard<'a> {
    state: &'a AtomicUsize,
}

impl Admission {
    /// Reserves admission atomically with respect to closing the producer gate.
    #[inline]
    pub fn enter(&self) -> Option<EnqueueGuard<'_>> {
        let previous = self.state.fetch_add(1, Ordering::Acquire);
        if previous & CLOSED != 0 {
            self.state.fetch_sub(1, Ordering::Release);
            None
        } else {
            Some(EnqueueGuard { state: &self.state })
        }
    }

    pub fn close(&self) {
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
    }

    pub async fn close_and_wait(&self) {
        self.close();
        while self.state.load(Ordering::Acquire) & !CLOSED != 0 {
            tokio::task::yield_now().await;
        }
    }
}

impl Drop for EnqueueGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.state.fetch_sub(1, Ordering::Release);
    }
}
