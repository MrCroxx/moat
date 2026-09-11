// Copyright 2026- Moat Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_util::{future::BoxFuture, task::AtomicWaker};
use parking_lot::Mutex;

/// Schedules batched completion delivery on an application's existing executor.
/// It must arrange for each task to be polled without blocking `spawn`.
/// Dropping a task falls back to ordered delivery on the producer/drop thread.
pub trait CompletionExecutor: Debug + Send + Sync + 'static {
    /// Schedules one driver task per disk without adding an I/O worker.
    fn spawn(&self, task: BoxFuture<'static, ()>);
}

type Action = Box<dyn FnOnce() + Send>;
struct Queue {
    batches: VecDeque<Vec<Action>>,
    producer_closed: bool,
    driver_alive: bool,
    draining: bool,
}
struct Shared {
    queue: Mutex<Queue>,
    wake: AtomicWaker,
}
impl Shared {
    fn drain_fallback(&self) {
        loop {
            let batch = {
                let mut queue = self.queue.lock();
                match queue.batches.pop_front() {
                    Some(batch) => batch,
                    None => {
                        queue.draining = false;
                        return;
                    }
                }
            };
            for action in batch {
                action();
            }
        }
    }
}

pub(crate) struct Delivery {
    shared: Option<Arc<Shared>>,
    pending: Vec<Action>,
}
impl Delivery {
    pub fn new(executor: Option<&Arc<dyn CompletionExecutor>>) -> Self {
        let shared = executor.map(|executor| {
            let shared = Arc::new(Shared {
                queue: Mutex::new(Queue {
                    batches: VecDeque::new(),
                    producer_closed: false,
                    driver_alive: true,
                    draining: false,
                }),
                wake: AtomicWaker::new(),
            });
            executor.spawn(Box::pin(Driver { shared: shared.clone() }));
            shared
        });
        Self {
            shared,
            pending: Vec::new(),
        }
    }
    pub fn push(&mut self, action: impl FnOnce() + Send + 'static) {
        if self.shared.is_some() {
            self.pending.push(Box::new(action));
        } else {
            action();
        }
    }
    pub fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let shared = self.shared.as_ref().expect("batched delivery");
        let fallback = {
            let mut queue = shared.queue.lock();
            queue.batches.push_back(std::mem::take(&mut self.pending));
            let fallback = !queue.driver_alive && !queue.draining;
            if fallback {
                queue.draining = true;
            }
            fallback
        };
        if fallback {
            shared.drain_fallback();
        } else {
            shared.wake.wake();
        }
    }
}
impl Drop for Delivery {
    fn drop(&mut self) {
        self.flush();
        if let Some(shared) = &self.shared {
            shared.queue.lock().producer_closed = true;
            shared.wake.wake();
        }
    }
}
struct Driver {
    shared: Arc<Shared>,
}
impl Future for Driver {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.shared.wake.register(cx.waker());
        let (batch, closed) = {
            let mut queue = self.shared.queue.lock();
            (queue.batches.pop_front(), queue.producer_closed)
        };
        if let Some(batch) = batch {
            for action in batch {
                action();
            }
            // Yield between producer batches so delivery cannot monopolize an
            // application worker while other ready cache clients wait.
            cx.waker().wake_by_ref();
            Poll::Pending
        } else if closed {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        let drain = {
            let mut queue = self.shared.queue.lock();
            queue.driver_alive = false;
            let drain = !queue.draining;
            queue.draining = true;
            drain
        };
        self.shared.wake.take();
        if drain {
            self.shared.drain_fallback();
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::task::noop_waker;

    use super::*;

    #[derive(Default)]
    struct Manual(Mutex<Option<BoxFuture<'static, ()>>>);
    impl Debug for Manual {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Manual")
        }
    }
    impl CompletionExecutor for Manual {
        fn spawn(&self, task: BoxFuture<'static, ()>) {
            *self.0.lock() = Some(task);
        }
    }
    fn enqueue(delivery: &mut Delivery, seen: &Arc<Mutex<Vec<usize>>>, value: usize) {
        let seen = seen.clone();
        delivery.push(move || seen.lock().push(value));
    }
    #[test]
    fn batches_yield_and_close_drains_in_order() {
        let executor = Arc::new(Manual::default());
        let erased: Arc<dyn CompletionExecutor> = executor.clone();
        let mut delivery = Delivery::new(Some(&erased));
        let seen = Arc::new(Mutex::new(Vec::new()));
        enqueue(&mut delivery, &seen, 1);
        enqueue(&mut delivery, &seen, 2);
        delivery.flush();
        enqueue(&mut delivery, &seen, 3);
        drop(delivery);
        assert!(seen.lock().is_empty());
        let mut task = executor.0.lock().take().unwrap();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(task.as_mut().poll(&mut cx).is_pending());
        assert_eq!(*seen.lock(), vec![1, 2]);
        assert!(task.as_mut().poll(&mut cx).is_pending());
        assert_eq!(*seen.lock(), vec![1, 2, 3]);
        assert!(task.as_mut().poll(&mut cx).is_ready());
    }
    #[test]
    fn executor_cancellation_drains_accepted_and_future_batches() {
        let executor = Arc::new(Manual::default());
        let erased: Arc<dyn CompletionExecutor> = executor.clone();
        let mut delivery = Delivery::new(Some(&erased));
        let seen = Arc::new(Mutex::new(Vec::new()));
        enqueue(&mut delivery, &seen, 1);
        delivery.flush();
        enqueue(&mut delivery, &seen, 2);
        drop(executor.0.lock().take());
        assert_eq!(*seen.lock(), vec![1]);
        delivery.flush();
        enqueue(&mut delivery, &seen, 3);
        drop(delivery);
        assert_eq!(*seen.lock(), vec![1, 2, 3]);
    }
    #[test]
    fn direct_delivery_runs_immediately() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut delivery = Delivery::new(None);
        enqueue(&mut delivery, &seen, 1);
        assert_eq!(*seen.lock(), vec![1]);
    }
}
