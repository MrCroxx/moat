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

//! A bounded completion coordinator, independent of the caller's runtime.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    panic::AssertUnwindSafe,
    sync::Arc,
    thread,
};

use futures_channel::mpsc;
use futures_util::{
    FutureExt, StreamExt,
    future::{BoxFuture, Either, select},
    stream::FuturesUnordered,
};
use moat_common::ChunkId;
use parking_lot::Mutex;

use crate::{Error, Result};

struct Budget {
    used: Mutex<(usize, usize)>,
    jobs: usize,
    bytes: usize,
}
pub(crate) struct Permit {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut used = self.budget.used.lock();
        used.0 -= 1;
        used.1 -= self.bytes;
    }
}

pub(crate) struct Job {
    pub id: Option<ChunkId>,
    pub future: BoxFuture<'static, ()>,
    pub permit: Option<Permit>,
}
#[derive(Clone)]
pub(crate) struct Scheduler {
    sender: mpsc::UnboundedSender<Job>,
    budget: Arc<Budget>,
}
impl Scheduler {
    pub fn new(max_jobs: usize, max_bytes: usize) -> Result<Self> {
        if max_jobs == 0 {
            return Err(Error::Invalid("pending operation count must be nonzero"));
        }
        let budget = Arc::new(Budget {
            used: Mutex::new((0, 0)),
            jobs: max_jobs,
            bytes: max_bytes,
        });
        let (sender, receiver) = mpsc::unbounded();
        thread::Builder::new()
            .name("moat-cache".into())
            .spawn(move || futures_executor::block_on(run(receiver)))
            .map_err(|error| Error::Store(error.into()))?;
        Ok(Self { sender, budget })
    }
    pub fn reserve(&self, bytes: usize) -> Result<Permit> {
        let mut used = self.budget.used.lock();
        if used.0 >= self.budget.jobs || bytes > self.budget.bytes - used.1 {
            return Err(Error::Busy);
        }
        used.0 += 1;
        used.1 += bytes;
        Ok(Permit {
            budget: self.budget.clone(),
            bytes,
        })
    }
    // Failure returns the owned job so its user values can be dropped after
    // releasing an enclosing admission lock.
    pub fn submit(&self, job: Job) -> std::result::Result<(), Job> {
        self.sender.unbounded_send(job).map_err(|error| error.into_inner())
    }
    pub fn snapshot(&self) -> (usize, usize) {
        *self.budget.used.lock()
    }
}

type Running = FuturesUnordered<BoxFuture<'static, ChunkId>>;
fn start(running: &Running, job: Job) {
    running.push(
        async move {
            let id = job.id.expect("keyed job");
            let _ = AssertUnwindSafe(job.future).catch_unwind().await;
            drop(job.permit);
            id
        }
        .boxed(),
    );
}
fn completed(
    id: ChunkId,
    running: &Running,
    active: &mut HashSet<ChunkId>,
    pending: &mut HashMap<ChunkId, VecDeque<Job>>,
) {
    if let Some(queue) = pending.get_mut(&id)
        && let Some(job) = queue.pop_front()
    {
        if queue.is_empty() {
            pending.remove(&id);
        }
        start(running, job);
    } else {
        active.remove(&id);
    }
}
async fn run(mut receiver: mpsc::UnboundedReceiver<Job>) {
    let mut running = Running::new();
    let mut active = HashSet::new();
    let mut pending = HashMap::<ChunkId, VecDeque<Job>>::new();
    let mut fence = None::<Job>;
    let mut disconnected = false;
    loop {
        if running.is_empty() {
            if let Some(job) = fence.take() {
                let _ = AssertUnwindSafe(job.future).catch_unwind().await;
                drop(job.permit);
                continue;
            }
            if disconnected {
                return;
            }
        }
        let next = if fence.is_some() || disconnected {
            let id = running.next().await.expect("running job");
            completed(id, &running, &mut active, &mut pending);
            continue;
        } else if running.is_empty() {
            receiver.next().await
        } else {
            match select(receiver.next(), running.next()).await {
                Either::Left((next, other)) => {
                    drop(other);
                    next
                }
                Either::Right((Some(id), other)) => {
                    drop(other);
                    completed(id, &running, &mut active, &mut pending);
                    continue;
                }
                Either::Right((None, _)) => unreachable!("nonempty running set"),
            }
        };
        let Some(job) = next else {
            disconnected = true;
            continue;
        };
        match job.id {
            None => fence = Some(job),
            Some(id) if active.insert(id) => start(&running, job),
            Some(id) => pending.entry(id).or_default().push_back(job),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use futures_channel::oneshot;

    use super::*;

    #[test]
    fn independent_ids_progress_while_same_id_jobs_and_barriers_keep_order() {
        let scheduler = Scheduler::new(4, 8).unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let (release, wait) = oneshot::channel();
        let (started, observed) = mpsc::channel();
        let first = Job {
            id: Some(ChunkId::from_u128(1)),
            permit: Some(scheduler.reserve(8).unwrap()),
            future: {
                let order = order.clone();
                async move {
                    started.send(()).unwrap();
                    let _ = wait.await;
                    order.lock().push(1);
                }
                .boxed()
            },
        };
        assert!(scheduler.submit(first).is_ok());
        observed.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(scheduler.reserve(1), Err(Error::Busy)));
        for (id, marker) in [(1, 2), (2, 3)] {
            let order = order.clone();
            let started = started_marker(&scheduler, id, marker, order);
            if id == 2 {
                started.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        let (finished, observed) = mpsc::channel();
        let barrier = Job {
            id: None,
            permit: Some(scheduler.reserve(0).unwrap()),
            future: {
                let order = order.clone();
                async move {
                    order.lock().push(4);
                    finished.send(()).unwrap();
                }
                .boxed()
            },
        };
        assert!(scheduler.submit(barrier).is_ok());
        assert_eq!(&*order.lock(), &[3]);
        release.send(()).unwrap();
        observed.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(&*order.lock(), &[3, 1, 2, 4]);
        // Completion replies can precede permit destruction. A second barrier
        // observes all previous permits released without polling a snapshot.
        let (done, observed) = mpsc::channel();
        let barrier = Job {
            id: None,
            permit: None,
            future: async move {
                done.send(()).unwrap();
            }
            .boxed(),
        };
        assert!(scheduler.submit(barrier).is_ok());
        observed.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(scheduler.snapshot(), (0, 0));
    }
    fn started_marker(scheduler: &Scheduler, id: u128, marker: u8, order: Arc<Mutex<Vec<u8>>>) -> mpsc::Receiver<()> {
        let (started, observed) = mpsc::channel();
        let job = Job {
            id: Some(ChunkId::from_u128(id)),
            permit: Some(scheduler.reserve(0).unwrap()),
            future: async move {
                order.lock().push(marker);
                let _ = started.send(());
            }
            .boxed(),
        };
        assert!(scheduler.submit(job).is_ok());
        observed
    }
}
