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

use std::{ops::Range, sync::Arc};

use futures_channel::oneshot;
use moat_common::ChunkId;
use moat_engine::{ChunkData, Outcome, ReclaimReport};

use crate::{Chunk, DeleteResult, Error, InventoryEntry, Result, budget::Permit};

pub(crate) type Reply<T> = oneshot::Sender<Result<T>>;

pub(crate) enum Command {
    Read {
        id: ChunkId,
        range: Option<Range<u64>>,
        reply: Reply<Option<Arc<Chunk>>>,
        permit: Permit,
    },
    Put {
        id: ChunkId,
        value: Arc<[u8]>,
        reply: Reply<u64>,
        permit: Permit,
    },
    Delete {
        id: ChunkId,
        expected_lsn: Option<u64>,
        reply: Reply<DeleteResult>,
        permit: Permit,
    },
    Fence {
        reply: FenceReply,
        permit: Option<Permit>,
    },
}
impl Command {
    pub fn fail(self, error: Error) {
        match self {
            Self::Read { reply, permit, .. } => {
                drop(permit);
                let _ = reply.send(Err(error));
            }
            Self::Put { reply, permit, .. } => {
                drop(permit);
                let _ = reply.send(Err(error));
            }
            Self::Delete { reply, permit, .. } => {
                drop(permit);
                let _ = reply.send(Err(error));
            }
            Self::Fence { reply, permit } => {
                drop(permit);
                reply.fail(error);
            }
        }
    }
}

pub(crate) enum FenceReply {
    Flush(Reply<()>),
    Inventory(Reply<Vec<InventoryEntry>>),
    Reclaim(Reply<Option<ReclaimReport>>),
    Close(Option<Reply<()>>),
}
impl FenceReply {
    pub fn fail(self, error: Error) {
        match self {
            Self::Flush(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Inventory(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Reclaim(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Close(Some(reply)) => {
                let _ = reply.send(Err(error));
            }
            Self::Close(None) => {}
        }
    }
}
pub(crate) struct Fence {
    pub reply: FenceReply,
    pub _permit: Option<Permit>,
    pub ticket: Option<u64>,
}

pub(crate) struct Waiter {
    pub reply: Reply<Option<Arc<Chunk>>>,
    pub permit: Option<Permit>,
}
pub(crate) enum Operation {
    Read {
        range: Option<Range<u64>>,
        lsn: u64,
        waiters: Vec<Waiter>,
    },
    Put {
        value: Arc<[u8]>,
        reply: Reply<u64>,
        permit: Permit,
    },
    Delete {
        expected_lsn: Option<u64>,
        reply: Reply<DeleteResult>,
        permit: Permit,
    },
}
impl Operation {
    pub fn read_done(self, result: Result<ChunkData>) {
        let Self::Read { lsn, mut waiters, .. } = self else {
            unreachable!("read ticket")
        };
        let result = result.map(|data| {
            let (buf, range) = data.into_raw();
            let mut permit = waiters[0].permit.take().expect("leader byte credit");
            permit.resize(buf.len());
            permit.finish_request();
            Some(Arc::new(Chunk {
                buf,
                range,
                lsn,
                _permit: permit,
                retention: std::sync::OnceLock::new(),
            }))
        });
        for waiter in waiters {
            drop(waiter.permit);
            let _ = waiter.reply.send(result.clone());
        }
    }
    pub fn miss(self) {
        let Self::Read { waiters, .. } = self else {
            unreachable!("read operation")
        };
        for waiter in waiters {
            drop(waiter.permit);
            let _ = waiter.reply.send(Ok(None));
        }
    }
    pub fn write_done(self, result: Result<Outcome>) {
        match self {
            Self::Put { reply, permit, .. } => {
                drop(permit);
                let result = result.and_then(|outcome| match outcome {
                    Outcome::Put { lsn } => Ok(lsn),
                    _ => Err(Error::Invalid("unexpected put completion")),
                });
                let _ = reply.send(result);
            }
            Self::Delete { reply, permit, .. } => {
                drop(permit);
                let result = result.and_then(|outcome| match outcome {
                    Outcome::Delete { lsn } => Ok(DeleteResult::Deleted(lsn)),
                    _ => Err(Error::Invalid("unexpected delete completion")),
                });
                let _ = reply.send(result);
            }
            Self::Read { .. } => unreachable!("write ticket"),
        }
    }
    pub fn deleted(self, result: DeleteResult) {
        let Self::Delete { reply, permit, .. } = self else {
            unreachable!("delete operation")
        };
        drop(permit);
        let _ = reply.send(Ok(result));
    }
    pub fn fail(self, error: Error) {
        match self {
            Self::Read { waiters, .. } => {
                for waiter in waiters {
                    drop(waiter.permit);
                    let _ = waiter.reply.send(Err(error.clone()));
                }
            }
            Self::Put { reply, permit, .. } => {
                drop(permit);
                let _ = reply.send(Err(error));
            }
            Self::Delete { reply, permit, .. } => {
                drop(permit);
                let _ = reply.send(Err(error));
            }
        }
    }
}
