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

//! Blocking conveniences for tests and single-threaded tools.
//!
//! The engine itself never waits; these helpers drive a queue and a pipeline
//! until a particular completion arrives. They are the only place where the
//! worker loop of `engine-api.md` is spelled out inside this crate, and they
//! are not meant for the data path of a server.

use std::ops::Range;

use moat_common::ChunkId;

use crate::{
    error::{Error, Result},
    io::IoQueue,
    reader::{ChunkData, ReadOutcome, Reader},
    writer::{Completion, Outcome, Ticket, Writer},
};

/// Drives `q` and `w` until the completion for `ticket` arrives and returns
/// its outcome. Other completions delivered meanwhile are appended to `others`.
pub fn wait_with(q: &mut dyn IoQueue, w: &mut Writer, ticket: Ticket, others: &mut Vec<Completion>) -> Result<Outcome> {
    let mut out = Vec::new();
    loop {
        w.poll(q, &mut out)?;
        if let Some(pos) = out.iter().position(|c| c.ticket == ticket) {
            let done = out.swap_remove(pos);
            others.append(&mut out);
            return done.result;
        }
        others.append(&mut out);
        if w.in_flight() == 0 && q.in_flight() == 0 && w.is_idle() {
            return Err(Error::InvalidOption(format!("ticket {ticket} is not pending")));
        }
        q.poll(true)?;
    }
}

/// Drives `q` and `w` until the completion for `ticket` arrives and returns
/// its outcome. Other completions delivered meanwhile are discarded.
pub fn wait(q: &mut dyn IoQueue, w: &mut Writer, ticket: Ticket) -> Result<Outcome> {
    let mut others = Vec::new();
    wait_with(q, w, ticket, &mut others)
}

/// Issues a barrier and waits for it: every previous write is durable and
/// visible afterwards. Returns the barrier's result.
pub fn flush(q: &mut dyn IoQueue, w: &mut Writer) -> Result<()> {
    let ticket = w.flush(q)?;
    wait(q, w, ticket).map(|_| ())
}

/// Seals both active segments and waits for it.
pub fn seal(q: &mut dyn IoQueue, w: &mut Writer) -> Result<()> {
    let ticket = w.seal(q)?;
    wait(q, w, ticket).map(|_| ())
}

/// Runs one reclaim pass to completion. `None` if there was nothing to
/// reclaim.
pub fn reclaim(q: &mut dyn IoQueue, w: &mut Writer) -> Result<Option<crate::writer::ReclaimReport>> {
    let Some(ticket) = w.reclaim(q)? else {
        return Ok(None);
    };
    match wait(q, w, ticket)? {
        Outcome::Reclaim(report) => Ok(Some(report)),
        other => Err(Error::InvalidOption(format!("unexpected outcome {other:?}"))),
    }
}

/// Reads a chunk synchronously: submits, waits, returns the value (or the
/// requested range of it).
pub fn get(q: &mut dyn IoQueue, r: &mut Reader, id: &ChunkId, range: Option<Range<u64>>) -> Result<Option<ChunkData>> {
    const TOKEN: u64 = u64::MAX;
    match r.get(q, id, range, TOKEN)? {
        ReadOutcome::Miss => return Ok(None),
        ReadOutcome::Submitted => {}
    }
    let mut out = Vec::with_capacity(1);
    loop {
        r.poll(q, &mut out)?;
        if let Some(pos) = out.iter().position(|c| c.token == TOKEN) {
            return out.swap_remove(pos).result.map(Some);
        }
        q.poll(true)?;
    }
}

/// Polls until the writer has nothing outstanding (after `seal` this means
/// it is safe to detach).
pub fn drain(q: &mut dyn IoQueue, w: &mut Writer) -> Result<Vec<Completion>> {
    let mut out = Vec::new();
    loop {
        w.poll(q, &mut out)?;
        if w.is_idle() {
            return Ok(out);
        }
        q.poll(true)?;
    }
}
