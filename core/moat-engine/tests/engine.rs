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

//! End-to-end tests of the engine on an in-memory device.
//!
//! Segments are kept tiny (1 MiB) so that a handful of writes exercises
//! rollover, sealing, reclaim and recovery. Everything runs through the
//! blocking `SyncQueue`, optionally with reversed completion order to
//! exercise out-of-order completion handling.

use std::{collections::HashMap, sync::Arc};

use moat_common::{ChunkId, HugePages, PoolOptions};
use moat_engine::{
    DeleteOutcome, Engine, Error, FormatOptions, IoQueue, ManualClock, MemDevice, Options, Outcome, PutOptions,
    PutOutcome, QueueOptions, Reader, ReclaimPolicy, Writer, blocking,
    io::{CompletionOrder, SyncQueue},
};

const SEGMENT: u64 = 1 << 20;
const CHUNK_MAX: u32 = 128 << 10;
const FILE_CHUNK_MAX: u32 = 64 << 10;

fn format_options() -> FormatOptions {
    FormatOptions {
        segment_size: SEGMENT,
        chunk_max: CHUNK_MAX,
        disk_uuid: [0xab; 16],
    }
}

/// A small pool keeps test memory modest; plain pages avoid depending on the
/// host's huge page configuration.
fn queue_options() -> QueueOptions {
    QueueOptions {
        depth: 64,
        pool: PoolOptions {
            bytes: 16 << 20,
            max_class: 1 << 20,
            huge_pages: HugePages::Disabled,
        },
        descriptors: 8,
    }
}

fn options() -> Options {
    Options {
        index_capacity: 1024,
        ..Default::default()
    }
}

fn queue() -> SyncQueue {
    SyncQueue::new(&queue_options(), CompletionOrder::Fifo).unwrap()
}

fn new_device(segments: u64) -> Arc<MemDevice> {
    let device = Arc::new(MemDevice::new(SEGMENT * (segments + 1)));
    moat_engine::format(&*device, &format_options()).unwrap();
    device
}

fn open(device: &Arc<MemDevice>) -> Engine {
    moat_engine::open(device.clone(), options()).unwrap().0
}

fn open_with(device: &Arc<MemDevice>, options: Options) -> Engine {
    moat_engine::open(device.clone(), options).unwrap().0
}

/// Opens the engine and attaches a writer and a reader to a fresh queue.
fn setup(device: &Arc<MemDevice>) -> (Engine, SyncQueue, Writer, Reader) {
    let engine = open(device);
    let mut q = queue();
    let writer = engine.writer(&mut q).unwrap();
    let reader = engine.reader(&mut q).unwrap();
    (engine, q, writer, reader)
}

fn id(n: u128) -> ChunkId {
    ChunkId::from_u128(n)
}

/// Reads a whole chunk into an owned vector for comparisons.
fn read(q: &mut dyn IoQueue, reader: &mut Reader, id: &ChunkId) -> Option<Vec<u8>> {
    blocking::get(q, reader, id, None).unwrap().map(|d| d.to_vec())
}

fn flush(q: &mut dyn IoQueue, writer: &mut Writer) {
    blocking::flush(q, writer).unwrap();
}

/// `put` with the back-pressure loop a worker would run: on `Busy` (the pool is
/// out of buffers because completions have not been reaped) poll and retry.
fn put(q: &mut dyn IoQueue, writer: &mut Writer, id: ChunkId, value: &[u8], opts: PutOptions) -> PutOutcome {
    let mut done = Vec::new();
    loop {
        match writer.put(q, id, value, opts) {
            Ok(outcome) => return outcome,
            Err(Error::Busy) => {
                q.poll(true).unwrap();
                writer.poll(q, &mut done).unwrap();
                for c in done.drain(..) {
                    c.result.unwrap();
                }
            }
            Err(e) => panic!("put failed: {e}"),
        }
    }
}

fn seal(q: &mut dyn IoQueue, writer: &mut Writer) {
    blocking::seal(q, writer).unwrap();
}

fn reclaim(q: &mut dyn IoQueue, writer: &mut Writer, policy: ReclaimPolicy) -> Option<moat_engine::ReclaimReport> {
    blocking::reclaim(q, writer, policy).unwrap()
}

fn delete(q: &mut dyn IoQueue, writer: &mut Writer, id: &ChunkId) -> bool {
    match writer.delete(q, id).unwrap() {
        DeleteOutcome::Deleted { ticket, .. } => {
            assert!(matches!(
                blocking::wait(q, writer, ticket).unwrap(),
                Outcome::Delete { .. }
            ));
            true
        }
        DeleteOutcome::Missing => false,
    }
}

/// Seals and detaches the writer cleanly, as a shutdown would.
fn close(q: &mut dyn IoQueue, writer: Writer) {
    let mut writer = writer;
    seal(q, &mut writer);
    blocking::drain(q, &mut writer).unwrap();
    assert!(writer.is_idle());
    writer.detach(q);
}

/// A deterministic value for `(key, version)` whose every byte can be checked.
fn value_for(key: u128, version: u32, len: usize) -> Vec<u8> {
    // Keys that differ in any bit must yield different streams (an earlier
    // `seed | 1` made neighbouring keys collide).
    let seed = (key as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ ((version as u64) << 40) ^ 0x5bd1_e995;
    let mut x = seed ^ (seed >> 29);
    if x == 0 {
        x = 1;
    }
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Value length distribution that covers inline-sized, packed and large records.
fn random_len(rng: &mut XorShift) -> usize {
    match rng.below(10) {
        0 => 0,
        1..=4 => rng.below(4096) as usize,
        5..=7 => rng.below(64 << 10) as usize,
        _ => (64 << 10) + rng.below((CHUNK_MAX as u64) - (64 << 10) + 1) as usize,
    }
}

#[test]
fn put_get_roundtrip_across_sizes() {
    let device = new_device(16);
    let (engine, mut q, mut writer, mut reader) = setup(&device);

    let sizes = [
        0usize,
        1,
        100,
        4095,
        4096,
        4097,
        65535,
        65536,
        65537,
        100_000,
        CHUNK_MAX as usize,
    ];
    for (i, &len) in sizes.iter().enumerate() {
        let value = value_for(i as u128, 0, len);
        assert!(matches!(
            writer
                .put(&mut q, id(i as u128), &value, PutOptions::default())
                .unwrap(),
            PutOutcome::Written { .. }
        ));
    }
    flush(&mut q, &mut writer);

    for (i, &len) in sizes.iter().enumerate() {
        let got = read(&mut q, &mut reader, &id(i as u128)).expect("present");
        assert_eq!(got, value_for(i as u128, 0, len), "size {len}");
        assert_eq!(reader.stat(&id(i as u128)).unwrap().len as usize, len);
        assert_eq!(engine.stat(&id(i as u128)).unwrap().len as usize, len);
    }
    assert!(read(&mut q, &mut reader, &id(999)).is_none());
    assert!(reader.stat(&id(999)).is_none());

    let too_big = vec![0u8; CHUNK_MAX as usize + 1];
    assert!(matches!(
        writer.put(&mut q, id(1000), &too_big, PutOptions::default()),
        Err(Error::ValueTooLarge { .. })
    ));
    // A second writer is refused until the first detaches.
    assert!(matches!(engine.writer(&mut q), Err(Error::Busy)));
    close(&mut q, writer);
    let writer = engine.writer(&mut q).unwrap();
    writer.detach(&mut q);
}

#[test]
fn zero_copy_large_put() {
    let device = new_device(4);
    let (_engine, mut q, mut writer, mut reader) = setup(&device);
    let value = value_for(3, 0, 100_000);

    let mut large = writer.prepare_large(&mut q, value.len() as u32).unwrap();
    large.value_mut().copy_from_slice(&value);
    let sums = moat_common::block_checksums(&value);
    let outcome = writer
        .put_large(&mut q, id(3), large, Some(&sums), PutOptions::default())
        .unwrap();
    assert!(matches!(outcome, PutOutcome::Written { .. }));
    flush(&mut q, &mut writer);
    assert_eq!(read(&mut q, &mut reader, &id(3)).unwrap(), value);

    // Wrong checksum count is rejected before anything is written.
    let large = writer.prepare_large(&mut q, value.len() as u32).unwrap();
    assert!(matches!(
        writer.put_large(&mut q, id(4), large, Some(&sums[..1]), PutOptions::default()),
        Err(Error::InvalidOption(_))
    ));
    // The producer may also leave checksum computation to the writer.
    let mut large = writer.prepare_large(&mut q, value.len() as u32).unwrap();
    large.value_mut().copy_from_slice(&value);
    writer
        .put_large(&mut q, id(5), large, None, PutOptions::default())
        .unwrap();
    flush(&mut q, &mut writer);
    assert_eq!(read(&mut q, &mut reader, &id(5)).unwrap(), value);
    // Values below the pack threshold are not large.
    assert!(matches!(
        writer.prepare_large(&mut q, 100),
        Err(Error::InvalidOption(_))
    ));

    // The returned data is a view into a pool buffer, usable without copying.
    let data = blocking::get(&mut q, &mut reader, &id(3), Some(10..20))
        .unwrap()
        .unwrap();
    assert_eq!(&*data, &value[10..20]);
    let (buf, range) = data.into_raw();
    assert_eq!(&buf[range], &value[10..20]);
}

#[test]
fn async_tickets_and_completions() {
    let device = new_device(8);
    let (_engine, mut q, mut writer, mut reader) = setup(&device);
    let mut tickets = HashMap::new();
    for i in 0..40u128 {
        let v = value_for(i, 0, if i % 4 == 0 { 70_000 } else { 3_000 });
        if let PutOutcome::Written { ticket, lsn } = writer.put(&mut q, id(i), &v, PutOptions::default()).unwrap() {
            tickets.insert(ticket, (i, lsn));
        }
    }
    // Small records are still pending: not visible yet.
    assert!(reader.stat(&id(1)).is_none());
    // Without a flush, every ticket still completes through polling: the
    // pending batch is closed at the end of a poll.
    let mut done = Vec::new();
    while !tickets.is_empty() {
        q.poll(true).unwrap();
        writer.poll(&mut q, &mut done).unwrap();
        for c in done.drain(..) {
            let (i, lsn) = tickets.remove(&c.ticket).expect("known ticket");
            assert_eq!(c.result.unwrap(), Outcome::Put { lsn });
            assert_eq!(reader.stat(&id(i)).unwrap().lsn, lsn);
        }
    }
    for i in 0..40u128 {
        assert!(read(&mut q, &mut reader, &id(i)).is_some());
    }
    // A barrier completes after everything before it, with its own outcome.
    let t = writer.flush(&mut q).unwrap();
    assert_eq!(blocking::wait(&mut q, &mut writer, t).unwrap(), Outcome::Flush);
    let t = writer.seal(&mut q).unwrap();
    assert_eq!(blocking::wait(&mut q, &mut writer, t).unwrap(), Outcome::Seal);
    assert!(writer.is_idle());
}

#[test]
fn out_of_order_completions_are_applied_in_order() {
    let device = new_device(12);
    let engine = open(&device);
    let mut q = SyncQueue::new(&queue_options(), CompletionOrder::Reverse).unwrap();
    let mut writer = engine.writer(&mut q).unwrap();
    let mut reader = engine.reader(&mut q).unwrap();
    let overwrite = PutOptions {
        overwrite: true,
        ..Default::default()
    };
    let mut expected = HashMap::new();
    let mut rng = XorShift(0x5151);
    // Many overwrites of a small key set with mixed sizes: large records are
    // submitted immediately while small ones ride in packed batches, so the
    // same key's versions complete in scrambled order.
    for round in 0..30u32 {
        for k in 0..12u128 {
            let v = value_for(k, round, random_len(&mut rng));
            put(&mut q, &mut writer, id(k), &v, overwrite);
            expected.insert(k, v);
        }
    }
    flush(&mut q, &mut writer);
    for (k, v) in &expected {
        assert_eq!(read(&mut q, &mut reader, &id(*k)).unwrap(), *v, "key {k}");
    }
    drop(writer);
    drop(reader);
    let (_engine, mut q, _writer, mut reader) = setup(&device);
    for (k, v) in &expected {
        assert_eq!(read(&mut q, &mut reader, &id(*k)).unwrap(), *v, "key {k} after reopen");
    }
}

#[test]
fn write_failure_truncates_segment_and_continues() {
    let device = new_device(8);
    let (engine, mut q, mut writer, mut reader) = setup(&device);
    let mut ok_keys = Vec::new();
    for i in 0..3u128 {
        writer
            .put(&mut q, id(i), &value_for(i, 0, 70_000), PutOptions::default())
            .unwrap();
        ok_keys.push(i);
    }
    flush(&mut q, &mut writer);

    // The hot segment is segment 0 at device offset SEGMENT; fail every write
    // into its second half.
    device.fail_writes_in(Some(SEGMENT + SEGMENT / 2..2 * SEGMENT));
    let mut outcomes = HashMap::new();
    for i in 10..30u128 {
        if let PutOutcome::Written { ticket, .. } = writer
            .put(&mut q, id(i), &value_for(i, 0, 70_000), PutOptions::default())
            .unwrap()
        {
            outcomes.insert(ticket, i);
        }
    }
    let mut done = Vec::new();
    let (mut succeeded, mut failed) = (Vec::new(), Vec::new());
    while !outcomes.is_empty() {
        q.poll(true).unwrap();
        writer.poll(&mut q, &mut done).unwrap();
        for c in done.drain(..) {
            let i = outcomes.remove(&c.ticket).unwrap();
            match c.result {
                Ok(Outcome::Put { .. }) => succeeded.push(i),
                Err(Error::Io(_)) => failed.push(i),
                other => panic!("unexpected {other:?}"),
            }
        }
    }
    assert!(!failed.is_empty(), "the fault must have hit");
    device.fail_writes_in(None);

    // Writing continues on a fresh segment.
    for i in 100..105u128 {
        writer
            .put(&mut q, id(i), &value_for(i, 0, 70_000), PutOptions::default())
            .unwrap();
    }
    flush(&mut q, &mut writer);

    let check = |q: &mut dyn IoQueue, reader: &mut Reader, engine: &Engine| {
        for i in ok_keys.iter().chain(succeeded.iter()).copied().chain(100..105u128) {
            assert_eq!(read(q, reader, &id(i)).unwrap(), value_for(i, 0, 70_000), "key {i}");
        }
        for i in &failed {
            assert!(!engine.contains(&id(*i)), "failed key {i} must not be indexed");
        }
    };
    check(&mut q, &mut reader, &engine);
    drop(writer);
    drop(reader);
    let (engine, report) = moat_engine::open(device.clone(), options()).unwrap();
    let mut q = queue();
    let mut reader = engine.reader(&mut q).unwrap();
    check(&mut q, &mut reader, &engine);
    assert_eq!(report.chunks, ok_keys.len() + succeeded.len() + 5);
}

#[test]
fn lsn_is_monotonic_and_reported() {
    let device = new_device(4);
    let (_engine, mut q, mut writer, _reader) = setup(&device);
    let mut last = 0;
    for i in 0..50u128 {
        let PutOutcome::Written { lsn, .. } = writer.put(&mut q, id(i), b"x", PutOptions::default()).unwrap() else {
            panic!("expected write");
        };
        assert!(lsn > last);
        last = lsn;
    }
    assert_eq!(writer.next_lsn(), last + 1);
    // A re-created writer continues the sequence.
    flush(&mut q, &mut writer);
    let engine = open(&device);
    let mut q2 = queue();
    let writer2 = engine.writer(&mut q2).unwrap();
    assert!(writer2.next_lsn() > last);
}

#[test]
fn range_reads_cover_boundaries_in_both_modes() {
    for verify_reads in [false, true] {
        let device = new_device(8);
        let engine = open_with(
            &device,
            Options {
                verify_reads,
                ..options()
            },
        );
        let mut q = queue();
        let mut writer = engine.writer(&mut q).unwrap();
        let mut reader = engine.reader(&mut q).unwrap();
        for (i, len) in [0, 1000, 4096, 65536, 100_000, CHUNK_MAX as usize]
            .into_iter()
            .enumerate()
        {
            let key = id(i as u128);
            let value = value_for(i as u128, 0, len);
            writer.put(&mut q, key, &value, PutOptions::default()).unwrap();
            flush(&mut q, &mut writer);
            for range in [
                0..0,
                0..10,
                100..100,
                4090..4110,
                65530..65540,
                70_000..70_010,
                99_990..200_000,
                200_000..300_000,
            ] {
                let start = (range.start as usize).min(len);
                let end = (range.end as usize).min(len).max(start);
                let got = blocking::get(&mut q, &mut reader, &key, Some(range)).unwrap().unwrap();
                assert_eq!(&*got, &value[start..end], "len={len}, verify_reads={verify_reads}");
            }
            assert_eq!(read(&mut q, &mut reader, &key).unwrap(), value);
        }
    }
}

#[test]
fn unchecked_range_reads_need_only_one_page() {
    let device = new_device(4);
    let (_engine, mut q, mut writer, mut reader) = setup(&device);
    assert!(!Options::default().verify_reads);
    let value = value_for(1, 0, CHUNK_MAX as usize);
    writer.put(&mut q, id(1), &value, PutOptions::default()).unwrap();
    flush(&mut q, &mut writer);
    for range in [0..10, 70_000..70_010, CHUNK_MAX as u64..CHUNK_MAX as u64] {
        let data = blocking::get(&mut q, &mut reader, &id(1), Some(range.clone()))
            .unwrap()
            .unwrap();
        assert_eq!(&*data, &value[range.start as usize..range.end as usize]);
        let (buf, _) = data.into_raw();
        assert_eq!(buf.capacity(), 4096);
    }
}

#[test]
fn overwrite_semantics() {
    let device = new_device(4);
    let (_engine, mut q, mut writer, mut reader) = setup(&device);
    let v0 = value_for(1, 0, 3000);
    let v1 = value_for(1, 1, 90_000);
    let v2 = value_for(1, 2, 10);

    let PutOutcome::Written { lsn: lsn0, .. } = writer.put(&mut q, id(1), &v0, PutOptions::default()).unwrap() else {
        panic!()
    };
    // A duplicate is rejected even while the first put is still pending.
    assert_eq!(
        writer.put(&mut q, id(1), &v1, PutOptions::default()).unwrap(),
        PutOutcome::Exists
    );
    flush(&mut q, &mut writer);
    assert_eq!(
        writer.put(&mut q, id(1), &v1, PutOptions::default()).unwrap(),
        PutOutcome::Exists
    );
    assert_eq!(read(&mut q, &mut reader, &id(1)).unwrap(), v0);

    let overwrite = PutOptions {
        overwrite: true,
        ..Default::default()
    };
    let PutOutcome::Written { lsn: lsn1, .. } = writer.put(&mut q, id(1), &v1, overwrite).unwrap() else {
        panic!()
    };
    assert!(lsn1 > lsn0);
    // A large record is submitted immediately but only visible once applied.
    flush(&mut q, &mut writer);
    assert_eq!(read(&mut q, &mut reader, &id(1)).unwrap(), v1);
    assert_eq!(reader.stat(&id(1)).unwrap().lsn, lsn1);

    // A small overwrite following a large one: the pending small record has a
    // higher LSN and must win once flushed.
    writer.put(&mut q, id(1), &v2, overwrite).unwrap();
    flush(&mut q, &mut writer);
    assert_eq!(read(&mut q, &mut reader, &id(1)).unwrap(), v2);

    // The reverse: a small pending record followed by a large one. The large
    // record reaches the device first but has the higher LSN and must win.
    let v3 = value_for(1, 3, 20);
    let v4 = value_for(1, 4, 70_000);
    writer.put(&mut q, id(1), &v3, overwrite).unwrap();
    writer.put(&mut q, id(1), &v4, overwrite).unwrap();
    flush(&mut q, &mut writer);
    assert_eq!(read(&mut q, &mut reader, &id(1)).unwrap(), v4);
}

#[test]
fn delete_is_durable_across_reopen() {
    let device = new_device(4);
    {
        let (_engine, mut q, mut writer, mut reader) = setup(&device);
        writer.put(&mut q, id(1), b"a", PutOptions::default()).unwrap();
        writer.put(&mut q, id(2), b"b", PutOptions::default()).unwrap();
        flush(&mut q, &mut writer);
        assert!(delete(&mut q, &mut writer, &id(1)));
        assert!(!delete(&mut q, &mut writer, &id(1)));
        assert!(!delete(&mut q, &mut writer, &id(3)));
        assert!(read(&mut q, &mut reader, &id(1)).is_none());
        assert_eq!(read(&mut q, &mut reader, &id(2)).unwrap(), b"b");
        // Deliberately no seal: recovery must find the tombstone by scanning.
    }
    let (engine, report) = moat_engine::open(device.clone(), options()).unwrap();
    let mut q = queue();
    let mut reader = engine.reader(&mut q).unwrap();
    assert_eq!(report.chunks, 1);
    assert!(read(&mut q, &mut reader, &id(1)).is_none());
    assert_eq!(read(&mut q, &mut reader, &id(2)).unwrap(), b"b");
}

#[test]
fn delete_interleaved_with_pending_puts() {
    let device = new_device(4);
    let (_engine, mut q, mut writer, mut reader) = setup(&device);
    // Delete of a key whose put is still pending: the tombstone outranks it,
    // and the key is free for a new put at once (not `Exists`).
    writer.put(&mut q, id(1), b"pending", PutOptions::default()).unwrap();
    assert!(matches!(
        writer.delete(&mut q, &id(1)).unwrap(),
        DeleteOutcome::Deleted { .. }
    ));
    assert!(matches!(
        writer.put(&mut q, id(1), b"x", PutOptions::default()).unwrap(),
        PutOutcome::Written { .. }
    ));
    writer.put(&mut q, id(4), b"pending", PutOptions::default()).unwrap();
    assert!(matches!(
        writer.delete(&mut q, &id(4)).unwrap(),
        DeleteOutcome::Deleted { .. }
    ));
    assert!(matches!(writer.delete(&mut q, &id(4)).unwrap(), DeleteOutcome::Missing));
    // Large put (submitted at once) then a small pending put of the same key,
    // then a delete, then a new put: the last put must be what remains.
    let overwrite = PutOptions {
        overwrite: true,
        ..Default::default()
    };
    writer.put(&mut q, id(2), &value_for(2, 0, 70_000), overwrite).unwrap();
    writer.put(&mut q, id(2), b"small", overwrite).unwrap();
    assert!(matches!(
        writer.delete(&mut q, &id(2)).unwrap(),
        DeleteOutcome::Deleted { .. }
    ));
    writer.put(&mut q, id(2), b"final", overwrite).unwrap();
    // A framed record pending in the other staging batch, then deleted; the
    // framed batch is closed after the inline one that holds the tombstone.
    writer.put(&mut q, id(3), &value_for(3, 0, 4096), overwrite).unwrap();
    assert!(matches!(
        writer.delete(&mut q, &id(3)).unwrap(),
        DeleteOutcome::Deleted { .. }
    ));
    flush(&mut q, &mut writer);
    let check = |q: &mut dyn IoQueue, reader: &mut Reader| {
        assert_eq!(read(q, reader, &id(1)).unwrap(), b"x");
        assert_eq!(read(q, reader, &id(2)).unwrap(), b"final");
        assert!(read(q, reader, &id(3)).is_none());
        assert!(read(q, reader, &id(4)).is_none());
    };
    check(&mut q, &mut reader);
    drop(writer);
    drop(reader);
    let (_engine, mut q, _writer, mut reader) = setup(&device);
    check(&mut q, &mut reader);
}

#[test]
fn recovery_from_footers_and_scan() {
    let device = new_device(16);
    let mut rng = XorShift(0x1234);
    let mut expected = HashMap::new();
    {
        let (_engine, mut q, mut writer, _reader) = setup(&device);
        for i in 0..400u128 {
            let len = random_len(&mut rng);
            let v = value_for(i, 0, len);
            put(&mut q, &mut writer, id(i), &v, PutOptions::default());
            expected.insert(i, v);
        }
        flush(&mut q, &mut writer);
        // Drop without sealing: the hot segment stays active.
    }
    let (engine, report) = moat_engine::open(device.clone(), options()).unwrap();
    let mut q = queue();
    let mut reader = engine.reader(&mut q).unwrap();
    let writer = engine.writer(&mut q).unwrap();
    assert!(report.scanned >= 1, "an active segment must have been scanned");
    assert!(report.sealed >= 1, "earlier segments must have been sealed");
    assert_eq!(report.chunks, 400);
    for (i, v) in &expected {
        assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v);
    }
    close(&mut q, writer);

    let (engine, report) = moat_engine::open(device.clone(), options()).unwrap();
    let mut q = queue();
    let mut reader = engine.reader(&mut q).unwrap();
    assert_eq!(report.scanned, 0, "a clean shutdown seals everything");
    assert_eq!(report.chunks, 400);
    for (i, v) in &expected {
        assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v);
    }
}

/// Simulates a crash in the middle of the last writes: bytes written after a
/// known-good point are damaged (flipped or zeroed). Acknowledged data before
/// the point must survive; damaged records must read as missing or corrupt,
/// never as wrong bytes.
#[test]
fn torn_tail_never_returns_wrong_data() {
    for mode in ["flip", "zero"] {
        let device = new_device(8);
        let mut rng = XorShift(0x77);
        let mut safe = HashMap::new();
        let mut unsafe_keys = HashMap::new();
        {
            let (_engine, mut q, mut writer, _reader) = setup(&device);
            for i in 0..60u128 {
                let v = value_for(i, 0, random_len(&mut rng));
                writer.put(&mut q, id(i), &v, PutOptions::default()).unwrap();
                safe.insert(i, v);
            }
            flush(&mut q, &mut writer);
            let before = device.with_data(|d| d.to_vec());
            for i in 100..130u128 {
                let v = value_for(i, 0, random_len(&mut rng));
                writer.put(&mut q, id(i), &v, PutOptions::default()).unwrap();
                unsafe_keys.insert(i, v);
            }
            flush(&mut q, &mut writer);
            // Damage every page that changed after the safe point.
            device.with_data_mut(|after| {
                let mut damaged = 0;
                for (page, (a, b)) in after.chunks_mut(4096).zip(before.chunks(4096)).enumerate() {
                    if a != b {
                        // Segment header pages are written atomically by real
                        // devices (single logical block); do not damage them.
                        if (page as u64 * 4096).is_multiple_of(SEGMENT) {
                            continue;
                        }
                        // Leave every other changed page intact so some later
                        // records survive and some do not.
                        if page % 2 == 0 {
                            continue;
                        }
                        damaged += 1;
                        match mode {
                            "flip" => a[100] ^= 0xff,
                            _ => a.fill(0),
                        }
                    }
                }
                assert!(damaged > 0);
            });
        }
        let (_engine, mut q, _writer, mut reader) = setup(&device);
        for (i, v) in &safe {
            assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v, "mode {mode} key {i}");
        }
        let mut survived = 0;
        for (i, v) in &unsafe_keys {
            match blocking::get(&mut q, &mut reader, &id(*i), None) {
                Ok(Some(got)) => {
                    assert_eq!(&*got, &v[..], "mode {mode} key {i}");
                    survived += 1;
                }
                Ok(None) | Err(Error::Corrupt(_)) => {}
                Err(e) => panic!("unexpected error {e}"),
            }
        }
        assert!(survived < unsafe_keys.len(), "damage must have removed something");
    }
}

#[test]
fn bit_rot_in_sealed_value_is_detected_and_reclaim_drops_it() {
    for verify_reads in [false, true] {
        let device = new_device(4);
        let engine = open_with(
            &device,
            Options {
                verify_reads,
                ..options()
            },
        );
        let mut q = queue();
        let mut writer = engine.writer(&mut q).unwrap();
        let mut reader = engine.reader(&mut q).unwrap();
        let v = value_for(5, 0, 90_000);
        writer.put(&mut q, id(5), &v, PutOptions::default()).unwrap();
        writer.put(&mut q, id(6), b"neighbour", PutOptions::default()).unwrap();
        seal(&mut q, &mut writer);
        let before = device.with_data(|d| d.to_vec());
        // Find the value on disk and flip one byte deep inside it.
        let pos = before
            .windows(64)
            .position(|w| w == &v[1000..1064])
            .expect("value on disk");
        device.with_data_mut(|d| d[pos + 70_000] ^= 1);

        // Block 0 (first 64 KiB) is intact; a range inside it still reads.
        assert_eq!(
            &*blocking::get(&mut q, &mut reader, &id(5), Some(0..100))
                .unwrap()
                .unwrap(),
            &v[0..100]
        );
        // Enabling verification rejects corruption on every read, even when the
        // damaged byte is outside the requested range but in a touched block.
        for range in [None, None, Some(70_000..70_010)] {
            let result = blocking::get(&mut q, &mut reader, &id(5), range.clone());
            if verify_reads {
                assert!(matches!(result, Err(Error::Corrupt(_))));
            } else {
                let mut damaged = v.clone();
                damaged[71_000] ^= 1;
                let range = range.unwrap_or(0..v.len() as u64);
                assert_eq!(
                    &*result.unwrap().unwrap(),
                    &damaged[range.start as usize..range.end as usize]
                );
            }
        }
        // Reclaim drops the rotten record instead of copying it forward, and
        // keeps its intact neighbour.
        let report = reclaim(&mut q, &mut writer, ReclaimPolicy::Storage).unwrap();
        assert_eq!(report.corrupt, 1);
        assert_eq!(report.relocated, 1);
        assert!(!engine.contains(&id(5)));
        assert_eq!(read(&mut q, &mut reader, &id(6)).unwrap(), b"neighbour");
    }
}

#[test]
fn header_validation_is_required_for_verification_or_expiry() {
    for verify_reads in [false, true] {
        for expire_at in [0, u64::MAX] {
            let device = new_device(4);
            let engine = open_with(
                &device,
                Options {
                    verify_reads,
                    ..options()
                },
            );
            let mut q = queue();
            let mut writer = engine.writer(&mut q).unwrap();
            let mut reader = engine.reader(&mut q).unwrap();
            let value = value_for(1, 0, 100_000);
            writer
                .put(
                    &mut q,
                    id(1),
                    &value,
                    PutOptions {
                        expire_at,
                        ..Default::default()
                    },
                )
                .unwrap();
            flush(&mut q, &mut writer);
            let stat = reader.stat(&id(1)).unwrap();
            let segment = SEGMENT * (stat.segment as u64 + 1);
            // The first large batch follows the segment header page.
            let header = segment as usize + 4096 + moat_engine::layout::BATCH_HEADER_LEN;
            device.with_data_mut(|data| data[header] ^= 1);
            for range in [0..10, 70000..70010, 100_000..100_000] {
                let result = blocking::get(&mut q, &mut reader, &id(1), Some(range.clone()));
                if verify_reads || expire_at != 0 {
                    assert!(matches!(result, Err(Error::Corrupt(_))));
                } else {
                    assert_eq!(
                        &*result.unwrap().unwrap(),
                        &value[range.start as usize..range.end as usize]
                    );
                }
            }
        }
    }
}

#[test]
fn corrupt_footer_falls_back_to_scan() {
    let device = new_device(6);
    let mut rng = XorShift(0x42);
    let mut expected = HashMap::new();
    {
        let (_engine, mut q, mut writer, _reader) = setup(&device);
        for i in 0..40u128 {
            let v = value_for(i, 0, random_len(&mut rng));
            writer.put(&mut q, id(i), &v, PutOptions::default()).unwrap();
            expected.insert(i, v);
        }
        close(&mut q, writer);
    }
    // Damage the first footer: it sits right after the data of segment 0, so
    // locate it by its magic.
    let magic = b"MOATFOT1";
    device.with_data_mut(|d| {
        let pos = d.windows(8).position(|w| w == magic).expect("footer present");
        d[pos + 100] ^= 0xff;
    });
    let (engine, report) = moat_engine::open(device.clone(), options()).unwrap();
    let mut q = queue();
    let mut reader = engine.reader(&mut q).unwrap();
    assert_eq!(report.bad_footers, 1);
    assert!(report.scanned >= 1);
    for (i, v) in &expected {
        assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v);
    }
    // The rescanned segment was re-sealed with a fresh footer.
    let (_engine, report) = moat_engine::open(device.clone(), options()).unwrap();
    assert_eq!(report.bad_footers, 0);
    assert_eq!(report.scanned, 0);
}

#[test]
fn reclaim_storage_keeps_every_live_chunk() {
    let device = new_device(20);
    let (engine, mut q, mut writer, mut reader) = setup(&device);
    let mut rng = XorShift(0xbeef);
    let mut model: HashMap<u128, Vec<u8>> = HashMap::new();
    let overwrite = PutOptions {
        overwrite: true,
        ..Default::default()
    };

    for round in 0..6u32 {
        for i in 0..150u128 {
            match rng.below(4) {
                0 if model.contains_key(&i) => {
                    // Deletes are not awaited: the tombstone rides in the
                    // pending batch like any record.
                    assert!(matches!(
                        writer.delete(&mut q, &id(i)).unwrap(),
                        DeleteOutcome::Deleted { .. }
                    ));
                    model.remove(&i);
                }
                _ => {
                    let v = value_for(i, round, random_len(&mut rng));
                    put(&mut q, &mut writer, id(i), &v, overwrite);
                    model.insert(i, v);
                }
            }
        }
        flush(&mut q, &mut writer);
        while writer.free_segments() < 6 {
            let report = reclaim(&mut q, &mut writer, ReclaimPolicy::Storage).expect("victim");
            assert_eq!(
                report.records,
                report.relocated + report.dropped + report.tombstones_relocated + report.tombstones_dropped
            );
        }
    }
    // A few extra passes exercise the tombstone rules on already-compacted
    // segments (relocated records and forwarded tombstones).
    for _ in 0..8 {
        if writer.free_segments() < 3 || reclaim(&mut q, &mut writer, ReclaimPolicy::Storage).is_none() {
            break;
        }
    }

    let check = |q: &mut dyn IoQueue, reader: &mut Reader| {
        for i in 0..150u128 {
            assert_eq!(read(q, reader, &id(i)), model.get(&i).cloned(), "key {i}");
        }
    };
    check(&mut q, &mut reader);
    let usage = engine.usage();
    assert_eq!(usage.chunks, model.len());

    drop(writer);
    drop(reader);
    let (_engine, mut q, _writer, mut reader) = setup(&device);
    check(&mut q, &mut reader);
}

#[test]
fn reclaim_runs_concurrently_with_foreground_writes() {
    let device = new_device(24);
    let (engine, mut q, mut writer, mut reader) = setup(&device);
    let overwrite = PutOptions {
        overwrite: true,
        ..Default::default()
    };
    let mut model: HashMap<u128, Vec<u8>> = HashMap::new();
    let mut rng = XorShift(0xc0ffee);
    for i in 0..200u128 {
        let v = value_for(i, 0, 20_000);
        put(&mut q, &mut writer, id(i), &v, PutOptions::default());
        model.insert(i, v);
    }
    seal(&mut q, &mut writer);

    // Start a reclaim pass and keep writing (overwrites and deletes of keys
    // the pass is relocating) while it runs; poll both to completion.
    let ticket = writer.reclaim(&mut q, ReclaimPolicy::Storage).unwrap().expect("victim");
    let mut done = Vec::new();
    let mut finished = false;
    let mut round = 1u32;
    while !finished {
        for _ in 0..5 {
            let k = rng.below(200) as u128;
            if rng.below(3) == 0 {
                match writer.delete(&mut q, &id(k)).unwrap() {
                    DeleteOutcome::Deleted { .. } => {
                        model.remove(&k);
                    }
                    DeleteOutcome::Missing => assert!(!model.contains_key(&k)),
                }
            } else {
                let v = value_for(k, round, 3_000 + (k as usize % 5) * 7_000);
                writer.put(&mut q, id(k), &v, overwrite).unwrap();
                model.insert(k, v);
            }
        }
        round += 1;
        q.poll(true).unwrap();
        writer.poll(&mut q, &mut done).unwrap();
        for c in done.drain(..) {
            if c.ticket == ticket {
                let Outcome::Reclaim(report) = c.result.unwrap() else {
                    panic!("not a reclaim outcome")
                };
                assert!(report.records > 0);
                finished = true;
            } else {
                c.result.unwrap();
            }
        }
    }
    flush(&mut q, &mut writer);
    for k in 0..200u128 {
        assert_eq!(read(&mut q, &mut reader, &id(k)), model.get(&k).cloned(), "key {k}");
    }
    assert_eq!(engine.usage().chunks, model.len());
    close(&mut q, writer);
    drop(reader);
    let (_engine, mut q, _writer, mut reader) = setup(&device);
    for k in 0..200u128 {
        assert_eq!(
            read(&mut q, &mut reader, &id(k)),
            model.get(&k).cloned(),
            "key {k} after reopen"
        );
    }
}

#[test]
fn reclaim_cache_evicts_oldest_and_reinserts_accessed() {
    let device = new_device(12);
    let (engine, mut q, mut writer, mut reader) = setup(&device);
    // Fill several segments with 48 KiB values (packed, ~20 per segment).
    for i in 0..100u128 {
        put(
            &mut q,
            &mut writer,
            id(i),
            &value_for(i, 0, 48 << 10),
            PutOptions::default(),
        );
    }
    seal(&mut q, &mut writer);
    let free_before = writer.free_segments();

    // Touch the even keys among the oldest records.
    for i in (0..20u128).step_by(2) {
        read(&mut q, &mut reader, &id(i)).unwrap();
    }

    let report = reclaim(
        &mut q,
        &mut writer,
        ReclaimPolicy::Cache {
            reinsert_accessed: true,
        },
    )
    .unwrap();
    assert!(report.relocated > 0 && report.dropped > 0);
    assert_eq!(
        writer.free_segments(),
        free_before + 1 - u32::from(report.relocated > 0)
    );
    for i in 0..report.records as u128 {
        let present = engine.contains(&id(i));
        assert_eq!(present, i % 2 == 0, "key {i}");
    }

    // Pure FIFO: the next oldest segment is dropped wholesale.
    let victim = writer
        .pick_victim(ReclaimPolicy::Cache {
            reinsert_accessed: false,
        })
        .unwrap();
    let report = reclaim(
        &mut q,
        &mut writer,
        ReclaimPolicy::Cache {
            reinsert_accessed: false,
        },
    )
    .unwrap();
    assert_eq!(report.seg_no, victim);
    assert_eq!(report.relocated, 0);
    assert!(report.dropped > 0);
    // Only one pass at a time.
    if let Some(t) = writer.reclaim(&mut q, ReclaimPolicy::Storage).unwrap() {
        assert!(matches!(
            writer.reclaim(&mut q, ReclaimPolicy::Storage),
            Err(Error::Busy)
        ));
        blocking::wait(&mut q, &mut writer, t).unwrap();
    }
}

#[test]
fn expiry_hides_and_reclaim_drops() {
    let clock = Arc::new(ManualClock::new(1_000));
    let device = new_device(6);
    let engine = open_with(
        &device,
        Options {
            clock: clock.clone(),
            ..options()
        },
    );
    let mut q = queue();
    let mut writer = engine.writer(&mut q).unwrap();
    let mut reader = engine.reader(&mut q).unwrap();
    writer
        .put(
            &mut q,
            id(1),
            b"ephemeral",
            PutOptions {
                expire_at: 1_100,
                ..Default::default()
            },
        )
        .unwrap();
    writer.put(&mut q, id(2), b"forever", PutOptions::default()).unwrap();
    flush(&mut q, &mut writer);
    assert!(read(&mut q, &mut reader, &id(1)).is_some());
    clock.set(1_100);
    assert!(read(&mut q, &mut reader, &id(1)).is_none());
    assert!(read(&mut q, &mut reader, &id(2)).is_some());

    seal(&mut q, &mut writer);
    let report = reclaim(&mut q, &mut writer, ReclaimPolicy::Storage).unwrap();
    assert_eq!(report.dropped, 1);
    assert_eq!(report.relocated, 1);
    assert!(!engine.contains(&id(1)));
    assert!(read(&mut q, &mut reader, &id(2)).is_some());
}

#[test]
fn no_space_is_reported_not_panicked() {
    let device = new_device(2);
    let (_engine, mut q, mut writer, _reader) = setup(&device);
    let big = vec![1u8; CHUNK_MAX as usize];
    let mut written = 0;
    loop {
        match writer.put(&mut q, id(written), &big, PutOptions::default()) {
            Ok(PutOutcome::Written { .. }) => written += 1,
            Err(Error::NoSpace) => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(written >= 14, "two segments of 1 MiB hold at least 14 x 128 KiB");
    flush(&mut q, &mut writer);
}

#[test]
fn index_budget_is_enforced() {
    let device = new_device(4);
    let engine = open_with(
        &device,
        Options {
            index_capacity: 64,
            // Exactly the initial table: no growth allowed.
            index_memory_budget: 64 * 64,
            ..options()
        },
    );
    let mut q = queue();
    let mut writer = engine.writer(&mut q).unwrap();
    let mut n = 0u128;
    loop {
        match writer.put(&mut q, id(n), b"v", PutOptions::default()) {
            Ok(PutOutcome::Written { .. }) => n += 1,
            Err(Error::IndexFull) => break,
            other => panic!("unexpected {other:?}"),
        }
        flush(&mut q, &mut writer);
    }
    assert_eq!(n, 56, "a 64-slot table takes 7/8 load");
    // Overwrites still work at the budget.
    assert!(matches!(
        writer.put(
            &mut q,
            id(0),
            b"w",
            PutOptions {
                overwrite: true,
                ..Default::default()
            }
        ),
        Ok(PutOutcome::Written { .. })
    ));
    // One delete is not enough: a rebuild at the same size needs the live
    // count at or below 3/4, so the table is compacted only after enough
    // deletes; then new keys fit again.
    assert!(delete(&mut q, &mut writer, &id(1)));
    assert!(matches!(
        writer.put(&mut q, id(1000), b"v", PutOptions::default()),
        Err(Error::IndexFull)
    ));
    for i in 2..12u128 {
        assert!(delete(&mut q, &mut writer, &id(i)));
    }
    assert!(matches!(
        writer.put(&mut q, id(1000), b"v", PutOptions::default()),
        Ok(PutOutcome::Written { .. })
    ));
    flush(&mut q, &mut writer);
    assert_eq!(engine.usage().chunks, 56 - 11 + 1);
}

#[test]
fn open_rejects_unformatted_and_foreign_devices() {
    let blank = Arc::new(MemDevice::new(SEGMENT * 4));
    assert!(matches!(
        moat_engine::open(blank, options()),
        Err(Error::Unformatted(_))
    ));

    // Segments formatted under a different uuid are treated as free, not as data.
    let device = new_device(4);
    {
        let (_engine, mut q, mut writer, _reader) = setup(&device);
        writer.put(&mut q, id(1), b"old", PutOptions::default()).unwrap();
        close(&mut q, writer);
    }
    let mut opts = format_options();
    opts.disk_uuid = [0xcd; 16];
    // Rewrite only the superblocks with the new uuid: every segment header now
    // carries a foreign uuid.
    let mut sb = moat_common::AlignedBuf::zeroed(4096);
    moat_engine::layout::Superblock {
        generation: 5,
        disk_uuid: opts.disk_uuid,
        segment_size: SEGMENT,
        chunk_max: CHUNK_MAX,
        segment_count: 4,
        created_at: 0,
    }
    .encode(&mut sb);
    use moat_engine::Device;
    device.write_at(&sb, 0).unwrap();
    device.write_at(&sb, 4096).unwrap();
    let (engine, report) = moat_engine::open(device.clone(), options()).unwrap();
    assert_eq!(report.unreadable_headers, 4);
    assert_eq!(report.chunks, 0);
    assert!(!engine.contains(&id(1)));
}

/// Random operations against a reference model, with periodic flushes,
/// reclaim passes and reopen cycles. Every observable read must agree with the
/// model.
#[test]
fn randomized_against_model() {
    let device = new_device(20);
    let mut rng = XorShift(0x9e37_79b9);
    let mut model: HashMap<u128, (u32, Vec<u8>)> = HashMap::new();
    let mut versions: HashMap<u128, u32> = HashMap::new();
    let (mut engine, mut q, mut writer, mut reader) = setup(&device);
    let keys = 300u64;

    for step in 0..6_000u32 {
        let k = rng.below(keys) as u128;
        match rng.below(100) {
            0..=54 => {
                let version = versions.entry(k).or_insert(0);
                *version += 1;
                let v = value_for(k, *version, random_len(&mut rng));
                let outcome = put(
                    &mut q,
                    &mut writer,
                    id(k),
                    &v,
                    PutOptions {
                        overwrite: true,
                        ..Default::default()
                    },
                );
                assert!(matches!(outcome, PutOutcome::Written { .. }));
                model.insert(k, (*version, v));
            }
            55..=69 => {
                let existed = matches!(writer.delete(&mut q, &id(k)).unwrap(), DeleteOutcome::Deleted { .. });
                assert_eq!(existed, model.remove(&k).is_some(), "delete {k} at step {step}");
            }
            70..=89 => {
                // Reads see everything flushed; flush first so the model applies.
                flush(&mut q, &mut writer);
                let got = read(&mut q, &mut reader, &id(k));
                assert_eq!(got, model.get(&k).map(|(_, v)| v.clone()), "get {k} at step {step}");
            }
            90..=95 => {
                flush(&mut q, &mut writer);
                if writer.free_segments() < 6 {
                    reclaim(&mut q, &mut writer, ReclaimPolicy::Storage);
                }
            }
            _ => {
                // Reopen: clean shutdown half the time, a "crash" (drop with
                // everything flushed but unsealed) otherwise.
                drop(reader);
                if rng.below(2) == 0 {
                    close(&mut q, writer);
                } else {
                    flush(&mut q, &mut writer);
                    drop(writer);
                }
                drop(engine);
                (engine, q, writer, reader) = setup(&device);
            }
        }
        if writer.free_segments() < 3 {
            flush(&mut q, &mut writer);
            for _ in 0..32 {
                if writer.free_segments() >= 6 || reclaim(&mut q, &mut writer, ReclaimPolicy::Storage).is_none() {
                    break;
                }
            }
        }
    }

    flush(&mut q, &mut writer);
    for k in 0..keys as u128 {
        let got = read(&mut q, &mut reader, &id(k));
        assert_eq!(got, model.get(&k).map(|(_, v)| v.clone()), "final {k}");
    }
    assert_eq!(engine.usage().chunks, model.len());
}

#[test]
fn concurrent_readers_never_see_torn_values() {
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        thread,
    };

    let device = new_device(16);
    let (engine, mut q, mut writer, _reader) = setup(&device);
    let keys = 64u128;
    // Values are 40 KiB so every segment holds ~25 records and reclaim churns.
    let len = 40 << 10;
    for k in 0..keys {
        put(&mut q, &mut writer, id(k), &value_for(k, 0, len), PutOptions::default());
    }
    flush(&mut q, &mut writer);

    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|t| {
            let engine = engine.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                // Each reading thread owns its queue and pool.
                let mut q = queue();
                let mut reader = engine.reader(&mut q).unwrap();
                let mut rng = XorShift(0x100 + t);
                let mut reads = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let k = rng.below(keys as u64) as u128;
                    match blocking::get(&mut q, &mut reader, &id(k), None).unwrap() {
                        Some(v) => {
                            // Any version is acceptable, but it must be a whole one.
                            let ok = (0..64u32).any(|ver| *v == value_for(k, ver, len)[..]);
                            assert!(ok, "torn or foreign value for key {k}");
                            reads += 1;
                        }
                        None => panic!("key {k} vanished"),
                    }
                }
                reader.detach(&mut q);
                reads
            })
        })
        .collect();

    let overwrite = PutOptions {
        overwrite: true,
        ..Default::default()
    };
    for round in 1..40u32 {
        for k in 0..keys {
            put(&mut q, &mut writer, id(k), &value_for(k, round, len), overwrite);
        }
        flush(&mut q, &mut writer);
        for _ in 0..32 {
            if writer.free_segments() >= 6 {
                break;
            }
            reclaim(&mut q, &mut writer, ReclaimPolicy::Storage).unwrap();
        }
    }
    stop.store(true, Ordering::Relaxed);
    let total: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total > 0);
}

/// The same engine on a real file, with `O_DIRECT` and io_uring where the
/// platform supports them (tmpfs does not support `O_DIRECT`; the test then
/// falls back to buffered I/O).
#[cfg(target_os = "linux")]
#[test]
fn file_device_roundtrip_with_direct_io_and_uring() {
    use moat_engine::{FileDevice, uring::UringQueue};

    /// Keep the registered pool below the locked-memory limit of standard
    /// hosted CI runners. The 128 KiB class still accommodates the test
    /// format's 64 KiB maximum chunk plus its metadata.
    fn file_queue_options() -> QueueOptions {
        QueueOptions {
            depth: 64,
            pool: PoolOptions {
                bytes: 1 << 20,
                max_class: 128 << 10,
                huge_pages: HugePages::Disabled,
            },
            descriptors: 4,
        }
    }
    let file_options = Options {
        batch_limit: 128 << 10,
        scan_window: 128 << 10,
        ..options()
    };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("disk.img");
    let len = SEGMENT * 5;
    let device = match FileDevice::create(&path, len, true) {
        Ok(d) => d,
        Err(_) => FileDevice::create(&path, len, false).unwrap(),
    };
    let device: Arc<FileDevice> = Arc::new(device);
    moat_engine::format(
        &*device,
        &FormatOptions {
            chunk_max: FILE_CHUNK_MAX,
            ..format_options()
        },
    )
    .unwrap();

    let mut rng = XorShift(0xfeed);
    let mut expected = HashMap::new();
    {
        let (engine, _) = moat_engine::open(device.clone(), file_options.clone()).unwrap();
        let mut q = UringQueue::new(&file_queue_options()).unwrap();
        let mut writer = engine.writer(&mut q).unwrap();
        let mut reader = engine.reader(&mut q).unwrap();
        for i in 0..64u128 {
            let len = random_len(&mut rng).min(FILE_CHUNK_MAX as usize);
            let v = value_for(i, 0, len);
            put(&mut q, &mut writer, id(i), &v, PutOptions::default());
            expected.insert(i, v);
        }
        flush(&mut q, &mut writer);
        for (i, v) in &expected {
            assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v);
        }
        // Reclaim through the ring as well.
        seal(&mut q, &mut writer);
        reclaim(&mut q, &mut writer, ReclaimPolicy::Storage).unwrap();
        for (i, v) in &expected {
            assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v);
        }
        close(&mut q, writer);
        reader.detach(&mut q);
    }
    let reopened = Arc::new(FileDevice::open(&path, false).unwrap());
    let (engine, report) = moat_engine::open(reopened, file_options).unwrap();
    let mut q = UringQueue::new(&file_queue_options()).unwrap();
    let mut reader = engine.reader(&mut q).unwrap();
    assert_eq!(report.chunks, 64);
    assert_eq!(report.scanned, 0);
    for (i, v) in &expected {
        assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v);
    }
}

/// Values that are (just under) a page multiple are stored framed: header in
/// the batch's header area, value page aligned.
#[test]
fn framed_records_roundtrip_recover_and_reclaim() {
    let device = new_device(16);
    let (_engine, mut q, mut writer, mut reader) = setup(&device);
    let sizes = [4096usize, 4029, 8192, 12288, 16384 - 10, 65536 - 68];
    let mut expected = HashMap::new();
    for i in 0..300u128 {
        let len = sizes[i as usize % sizes.len()];
        let v = value_for(i, 0, len);
        put(&mut q, &mut writer, id(i), &v, PutOptions::default());
        expected.insert(i, v);
    }
    // An expiring page-sized value must not be framed (its expiry lives in the
    // header); it still reads back correctly.
    let clockless = value_for(1000, 0, 4096);
    writer
        .put(
            &mut q,
            id(1000),
            &clockless,
            PutOptions {
                expire_at: u64::MAX,
                ..Default::default()
            },
        )
        .unwrap();
    flush(&mut q, &mut writer);
    for (i, v) in &expected {
        assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v, "key {i}");
    }
    assert_eq!(read(&mut q, &mut reader, &id(1000)).unwrap(), clockless);
    // Range reads inside a framed value.
    let got = blocking::get(&mut q, &mut reader, &id(0), Some(100..300))
        .unwrap()
        .unwrap();
    assert_eq!(&*got, &expected[&0][100..300]);

    // Recovery from a mix of footers (sealed) and scan (active).
    drop(writer);
    drop(reader);
    let (_engine, mut q, mut writer, mut reader) = setup(&device);
    for (i, v) in &expected {
        assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v, "key {i} after reopen");
    }

    // Reclaim relocates framed records as framed records.
    seal(&mut q, &mut writer);
    let mut relocated = 0;
    for _ in 0..4 {
        if let Some(report) = reclaim(&mut q, &mut writer, ReclaimPolicy::Storage) {
            relocated += report.relocated;
        }
    }
    assert!(relocated > 0);
    for (i, v) in &expected {
        assert_eq!(read(&mut q, &mut reader, &id(*i)).unwrap(), *v, "key {i} after reclaim");
    }
    close(&mut q, writer);
    drop(reader);
    let (engine, report) = moat_engine::open(device.clone(), options()).unwrap();
    let mut q = queue();
    let mut reader = engine.reader(&mut q).unwrap();
    assert_eq!(report.chunks, expected.len() + 1);
    for (i, v) in &expected {
        assert_eq!(
            read(&mut q, &mut reader, &id(*i)).unwrap(),
            *v,
            "key {i} after second reopen"
        );
    }
}

#[test]
fn framed_corruption_obeys_read_verification_option() {
    for verify_reads in [false, true] {
        let device = new_device(4);
        let engine = open_with(
            &device,
            Options {
                verify_reads,
                ..options()
            },
        );
        let mut q = queue();
        let mut writer = engine.writer(&mut q).unwrap();
        let mut reader = engine.reader(&mut q).unwrap();
        let v = value_for(9, 0, 4096);
        writer.put(&mut q, id(9), &v, PutOptions::default()).unwrap();
        close(&mut q, writer);
        let pos = device
            .with_data(|d| d.windows(64).position(|w| w == &v[2000..2064]))
            .expect("value on disk");
        // The value starts on a page boundary: framed layout.
        assert_eq!((pos as u64 - 2000) % 4096, 0);
        device.with_data_mut(|d| d[pos + 100] ^= 1);
        let result = blocking::get(&mut q, &mut reader, &id(9), None);
        if verify_reads {
            assert!(matches!(result, Err(Error::Corrupt(_))));
        } else {
            let mut damaged = v.clone();
            damaged[2100] ^= 1;
            assert_eq!(&*result.unwrap().unwrap(), damaged);
        }
    }
}

/// Every small record reads in the minimum number of pages its length
/// allows: verified from the physical placement the index reports.
#[test]
fn small_records_never_straddle_unnecessarily() {
    let device = new_device(24);
    let (_engine, mut q, mut writer, reader) = setup(&device);
    let mut rng = XorShift(0x3333);
    let mut lens = Vec::new();
    for i in 0..500u128 {
        let len = (rng.below(60 << 10) as usize).max(16);
        put(&mut q, &mut writer, id(i), &value_for(i, 0, len), PutOptions::default());
        lens.push(len);
    }
    flush(&mut q, &mut writer);
    for (i, &len) in lens.iter().enumerate() {
        let stat = reader.stat(&id(i as u128)).unwrap();
        let meta = 68u64; // header + one block checksum
        let value_off = stat.value_offset as u64;
        let spanned_value = (value_off % 4096 + len as u64).div_ceil(4096);
        let min_pages = (len as u64).div_ceil(4096);
        let page_multiple = len % 4096 == 0 || len % 4096 > 4096 - meta as usize;
        assert_eq!(stat.framed, page_multiple, "len {len}");
        if stat.framed {
            assert_eq!(value_off % 4096, 0, "framed value must be page aligned");
            assert_eq!(spanned_value, min_pages);
        } else {
            // Header and value are read together: the whole inline record must
            // span the minimum number of pages its total length allows.
            let hdr_off = value_off - meta;
            let spanned = (hdr_off % 4096 + meta + len as u64).div_ceil(4096);
            assert_eq!(spanned, (meta + len as u64).div_ceil(4096), "len {len} at {hdr_off}");
        }
    }
}

/// Room for the footer must account for records in batches that are enqueued
/// but not yet applied; with many batches in flight at a segment boundary an
/// undercount would let the footer spill into the next segment.
#[test]
fn footer_room_accounts_for_unapplied_batches() {
    let device = new_device(12);
    let engine = open(&device);
    let opts = QueueOptions {
        depth: 1024,
        ..queue_options()
    };
    let mut q = SyncQueue::new(&opts, CompletionOrder::Fifo).unwrap();
    let mut writer = engine.writer(&mut q).unwrap();
    // Tiny values pack ~1,900 records into each 256 KiB staging batch, so a
    // batch's footer share (~90 KiB) is what decides whether it fits; nothing
    // is polled until the pool runs dry, so many batches are in flight when
    // segments roll over.
    let n = 30_000u128;
    for i in 0..n {
        put(&mut q, &mut writer, id(i), &value_for(i, 0, 64), PutOptions::default());
    }
    close(&mut q, writer);
    let (engine, report) = moat_engine::open(device.clone(), options()).unwrap();
    assert_eq!(report.unreadable_headers, 0);
    assert_eq!(report.chunks, n as usize);
    let mut q = queue();
    let mut reader = engine.reader(&mut q).unwrap();
    for i in (0..n).step_by(97) {
        assert_eq!(
            read(&mut q, &mut reader, &id(i)).unwrap(),
            value_for(i, 0, 64),
            "key {i}"
        );
    }
}

/// The pool, not the ring depth, is the back-pressure signal: a tiny ring is
/// absorbed by the writer's and reader's ready queues.
#[test]
fn tiny_ring_depth_is_absorbed_by_ready_queues() {
    let device = new_device(8);
    let engine = open(&device);
    let opts = QueueOptions {
        depth: 2,
        ..queue_options()
    };
    let mut q = SyncQueue::new(&opts, CompletionOrder::Reverse).unwrap();
    let mut writer = engine.writer(&mut q).unwrap();
    let mut reader = engine.reader(&mut q).unwrap();
    let mut expected = HashMap::new();
    for i in 0..30u128 {
        let v = value_for(i, 0, 70_000);
        writer.put(&mut q, id(i), &v, PutOptions::default()).unwrap();
        expected.insert(i, v);
    }
    assert!(writer.in_flight() > 2, "most batches wait in the ready queue");
    flush(&mut q, &mut writer);
    let mut tokens = HashMap::new();
    for i in expected.keys() {
        assert_eq!(
            reader.get(&mut q, &id(*i), None, *i as u64).unwrap(),
            moat_engine::ReadOutcome::Submitted
        );
        tokens.insert(*i as u64, *i);
    }
    let mut out = Vec::new();
    while !tokens.is_empty() {
        q.poll(true).unwrap();
        reader.poll(&mut q, &mut out).unwrap();
        for c in out.drain(..) {
            let i = tokens.remove(&c.token).unwrap();
            assert_eq!(&*c.result.unwrap().unwrap(), &expected[&i][..]);
        }
    }
    assert_eq!(reader.in_flight(), 0);
}

/// The load generator uses a per-disk FIFO for each homogeneous write phase.
/// Device completion order must not change publication order for that phase.
#[test]
fn homogeneous_writes_publish_tickets_in_order() {
    for len in [4096, 70_000] {
        let device = new_device(8);
        let engine = open(&device);
        let mut q = SyncQueue::new(
            &QueueOptions {
                depth: 2,
                ..queue_options()
            },
            CompletionOrder::Reverse,
        )
        .unwrap();
        let mut writer = engine.writer(&mut q).unwrap();
        let mut tickets = Vec::new();
        for i in 0..32 {
            let PutOutcome::Written { ticket, .. } = writer
                .put(&mut q, id(i), &vec![0x5a; len], PutOptions::default())
                .unwrap()
            else {
                panic!("new key must be written");
            };
            tickets.push(ticket);
        }
        let barrier = writer.flush(&mut q).unwrap();
        let mut observed = Vec::new();
        let mut done = Vec::new();
        let mut flushed = false;
        while !flushed {
            q.poll(true).unwrap();
            writer.poll(&mut q, &mut done).unwrap();
            for completion in done.drain(..) {
                completion.result.unwrap();
                if completion.ticket == barrier {
                    flushed = true;
                } else {
                    observed.push(completion.ticket);
                }
            }
        }
        assert_eq!(observed, tickets);
    }
}

// Two 64 KiB batches fill each scan window exactly; the next window still
// contains live records. Exercise both recovery and reclaim across that edge.
fn check_scan_window_boundary(mode: &str) {
    for order in [CompletionOrder::Fifo, CompletionOrder::Reverse] {
        let device = Arc::new(MemDevice::new(SEGMENT * 5));
        moat_engine::format(
            &*device,
            &FormatOptions {
                chunk_max: 64 << 10,
                ..format_options()
            },
        )
        .unwrap();
        let opts = Options {
            pack_threshold: 32 << 10,
            batch_limit: 128 << 10,
            scan_window: 128 << 10,
            ..options()
        };
        let expected: Vec<_> = (0..6).map(|i| value_for(i, 0, 60 << 10)).collect();
        {
            let (engine, _) = moat_engine::open(device.clone(), opts.clone()).unwrap();
            let mut q = SyncQueue::new(&queue_options(), order).unwrap();
            let mut writer = engine.writer(&mut q).unwrap();
            for (i, value) in expected.iter().enumerate() {
                put(&mut q, &mut writer, id(i as u128), value, PutOptions::default());
            }
            flush(&mut q, &mut writer);
            if mode == "reclaim" {
                seal(&mut q, &mut writer);
                let report = reclaim(&mut q, &mut writer, ReclaimPolicy::Storage).unwrap();
                assert_eq!(report.relocated, 6, "{mode}");
                close(&mut q, writer);
            } else if mode == "bad_footer" {
                close(&mut q, writer);
            }
        }
        if mode == "bad_footer" {
            device.with_data_mut(|data| {
                let pos = data.windows(8).position(|w| w == b"MOATFOT1").unwrap();
                data[pos + 100] ^= 0xff;
            });
        }
        let (engine, report) = moat_engine::open(device.clone(), opts).unwrap();
        assert_eq!(report.chunks, 6, "{mode}");
        let mut q = queue();
        let mut reader = engine.reader(&mut q).unwrap();
        for (i, value) in expected.iter().enumerate() {
            assert_eq!(
                read(&mut q, &mut reader, &id(i as u128)).as_ref(),
                Some(value),
                "{mode}"
            );
        }
    }
}

#[test]
fn scan_window_boundary_active_recovery() {
    check_scan_window_boundary("active");
}

#[test]
fn scan_window_boundary_footer_recovery() {
    check_scan_window_boundary("bad_footer");
}

#[test]
fn scan_window_boundary_reclaim() {
    check_scan_window_boundary("reclaim");
}
