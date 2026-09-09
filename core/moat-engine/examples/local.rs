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

//! File-backed local development on Linux and macOS.
//!
//! Run with `cargo run -p moat-engine --example local`.
//! Uses a temporary file, removed automatically on exit.

use std::sync::Arc;

use moat_common::{ChunkId, PoolOptions};
use moat_engine::{FileDevice, FormatOptions, Options, PutOptions, QueueBackend, QueueOptions, blocking};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("disk.img");
    let device = Arc::new(FileDevice::create(&path, 16 << 20, false)?);
    moat_engine::format(
        &*device,
        &FormatOptions {
            segment_size: 1 << 20,
            chunk_max: 64 << 10,
            ..Default::default()
        },
    )?;
    let options = Options {
        index_capacity: 1024,
        batch_limit: 128 << 10,
        ..Default::default()
    };
    let (engine, _) = moat_engine::open(device, options.clone())?;
    let queue_options = QueueOptions {
        depth: 16,
        descriptors: 4,
        pool: PoolOptions {
            bytes: 1 << 20,
            max_class: 128 << 10,
            ..Default::default()
        },
    };
    let mut queue = queue_options.build(QueueBackend::Auto)?;
    println!("I/O backend: {:?}", QueueBackend::Auto.resolve());
    let mut writer = engine.writer(queue.as_mut())?;
    let mut reader = engine.reader(queue.as_mut())?;
    let id = ChunkId::from_u128(1);
    let value = vec![0x42; 4096];
    writer.put(queue.as_mut(), id, &value, PutOptions::default())?;
    blocking::flush(queue.as_mut(), &mut writer)?;
    assert_eq!(
        blocking::get(queue.as_mut(), &mut reader, &id, None)?.as_deref(),
        Some(value.as_slice())
    );
    blocking::seal(queue.as_mut(), &mut writer)?;
    writer.detach(queue.as_mut());
    reader.detach(queue.as_mut());
    drop(engine);
    drop(queue);

    let device = Arc::new(FileDevice::open(&path, false)?);
    let (engine, _) = moat_engine::open(device, options)?;
    let mut queue = queue_options.build(QueueBackend::Auto)?;
    let mut reader = engine.reader(queue.as_mut())?;
    assert_eq!(
        blocking::get(queue.as_mut(), &mut reader, &id, None)?.as_deref(),
        Some(value.as_slice())
    );
    reader.detach(queue.as_mut());
    println!("Wrote, read, and recovered a 4 KiB chunk.");
    Ok(())
}
