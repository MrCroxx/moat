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

use moat_cache_memory::Priority;

use crate::{Bytes, Error, Result};

pub(crate) const HEADER_LEN: usize = 56;
const MAGIC: &[u8; 8] = b"MOATCAC2";
const FORMAT: u16 = 2;

pub(crate) struct Envelope {
    pub key: Bytes,
    pub properties: Bytes,
    pub value: Bytes,
    pub generation: u64,
    pub priority: Priority,
}

pub(crate) struct Format {
    pub namespace: [u8; 16],
    pub identity_version: u32,
}
pub(crate) struct Record<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub properties: &'a [u8],
    pub generation: u64,
    pub priority: Priority,
}
impl Format {
    pub fn set_generation(bytes: &mut [u8], generation: u64) {
        bytes[32..40].copy_from_slice(&generation.to_le_bytes());
    }

    pub fn encode(&self, record: Record<'_>, max: usize) -> Result<Vec<u8>> {
        let Record {
            key,
            value,
            properties,
            generation,
            priority,
        } = record;
        let len = HEADER_LEN
            .checked_add(key.len())
            .and_then(|n| n.checked_add(properties.len()))
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(Error::TooLarge { len: usize::MAX, max })?;
        if len > max {
            return Err(Error::TooLarge { len, max });
        }
        let key_len = u32::try_from(key.len()).map_err(|_| Error::TooLarge { len, max })?;
        let properties_len = u32::try_from(properties.len()).map_err(|_| Error::TooLarge { len, max })?;
        let value_len = value.len() as u64;
        let mut output = Vec::with_capacity(len);
        output.resize(HEADER_LEN, 0);
        output.extend_from_slice(key);
        output.extend_from_slice(properties);
        output.extend_from_slice(value);
        output[..8].copy_from_slice(MAGIC);
        output[8..10].copy_from_slice(&FORMAT.to_le_bytes());
        output[10..12].copy_from_slice(&u16::from(priority == Priority::High).to_le_bytes());
        output[12..16].copy_from_slice(&self.identity_version.to_le_bytes());
        output[16..32].copy_from_slice(&self.namespace);
        output[32..40].copy_from_slice(&generation.to_le_bytes());
        output[40..44].copy_from_slice(&key_len.to_le_bytes());
        output[44..48].copy_from_slice(&properties_len.to_le_bytes());
        output[48..56].copy_from_slice(&value_len.to_le_bytes());
        Ok(output)
    }

    pub fn decode(&self, bytes: Bytes) -> Result<Envelope> {
        if bytes.len() < HEADER_LEN || &bytes[..8] != MAGIC {
            return Err(Error::Corrupt("invalid envelope header"));
        }
        if u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != FORMAT {
            return Err(Error::Corrupt("unsupported envelope version"));
        }
        if u32::from_le_bytes(bytes[12..16].try_into().unwrap()) != self.identity_version
            || bytes[16..32] != self.namespace
        {
            return Err(Error::Corrupt("cache namespace or identity version mismatch"));
        }
        let flags = u16::from_le_bytes(bytes[10..12].try_into().unwrap());
        let priority = match flags {
            0 => Priority::Normal,
            1 => Priority::High,
            _ => return Err(Error::Corrupt("unknown envelope flags")),
        };
        let generation = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let key_len = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
        let properties_len = u32::from_le_bytes(bytes[44..48].try_into().unwrap()) as usize;
        let value_len = usize::try_from(u64::from_le_bytes(bytes[48..56].try_into().unwrap()))
            .map_err(|_| Error::Corrupt("value length overflow"))?;
        let key_end = HEADER_LEN
            .checked_add(key_len)
            .ok_or(Error::Corrupt("key length overflow"))?;
        let properties_end = key_end
            .checked_add(properties_len)
            .ok_or(Error::Corrupt("properties length overflow"))?;
        let value_end = properties_end
            .checked_add(value_len)
            .ok_or(Error::Corrupt("entry length overflow"))?;
        if value_end != bytes.len() {
            return Err(Error::Corrupt("envelope length mismatch"));
        }
        Ok(Envelope {
            key: bytes.slice(HEADER_LEN..key_end)?,
            properties: bytes.slice(key_end..properties_end)?,
            value: bytes.slice(properties_end..value_end)?,
            generation,
            priority,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn format() -> Format {
        Format {
            namespace: [3; 16],
            identity_version: 2,
        }
    }
    fn encoded() -> Vec<u8> {
        format()
            .encode(
                Record {
                    key: b"key",
                    value: &[9; 8],
                    properties: &7u64.to_le_bytes(),
                    generation: 1,
                    priority: Priority::High,
                },
                128,
            )
            .unwrap()
    }
    #[test]
    fn byte_encoder_preserves_v2_layout_and_exact_size_boundary() {
        let expected = [
            b"MOATCAC2".as_slice(),
            &[2, 0, 1, 0],
            &2u32.to_le_bytes(),
            &[3; 16],
            &1u64.to_le_bytes(),
            &3u32.to_le_bytes(),
            &8u32.to_le_bytes(),
            &8u64.to_le_bytes(),
            b"key",
            &7u64.to_le_bytes(),
            &[9; 8],
        ]
        .concat();
        assert_eq!(encoded(), expected);
        for max in [expected.len() - 1, expected.len()] {
            let result = format().encode(
                Record {
                    key: b"key",
                    value: &[9; 8],
                    properties: &7u64.to_le_bytes(),
                    generation: 1,
                    priority: Priority::High,
                },
                max,
            );
            if max == expected.len() {
                assert_eq!(result.unwrap(), expected);
            } else {
                assert!(
                    matches!(result, Err(Error::TooLarge { len, max: bound }) if len == expected.len() && bound == max)
                );
            }
        }
    }

    #[test]
    fn envelope_preserves_shared_field_views_and_rejects_truncation_or_trailing_bytes() {
        let valid = encoded();
        let bytes = Bytes::from(valid.clone());
        let decoded = format().decode(bytes.clone()).unwrap();
        assert!(decoded.key.shares_backing(&bytes));
        assert!(decoded.value.shares_backing(&decoded.properties));
        assert_eq!(decoded.key.as_ref(), b"key");
        assert_eq!(decoded.value.as_ref(), &[9; 8]);
        assert_eq!(u64::from_le_bytes(decoded.properties.as_ref().try_into().unwrap()), 7);
        assert_eq!(decoded.priority, Priority::High);
        for end in 0..valid.len() {
            assert!(format().decode(Bytes::from(valid[..end].to_vec())).is_err());
        }
        let mut trailing = valid;
        trailing.push(0);
        assert!(format().decode(Bytes::from(trailing)).is_err());
    }
    #[test]
    fn lengths_flags_versions_and_namespace_are_validated_without_payload_checksums() {
        for (offset, field) in [
            (40, u32::MAX.to_le_bytes().to_vec()),
            (44, u32::MAX.to_le_bytes().to_vec()),
            (48, u64::MAX.to_le_bytes().to_vec()),
            (10, 2u16.to_le_bytes().to_vec()),
            (8, 1u16.to_le_bytes().to_vec()),
            (0, b"MOATCAC1".to_vec()),
            (12, 3u32.to_le_bytes().to_vec()),
            (16, vec![0; 16]),
        ] {
            let mut bytes = encoded();
            bytes[offset..offset + field.len()].copy_from_slice(&field);
            Format::set_generation(&mut bytes, 42);
            assert!(
                format().decode(Bytes::from(bytes)).is_err(),
                "accepted invalid field at {offset}"
            );
        }
        let mut bytes = encoded();
        Format::set_generation(&mut bytes, 42);
        assert_eq!(format().decode(Bytes::from(bytes)).unwrap().generation, 42);
    }

    #[test]
    fn payload_bytes_are_decoded_without_an_additional_integrity_scan() {
        let mut bytes = encoded();
        assert_eq!(bytes.len(), HEADER_LEN + 3 + 8 + 8);
        *bytes.last_mut().unwrap() ^= 1;
        let decoded = format().decode(Bytes::from(bytes)).unwrap();
        assert_eq!(decoded.value.as_ref(), &[9, 9, 9, 9, 9, 9, 9, 8]);
    }
}
