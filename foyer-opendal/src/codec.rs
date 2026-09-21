// Copyright 2026 foyer Project Authors
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

//! First-version whole-object encoding for immutable `String` / `Vec<u8>` entries.
//!
//! Layout, little-endian, no padding:
//!
//! ```text
//!  0..8   magic `FODL0001`
//!  8..12  key_len (u32)
//! 12..16  value_len (u32)
//! 16..20  CRC-32/ISO-HDLC of magic || key_len || value_len || key || value
//! 20..    key UTF-8 bytes
//! 20+k..  value bytes
//! ```
//!
//! `max_object_size` bounds the entire encoded object (engine default: cache
//! capacity). Length fields are checked against the real buffer before any
//! payload allocation. Decode verifies framing, checksum, and the full key, and
//! never returns bytes from a failed object. This instance agreement is not
//! compatible with the prototype `FOYODL01` envelope and is not a recovery or
//! generic codec format.

use foyer::{Error, ErrorKind, Result};

/// Recognizable first-version object magic. Distinct from experimental `FOYODL01`.
pub(crate) const MAGIC: [u8; 8] = *b"FODL0001";

/// Encoded header length in bytes, excluding key and value payloads.
pub(crate) const HEADER_LEN: usize = 20;

const KEY_LEN_OFFSET: usize = 8;
const VALUE_LEN_OFFSET: usize = 12;
const CHECKSUM_OFFSET: usize = 16;

const _: () = assert!(MAGIC.len() == KEY_LEN_OFFSET);
const _: () = assert!(HEADER_LEN == CHECKSUM_OFFSET + 4);

/// Encode `key` and `value` into one object no larger than `max_object_size`.
pub(crate) fn encode(key: &str, value: &[u8], max_object_size: usize) -> Result<Vec<u8>> {
    let key_len = u32::try_from(key.len()).map_err(|_| {
        Error::new(ErrorKind::OutOfRange, "cache object key exceeds u32 length").with_context("key_len", key.len())
    })?;
    let value_len = u32::try_from(value.len()).map_err(|_| {
        Error::new(ErrorKind::OutOfRange, "cache object value exceeds u32 length")
            .with_context("value_len", value.len())
    })?;
    let total = encoded_len(key.len(), value.len()).ok_or_else(|| {
        Error::new(ErrorKind::OutOfRange, "cache object encoded length overflow")
            .with_context("key_len", key.len())
            .with_context("value_len", value.len())
    })?;
    if total > max_object_size {
        return Err(
            Error::new(ErrorKind::OutOfRange, "encoded cache object exceeds max object size")
                .with_context("encoded", total)
                .with_context("max_object_size", max_object_size),
        );
    }

    let crc = checksum(key.as_bytes(), value, key_len, value_len);
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&key_len.to_le_bytes());
    bytes.extend_from_slice(&value_len.to_le_bytes());
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes.extend_from_slice(key.as_bytes());
    bytes.extend_from_slice(value);
    debug_assert_eq!(bytes.len(), total);
    Ok(bytes)
}

/// Decode `bytes` as the object for `expected_key`.
///
/// The slice must be the complete object and no larger than `max_object_size`.
/// Failures return no value bytes.
pub(crate) fn decode(bytes: &[u8], expected_key: &str, max_object_size: usize) -> Result<Vec<u8>> {
    if bytes.len() > max_object_size {
        return Err(
            Error::new(ErrorKind::OutOfRange, "cache object exceeds max object size")
                .with_context("len", bytes.len())
                .with_context("max_object_size", max_object_size),
        );
    }
    if bytes.len() < HEADER_LEN {
        return Err(Error::new(ErrorKind::Parse, "truncated cache object").with_context("len", bytes.len()));
    }
    if bytes[..KEY_LEN_OFFSET] != MAGIC {
        return Err(Error::new(ErrorKind::MagicMismatch, "cache object magic mismatch"));
    }

    let key_len_u32 = read_u32(bytes, KEY_LEN_OFFSET);
    let value_len_u32 = read_u32(bytes, VALUE_LEN_OFFSET);
    let stored_checksum = read_u32(bytes, CHECKSUM_OFFSET);
    let key_len = usize::try_from(key_len_u32)
        .map_err(|_| Error::new(ErrorKind::Parse, "cache object key length does not fit this platform"))?;
    let value_len = usize::try_from(value_len_u32)
        .map_err(|_| Error::new(ErrorKind::Parse, "cache object value length does not fit this platform"))?;

    let Some(key_end) = HEADER_LEN.checked_add(key_len) else {
        return Err(Error::new(ErrorKind::Parse, "cache object key length overflow"));
    };
    let Some(total) = key_end.checked_add(value_len) else {
        return Err(Error::new(ErrorKind::Parse, "cache object value length overflow"));
    };
    if total != bytes.len() {
        return Err(
            Error::new(ErrorKind::Parse, "cache object framing does not match buffer length")
                .with_context("framed", total)
                .with_context("len", bytes.len()),
        );
    }

    let key = &bytes[HEADER_LEN..key_end];
    let value = &bytes[key_end..total];
    let expected_checksum = checksum(key, value, key_len_u32, value_len_u32);
    if stored_checksum != expected_checksum {
        return Err(Error::new(
            ErrorKind::ChecksumMismatch,
            "cache object checksum mismatch",
        ));
    }
    if key != expected_key.as_bytes() {
        return Err(Error::new(
            ErrorKind::Parse,
            "cache object key does not match the requested key",
        ));
    }
    Ok(value.to_vec())
}

fn encoded_len(key_len: usize, value_len: usize) -> Option<usize> {
    HEADER_LEN.checked_add(key_len)?.checked_add(value_len)
}

fn checksum(key: &[u8], value: &[u8], key_len: u32, value_len: u32) -> u32 {
    let key_len_bytes = key_len.to_le_bytes();
    let value_len_bytes = value_len.to_le_bytes();
    crc32(&[&MAGIC, &key_len_bytes, &value_len_bytes, key, value])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    debug_assert!(bytes.len() >= HEADER_LEN);
    let mut out = [0u8; 4];
    out.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(out)
}

const fn crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut n = 0;
    while n < 256 {
        let mut crc = n as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB88320
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[n] = crc;
        n += 1;
    }
    table
}

const CRC32_TABLE: [u32; 256] = crc32_table();

fn crc32(parts: &[&[u8]]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for part in parts {
        for &byte in *part {
            let idx = ((crc ^ u32::from(byte)) & 0xFF) as usize;
            crc = CRC32_TABLE[idx] ^ (crc >> 8);
        }
    }
    !crc
}
