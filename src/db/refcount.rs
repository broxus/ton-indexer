use std::cmp::Ordering;
use std::convert::TryInto;

use rocksdb::compaction_filter::Decision;

pub fn merge_operator(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Vec<u8> {
    let (mut rc, mut payload) = existing.map_or((0, None), decode_value_with_rc);
    for (delta, new_payload) in operands.into_iter().map(decode_value_with_rc) {
        if payload.is_none() {
            payload = new_payload;
        } else if new_payload.is_some() {
            debug_assert_eq!(payload, new_payload);
        }
        rc += delta;
    }

    match rc.cmp(&0) {
        Ordering::Less => rc.to_le_bytes().to_vec(),
        Ordering::Equal => Vec::new(),
        Ordering::Greater => {
            let payload = payload.unwrap_or(&[]);
            let mut result = Vec::with_capacity(RC_BYTES + payload.len());
            result.extend_from_slice(&rc.to_le_bytes());
            result.extend_from_slice(payload);
            result
        }
    }
}

pub fn compaction_filter(_level: u32, _key: &[u8], value: &[u8]) -> Decision {
    if value.is_empty() {
        Decision::Remove
    } else {
        Decision::Keep
    }
}

pub fn decode_value_with_rc(bytes: &[u8]) -> (i64, Option<&[u8]>) {
    if bytes.len() < RC_BYTES {
        return (0, None);
    }
    let rc = RcType::from_le_bytes(bytes[..RC_BYTES].try_into().unwrap());
    if rc <= 0 {
        (rc, None)
    } else {
        (rc, Some(&bytes[RC_BYTES..]))
    }
}

pub fn strip_refcount(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < RC_BYTES {
        return None;
    }
    if i64::from_le_bytes(bytes[..RC_BYTES].try_into().unwrap()) > 0 {
        Some(&bytes[RC_BYTES..])
    } else {
        None
    }
}

pub fn add_positive_refcount(data: &[u8], rc: std::num::NonZeroU32) -> Vec<u8> {
    let mut result = Vec::with_capacity(RC_BYTES + data.len());
    result.extend_from_slice(&RcType::from(rc.get()).to_le_bytes());
    result.extend_from_slice(data);
    result
}

pub fn encode_negative_refcount(rc: std::num::NonZeroU32) -> Vec<u8> {
    (-RcType::from(rc.get())).to_le_bytes().to_vec()
}

type RcType = i64;

const RC_BYTES: usize = std::mem::size_of::<RcType>();
