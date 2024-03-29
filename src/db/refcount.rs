pub fn decode_value_with_rc(bytes: &[u8]) -> (RcType, Option<&[u8]>) {
    let without_payload = match bytes.len().cmp(&RC_BYTES) {
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => true,
        std::cmp::Ordering::Less => return (0, None),
    };

    let rc = RcType::from_le_bytes(bytes[..RC_BYTES].try_into().unwrap());
    if rc <= 0 || without_payload {
        (rc, None)
    } else {
        (rc, Some(&bytes[RC_BYTES..]))
    }
}

pub fn strip_refcount(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < RC_BYTES {
        return None;
    }
    if RcType::from_le_bytes(bytes[..RC_BYTES].try_into().unwrap()) > 0 {
        Some(&bytes[RC_BYTES..])
    } else {
        None
    }
}

pub fn add_positive_refount(rc: RcType, data: Option<&[u8]>, target: &mut Vec<u8>) {
    target.extend_from_slice(&rc.to_le_bytes());
    if let Some(data) = data {
        target.extend_from_slice(data);
    }
}

pub type RcType = i64;

pub const RC_BYTES: usize = std::mem::size_of::<RcType>();
