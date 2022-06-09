use rand::Rng;

const PKT_REQ: u32 = 0x01;
const PKT_RES: u32 = 0x02;

/// 0..4 - header ([0x1, 0, 0, 0])
/// 4..8 - session_id
/// 8..12 - reinit date
/// 12..16 - crc32
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ResolutionRequest {
    pub session_id: u32,
    pub reinit_date: u32,
}

impl ResolutionRequest {
    pub const SIZE: usize = 4 + 4 + 4 + 4;

    pub fn new() -> Self {
        Self {
            session_id: rand::thread_rng().gen(),
            reinit_date: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32,
        }
    }

    pub fn as_bytes(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0; Self::SIZE];

        buffer[..4].copy_from_slice(&PKT_REQ.to_le_bytes());
        buffer[4..8].copy_from_slice(&self.session_id.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.reinit_date.to_le_bytes());

        let checksum = crc32fast::hash(&buffer[..12]);
        buffer[12..16].copy_from_slice(&checksum.to_le_bytes());

        buffer
    }

    pub fn read_from(buffer: &[u8]) -> Option<Self> {
        if buffer.len() < Self::SIZE {
            return None;
        }

        let ty = u32::from_le_bytes(buffer[..4].try_into().unwrap());
        if ty != PKT_REQ {
            return None;
        }

        let session_id = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        let reinit_date = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
        let checksum = u32::from_le_bytes(buffer[12..16].try_into().unwrap());

        if checksum == crc32fast::hash(&buffer[..12]) {
            Some(Self {
                session_id,
                reinit_date,
            })
        } else {
            None
        }
    }
}

impl Default for ResolutionRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// 0..4 - header ([0x2, 0, 0, 0])
/// 4..8 - session_id
/// 8..12 - reinit date
/// 12..16 - ipv4
/// 16..18 - port
/// 18..22 - crc32
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ResolutionResponse {
    pub session_id: u32,
    pub reinit_date: u32,
    pub ip: u32,
    pub port: u16,
}

impl ResolutionResponse {
    pub const SIZE: usize = 4 + 4 + 4 + 4 + 2 + 4;

    pub fn as_bytes(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0; Self::SIZE];

        buffer[..4].copy_from_slice(&PKT_RES.to_le_bytes());
        buffer[4..8].copy_from_slice(&self.session_id.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.reinit_date.to_le_bytes());
        buffer[12..16].copy_from_slice(&self.ip.to_le_bytes());
        buffer[16..18].copy_from_slice(&self.port.to_le_bytes());

        let checksum = crc32fast::hash(&buffer[..18]);
        buffer[18..22].copy_from_slice(&checksum.to_le_bytes());

        buffer
    }

    pub fn read_from(buffer: &[u8]) -> Option<Self> {
        if buffer.len() < Self::SIZE {
            return None;
        }

        let ty = u32::from_le_bytes(buffer[..4].try_into().unwrap());
        if ty != PKT_RES {
            return None;
        }

        let session_id = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        let reinit_date = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
        let ip = u32::from_le_bytes(buffer[12..16].try_into().unwrap());
        let port = u16::from_le_bytes(buffer[16..18].try_into().unwrap());
        let checksum = u32::from_le_bytes(buffer[18..22].try_into().unwrap());

        if checksum == crc32fast::hash(&buffer[..18]) {
            Some(Self {
                session_id,
                reinit_date,
                ip,
                port,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn correct_packet_representation() {
        let req = ResolutionRequest {
            session_id: rand::thread_rng().gen(),
            reinit_date: rand::thread_rng().gen(),
        };

        let buffer = req.as_bytes();
        assert_eq!(req, ResolutionRequest::read_from(&buffer).unwrap());

        let res = ResolutionResponse {
            session_id: rand::thread_rng().gen(),
            reinit_date: rand::thread_rng().gen(),
            ip: rand::thread_rng().gen(),
            port: rand::thread_rng().gen(),
        };

        let buffer = res.as_bytes();
        assert_eq!(res, ResolutionResponse::read_from(&buffer).unwrap());
    }
}
