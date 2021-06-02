use std::convert::TryInto;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use aes::cipher::StreamCipher;
use anyhow::Result;
use sha2::Digest;
use shared_deps::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use ton_api::{ton, BoxedSerialize, Deserializer, IntoBoxed, Serializer};

use self::node_id::*;
use self::queries_cache::*;

mod node_id;
mod queries_cache;

struct AdnlClient {
    queries_cache: Arc<QueriesCache>,
    sender: mpsc::UnboundedSender<Vec<u8>>,
}

pub struct AdnlClientConfig {
    pub server_address: SocketAddrV4,
    pub server_key: ton::PublicKey,
}

impl AdnlClient {
    pub async fn query<T>(&self, query: &ton::TLObject) -> Result<ton::TLObject> {
        let (query_id, message) = build_query(query)?;
        let message = serialize(&message)?;

        let pending_query = self.queries_cache.add_query(query_id);
        let _ = self.sender.send(message);

        tokio::spawn({
            let queries_cache = self.queries_cache.clone();

            async move {
                let timeout = Duration::from_secs(10);
                tokio::time::sleep(timeout).await;

                match queries_cache.update_query(query_id, None).await {
                    Ok(true) => log::info!("Dropped query"),
                    Err(_) => log::info!("Failed to drop query"),
                    _ => {}
                }
            }
        });

        match pending_query.wait().await? {
            Some(query) => Ok(deserialize(&query)?),
            None => Err(QueryTimeout.into()),
        }
    }

    pub async fn connect(address: SocketAddrV4) -> Result<Arc<Self>> {
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.set_reuseaddr(true);

        let socket = tokio::net::TcpStream::connect(address).await?;
        socket.set_linger(Some(Duration::from_secs(0)));
        let (mut socket_rx, mut socket_tx) = socket.into_split();

        let (tx, mut rx) = mpsc::unbounded_channel();

        let client = Arc::new(AdnlClient {
            queries_cache: Arc::new(Default::default()),
            sender: tx,
        });

        tokio::spawn(async move {
            while let Some(packet) = rx.recv().await {
                socket_tx.write_all(&packet).await;
            }
        });

        tokio::spawn({
            let client = Arc::downgrade(&client);

            async move {
                let mut buffer = bytes::BytesMut::with_capacity(2048);
                loop {
                    let _client = match client.upgrade() {
                        Some(client) => client,
                        None => return,
                    };

                    let len = match socket_rx.read_buf(&mut buffer).await {
                        Ok(0) => continue,
                        Ok(len) => len,
                        Err(e) => {
                            log::error!("Failed to receive data: {}", e);
                            continue;
                        }
                    };
                }
            }
        });

        Ok(client)
    }
}

pub fn build_query(query: &ton::TLObject) -> Result<(QueryId, ton::adnl::Message)> {
    use rand::Rng;

    let query_id: QueryId = rand::thread_rng().gen();
    let query = serialize(query)?;

    Ok((
        query_id,
        ton::adnl::message::message::Query {
            query_id: ton::int256(query_id),
            query: ton::bytes(query),
        }
        .into_boxed(),
    ))
}

pub fn build_handshake_packet(
    peer_id: &AdnlNodeIdShort,
    peer_id_full: &AdnlNodeIdFull,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    // Create temp local key
    let temp_private_key = ed25519_dalek::SecretKey::generate(&mut rand::thread_rng());
    let temp_public_key = ed25519_dalek::PublicKey::from(&temp_private_key);

    // Prepare packet
    let hash = sha2::Sha256::digest(buffer.as_slice());
    let checksum: &[u8; 32] = hash.as_slice().try_into().unwrap();

    let length = buffer.len();
    buffer.resize(length + 96, 0);
    buffer.copy_within(..length, 96);

    buffer[..32].copy_from_slice(peer_id.as_slice());
    buffer[32..64].copy_from_slice(temp_public_key.as_bytes());
    buffer[64..96].copy_from_slice(checksum);

    // Encrypt packet data
    let temp_private_key_part = ed25519_dalek::ExpandedSecretKey::from(&temp_private_key)
        .to_bytes()[0..32]
        .try_into()
        .unwrap();

    let shared_secret = compute_shared_secret(&temp_private_key_part, peer_id_full.public_key())?;
    build_packet_cipher(&shared_secret, checksum).apply_keystream(&mut buffer[96..]);

    // Done
    Ok(())
}

pub fn build_packet_cipher(shared_secret: &[u8; 32], checksum: &[u8; 32]) -> aes::Aes256Ctr {
    use aes::cipher::NewCipher;

    let mut aes_key_bytes: [u8; 32] = *shared_secret;
    aes_key_bytes[16..32].copy_from_slice(&checksum[16..32]);
    let mut aes_ctr_bytes: [u8; 16] = checksum[0..16].try_into().unwrap();
    aes_ctr_bytes[4..16].copy_from_slice(&shared_secret[20..32]);

    aes::Aes256Ctr::new(
        generic_array::GenericArray::from_slice(&aes_key_bytes),
        generic_array::GenericArray::from_slice(&aes_ctr_bytes),
    )
}

pub fn compute_shared_secret(
    private_key_part: &[u8; 32],
    public_key: &[u8; 32],
) -> Result<[u8; 32]> {
    let point = curve25519_dalek::edwards::CompressedEdwardsY(*public_key)
        .decompress()
        .ok_or(BadPublicKeyData)?
        .to_montgomery()
        .to_bytes();
    Ok(x25519_dalek::x25519(*private_key_part, point))
}

#[derive(thiserror::Error, Debug)]
#[error("Bad public key data")]
struct BadPublicKeyData;

pub fn serialize<T: BoxedSerialize>(object: &T) -> Result<Vec<u8>> {
    let mut ret = Vec::new();
    Serializer::new(&mut ret).write_boxed(object).convert()?;
    Ok(ret)
}

pub fn deserialize(bytes: &[u8]) -> Result<ton::TLObject> {
    let mut reader = bytes;
    Deserializer::new(&mut reader)
        .read_boxed::<ton::TLObject>()
        .convert()
}

#[derive(thiserror::Error, Debug)]
#[error("Query timeout")]
struct QueryTimeout;
