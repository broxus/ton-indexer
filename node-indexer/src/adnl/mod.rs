use std::convert::TryInto;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use aes::cipher::NewCipher;
use aes::cipher::StreamCipher;
use anyhow::Result;
use rand::Rng;
use sha2::Digest;
use shared_deps::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use ton_api::{ton, BoxedSerialize, Deserializer, IntoBoxed, Serializer};

use self::node_id::*;
use self::queries_cache::*;

mod node_id;
mod queries_cache;

pub struct AdnlClient {
    queries_cache: Arc<QueriesCache>,
    sender: mpsc::UnboundedSender<PacketToSend>,
}

#[derive(Debug, Clone)]
pub struct AdnlClientConfig {
    pub server_address: SocketAddrV4,
    pub server_key: ed25519_dalek::PublicKey,
}

impl AdnlClient {
    pub async fn query(&self, query: &ton::TLObject) -> Result<ton::TLObject> {
        let (query_id, message) = build_query(query)?;
        let message = serialize(&message)?;

        let pending_query = self.queries_cache.add_query(query_id);
        let _ = self.sender.send(PacketToSend {
            data: message,
            should_encrypt: true,
        });

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

    pub async fn connect(config: AdnlClientConfig) -> Result<Arc<Self>> {
        let (peer_id_full, peer_id) = config.server_key.compute_node_ids()?;

        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.set_reuseaddr(true);

        let socket = tokio::net::TcpStream::connect(config.server_address).await?;
        socket.set_linger(Some(Duration::from_secs(0)));
        let (mut socket_rx, mut socket_tx) = socket.into_split();

        let (tx, mut rx) = mpsc::unbounded_channel();

        let mut rng = rand::thread_rng();
        let mut initial_buffer: Vec<u8> = (0..160).map(|_| rng.gen()).collect();

        let mut cipher_receive = aes::Aes256Ctr::new(
            generic_array::GenericArray::from_slice(&initial_buffer[0..32]),
            generic_array::GenericArray::from_slice(&initial_buffer[64..80]),
        );
        let mut cipher_send = aes::Aes256Ctr::new(
            generic_array::GenericArray::from_slice(&initial_buffer[32..64]),
            generic_array::GenericArray::from_slice(&initial_buffer[80..96]),
        );

        let client = Arc::new(AdnlClient {
            queries_cache: Arc::new(Default::default()),
            sender: tx,
        });

        tokio::spawn(async move {
            while let Some(mut packet) = rx.recv().await {
                if packet.should_encrypt {
                    let packet = &mut packet.data;

                    let len = packet.len();

                    packet.reserve(len + 68);
                    packet.resize(len + 36, 0);
                    packet[..].copy_within(..len, 36);
                    packet[..4].copy_from_slice(&((len + 64) as u32).to_le_bytes());

                    let nonce: [u8; 32] = rand::thread_rng().gen();
                    packet[4..36].copy_from_slice(&nonce);

                    packet.extend_from_slice(sha2::Sha256::digest(&packet[4..]).as_slice());
                    cipher_send.apply_keystream(packet);
                }

                if let Err(e) = socket_tx.write_all(&packet.data).await {
                    log::error!("Failed to send packet: {}", e);
                }
            }
        });

        tokio::spawn({
            let client = Arc::downgrade(&client);

            async move {
                loop {
                    let client = match client.upgrade() {
                        Some(client) => client,
                        None => return,
                    };

                    let mut length = [0; 4];
                    if let Err(e) = socket_rx.read_exact(&mut length).await {
                        log::error!("Failed to read packet length: {}", e);
                        continue;
                    }
                    cipher_receive.apply_keystream(&mut length);

                    let length = u32::from_le_bytes(length) as usize;
                    if length < 64 {
                        log::error!("Too small size for ADNL packet: {}", length);
                        continue;
                    }

                    let mut buffer = vec![0; length];
                    if let Err(e) = socket_rx.read_exact(&mut buffer).await {
                        log::error!("Failed to read buffer of length {}: {}", length, e);
                        continue;
                    }
                    cipher_receive.apply_keystream(&mut buffer);

                    if !sha2::Sha256::digest(&buffer[..length - 32])
                        .as_slice()
                        .eq(&buffer[length - 32..length])
                    {
                        log::error!("Invalid ADNL packet checksum");
                        continue;
                    }

                    buffer.truncate(length - 32);
                    buffer.drain(..32);

                    let data = match deserialize(&buffer) {
                        Ok(data) => data,
                        Err(e) => {
                            log::error!("Got invalid ADNL packet: {}", e);
                            continue;
                        }
                    };

                    match data.downcast::<ton::adnl::Message>() {
                        Ok(ton::adnl::Message::Adnl_Message_Answer(message)) => {
                            match client
                                .queries_cache
                                .update_query(message.query_id.0, Some(message.answer.0))
                                .await
                            {
                                Ok(true) => {}
                                _ => log::error!("Failed to resolve query"),
                            }
                        }
                        _ => log::error!("Got unknown ADNL message"),
                    }
                }
            }
        });

        log::info!("Created connection. Sending init packet...");

        build_handshake_packet(&peer_id, &peer_id_full, &mut initial_buffer);
        client.sender.send(PacketToSend {
            data: initial_buffer,
            should_encrypt: false,
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

struct PacketToSend {
    data: Vec<u8>,
    should_encrypt: bool,
}
