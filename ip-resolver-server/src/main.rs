use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use anyhow::Result;
use argh::FromArgs;
use tokio::net::UdpSocket;

use ip_resolver::proto;

#[derive(Debug, FromArgs)]
#[argh(description = "UDP hole punching server")]
pub struct App {
    /// listen port
    #[argh(option, short = 'p')]
    port: u16,
    /// number of threads
    #[argh(option, short = 'n', default = "4")]
    n: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let app: App = argh::from_env();

    let reinit_data = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as u32;

    let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, app.port))
        .await
        .map(Arc::new)?;

    let mut handles = Vec::new();
    for _ in 0..app.n {
        let socket = socket.clone();
        handles.push(async move {
            let mut buffer = [0; 128];
            loop {
                match socket.recv_from(&mut buffer).await {
                    Ok((len, addr)) if len == proto::ResolutionRequest::SIZE => {
                        let addr = match addr {
                            SocketAddr::V4(addr) => addr,
                            SocketAddr::V6(_) => continue,
                        };

                        let req = match proto::ResolutionRequest::read_from(&buffer) {
                            Some(req) if req.reinit_date >= reinit_data => req,
                            _ => continue,
                        };

                        let res = proto::ResolutionResponse {
                            session_id: req.session_id,
                            reinit_date: req.reinit_date,
                            ip: (*addr.ip()).into(),
                            port: addr.port(),
                        };

                        socket.send_to(&res.as_bytes(), addr).await.ok();
                    }
                    _ => continue,
                }
            }
        });
    }

    for handle in handles {
        handle.await;
    }
    Ok(())
}
