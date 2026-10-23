//! `parosd`'s HTTP endpoint (#366 (metrics)), on `PAROS_HTTP_LISTEN`: what a
//! monitoring system and an orchestrator ask a machine.
//!
//! | path | answer |
//! |---|---|
//! | `/metrics` | `200`, every metric of the machine in the Prometheus text format (`paros::metrics`) |
//! | `/healthz` | `200`: the process is alive and answers |
//! | `/readyz` | `200` once the machine is a cell member whose stores opened; `503` with its phase before |
//!
//! A plain HTTP/1.1 server, one request per connection, `GET` and `HEAD`
//! only: it holds no state and decides nothing, so it has no place in the
//! simulation. It reads the machine's metrics and never writes them.

use std::net::SocketAddr;
use std::time::Duration;

use futures::io::{AsyncReadExt as _, AsyncWriteExt as _};
use moonpool_core::{
    CompioNetworkProvider, CompioTcpStream, LocalNetworkProvider, LocalTcpListener,
};
use paros::metrics::Metrics;
use tokio_util::sync::CancellationToken;

/// The longest request head read; a longer one is refused.
const MAX_HEAD: usize = 8 << 10;

/// How long a connection may take to send its request head.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// One answer: the status line's code and reason, the content type and the
/// body.
#[derive(Debug, PartialEq, Eq)]
pub struct Answer {
    pub code: u16,
    pub reason: &'static str,
    pub content_type: &'static str,
    pub body: String,
}

impl Answer {
    fn text(code: u16, reason: &'static str, body: impl Into<String>) -> Self {
        Self {
            code,
            reason,
            content_type: "text/plain; charset=utf-8",
            body: body.into(),
        }
    }
}

/// The answer to `method path` from `metrics`.
#[must_use]
pub fn route(method: &str, path: &str, metrics: &Metrics) -> Answer {
    if method != "GET" && method != "HEAD" {
        return Answer::text(405, "Method Not Allowed", "GET or HEAD\n");
    }
    // A query string changes nothing here.
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    match path {
        "/metrics" => Answer {
            content_type: "text/plain; version=0.0.4; charset=utf-8",
            ..Answer::text(200, "OK", metrics.render())
        },
        "/healthz" => Answer::text(200, "OK", "ok\n"),
        "/readyz" if metrics.ready() => Answer::text(200, "OK", "ready\n"),
        "/readyz" => Answer::text(
            503,
            "Service Unavailable",
            format!("{}\n", metrics.phase().as_str()),
        ),
        _ => Answer::text(404, "Not Found", "/metrics, /healthz or /readyz\n"),
    }
}

/// Bind `addr` and serve `metrics` until `shutdown`.
///
/// # Errors
///
/// The address could not be bound.
pub async fn serve(
    addr: SocketAddr,
    metrics: Metrics,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let listener = CompioNetworkProvider::new().bind(&addr.to_string()).await?;
    tracing::info!(%addr, "parosd_http_listening");
    compio::runtime::spawn(async move {
        loop {
            moonpool_core::select! {
                () = shutdown.cancelled() => return,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        let metrics = metrics.clone();
                        compio::runtime::spawn(async move {
                            if let Err(error) = answer(stream, &metrics).await {
                                tracing::debug!(%error, "parosd_http_connection_failed");
                            }
                        })
                        .detach();
                    }
                    Err(error) => tracing::warn!(%error, "parosd_http_accept_failed"),
                },
            }
        }
    })
    .detach();
    Ok(())
}

/// Read one request head from `stream` and write its answer.
async fn answer(mut stream: CompioTcpStream, metrics: &Metrics) -> std::io::Result<()> {
    let mut head = Vec::with_capacity(512);
    let mut buffer = [0_u8; 1024];
    let read = compio::time::timeout(READ_TIMEOUT, async {
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            head.extend_from_slice(&buffer[..n]);
            if head.len() > MAX_HEAD {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    })
    .await;
    if read.is_err() {
        return Ok(());
    }
    read.unwrap_or(Ok(()))?;
    let text = String::from_utf8_lossy(&head);
    let mut words = text.lines().next().unwrap_or_default().split_whitespace();
    let (method, path) = (words.next().unwrap_or(""), words.next().unwrap_or(""));
    let answer = if head.len() > MAX_HEAD {
        Answer::text(431, "Request Header Fields Too Large", "")
    } else if path.is_empty() {
        Answer::text(400, "Bad Request", "")
    } else {
        route(method, path, metrics)
    };
    let mut response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        answer.code,
        answer.reason,
        answer.content_type,
        answer.body.len()
    )
    .into_bytes();
    if method != "HEAD" {
        response.extend_from_slice(answer.body.as_bytes());
    }
    stream.write_all(&response).await?;
    stream.close().await
}

#[cfg(test)]
mod tests {
    use paros::metrics::Metrics;

    use super::route;

    #[test]
    fn probes_and_metrics_answer_by_path() {
        let metrics = Metrics::on();
        assert_eq!(route("GET", "/healthz", &metrics).code, 200);
        let not_ready = route("GET", "/readyz", &metrics);
        assert_eq!(
            (not_ready.code, not_ready.body.as_str()),
            (503, "starting\n")
        );
        let text = route("GET", "/metrics?x=1", &metrics);
        assert_eq!(text.code, 200);
        assert!(text.body.contains("paros_machine_phase 0"));
        assert_eq!(route("POST", "/metrics", &metrics).code, 405);
        assert_eq!(route("GET", "/", &metrics).code, 404);
    }
}
