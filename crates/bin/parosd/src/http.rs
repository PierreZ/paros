//! `parosd`'s HTTP endpoint (#366 (metrics)), on `PAROS_HTTP_LISTEN`: what a
//! monitoring system and an orchestrator ask a machine.
//!
//! | path | answer |
//! |---|---|
//! | `/metrics` | `200`, every metric of the machine in the Prometheus text format (`paros::observe::metrics`) |
//! | `/healthz` | `200`: the process is alive and answers |
//! | `/readyz` | `200` once the machine is a cell member whose stores opened; `503` with its phase before |
//!
//! A plain HTTP/1.1 server, one request per connection, `GET` and `HEAD`
//! only: it holds no state and decides nothing, so it has no place in the
//! simulation. It reads the machine's metrics and never writes them.

use std::net::SocketAddr;
use std::time::Duration;

use futures::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};
use moonpool_core::{
    CompioNetworkProvider, CompioTcpStream, LocalNetworkProvider, LocalTcpListener,
};
use paros::observe::metrics::Metrics;
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
    let Ok(head) = compio::time::timeout(READ_TIMEOUT, read_head(&mut stream)).await else {
        return Ok(());
    };
    stream.write_all(&respond(&head?, metrics)).await?;
    stream.close().await
}

/// Read until the end of a request head, the end of the stream, or past
/// [`MAX_HEAD`] bytes.
async fn read_head(stream: &mut (impl AsyncRead + Unpin)) -> std::io::Result<Vec<u8>> {
    let mut head = Vec::with_capacity(512);
    let mut buffer = [0_u8; 1024];
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
    Ok(head)
}

/// The response bytes to the request head `head`: no body to a `HEAD`.
fn respond(head: &[u8], metrics: &Metrics) -> Vec<u8> {
    let text = String::from_utf8_lossy(head);
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
    response
}

#[cfg(test)]
mod tests {
    use paros::observe::metrics::Metrics;

    use super::{MAX_HEAD, read_head, respond, route};

    fn text(bytes: &[u8]) -> String {
        String::from_utf8(bytes.to_vec()).expect("UTF-8")
    }

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

    #[test]
    fn a_response_carries_its_length_and_no_body_to_a_head() {
        let metrics = Metrics::on();
        let get = text(&respond(
            b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n",
            &metrics,
        ));
        assert_eq!(
            get,
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\n\
             Content-Length: 3\r\nConnection: close\r\n\r\nok\n"
        );
        let head = text(&respond(b"HEAD /healthz HTTP/1.1\r\n\r\n", &metrics));
        assert!(head.ends_with("Content-Length: 3\r\nConnection: close\r\n\r\n"));
        let bad = text(&respond(b"GET\r\n\r\n", &metrics));
        assert!(bad.starts_with("HTTP/1.1 400 Bad Request\r\n"), "{bad}");
        assert!(text(&respond(b"", &metrics)).starts_with("HTTP/1.1 400 "));
        // A head of the bound itself is still read.
        let mut exact = b"GET /healthz HTTP/1.1\r\nX: ".to_vec();
        exact.resize(MAX_HEAD - 4, b'a');
        exact.extend_from_slice(b"\r\n\r\n");
        assert_eq!(exact.len(), MAX_HEAD);
        assert!(text(&respond(&exact, &metrics)).starts_with("HTTP/1.1 200 OK\r\n"));
        let long = vec![b'a'; MAX_HEAD + 1];
        let large = text(&respond(&long, &metrics));
        assert!(
            large.starts_with("HTTP/1.1 431 Request Header Fields Too Large\r\n"),
            "{large}"
        );
        assert!(large.ends_with("Content-Length: 0\r\nConnection: close\r\n\r\n"));
    }

    #[test]
    fn a_head_is_read_to_its_blank_line_the_end_or_the_bound() {
        let read = |bytes: &[u8]| {
            futures::executor::block_on(read_head(&mut futures::io::Cursor::new(bytes.to_vec())))
                .expect("a cursor never fails")
        };
        // Reads stop at the first read that holds the blank line.
        assert_eq!(read(b"GET / HTTP/1.1\r\n\r\n"), b"GET / HTTP/1.1\r\n\r\n");
        assert_eq!(read(b"GET / HTT"), b"GET / HTT", "the stream ended");
        let long = vec![b'a'; 3 * MAX_HEAD];
        let bounded = read(&long);
        assert!(bounded.len() > MAX_HEAD && bounded.len() <= MAX_HEAD + 1024);
    }
}
