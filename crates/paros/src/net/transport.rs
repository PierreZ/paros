//! How a paros runtime protects its sessions: [`Transport`], the one choice
//! every listening driver ([`run_journals`](crate::run_journals),
//! [`run_machine`](crate::machine::run_machine),
//! [`run_frontend`](crate::frontend::run_frontend), the matchmaker, proxy
//! and replica roles) and every client runtime makes once, at its start.
//!
//! Not the peer lanes (`driver/transport.rs`): those choose *what* a node
//! sends to *whom*; this chooses how the bytes travel. A runtime that
//! listens also dials its peers with the same choice, so every process of a
//! cell must make the same one: a plaintext dialer cannot reach a TLS
//! listener, and the reverse.
//!
//! [`Transport::Plaintext`] is the default and the only choice without the
//! `tls` feature, which keeps the library wasm-safe. With it,
//! [`Transport::Tls`] carries moonpool's TLS upgrade, built by
//! [`crate::net::tls`].

use std::future::Future;
use std::io;
use std::pin::Pin;

use crate::ParosProviders;
use moonpool_rpc::{RpcConfig, RpcDriver, RpcHandle};

/// A runtime's driving future: it resolves only when the runtime fails for
/// good, with the error.
pub type RuntimeFuture = Pin<Box<dyn Future<Output = io::Error>>>;

/// How a runtime's sessions travel.
#[derive(Clone, Debug, Default)]
pub enum Transport {
    /// Plain TCP: the deployment is a trusted network.
    #[default]
    Plaintext,
    /// TLS 1.3 on every session, mutual when the server side asks for
    /// client certificates ([`crate::net::tls`]).
    #[cfg(feature = "tls")]
    Tls(moonpool_rpc::security::tls::Tls),
}

impl Transport {
    /// Whether sessions are encrypted.
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        match self {
            Self::Plaintext => false,
            #[cfg(feature = "tls")]
            Self::Tls(_) => true,
        }
    }

    /// A short name for logs: `plaintext` or `tls`.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Plaintext => "plaintext",
            #[cfg(feature = "tls")]
            Self::Tls(_) => "tls",
        }
    }

    /// Bind `address` and build a runtime under `config` that accepts and
    /// dials with this transport. Nothing is served until the returned
    /// future is polled.
    ///
    /// # Errors
    ///
    /// A configuration the runtime refuses or a failed bind.
    pub async fn listen<P: ParosProviders>(
        &self,
        providers: P,
        address: &str,
        config: RpcConfig,
    ) -> io::Result<(RuntimeFuture, RpcHandle<P>)> {
        match self {
            Self::Plaintext => {
                let (driver, handle) = RpcDriver::listen(providers, address, config).await?;
                Ok((Box::pin(driver.run()), handle))
            }
            #[cfg(feature = "tls")]
            Self::Tls(tls) => {
                let (driver, handle) =
                    RpcDriver::listen_with(providers, address, config, tls.clone()).await?;
                Ok((Box::pin(driver.run()), handle))
            }
        }
    }

    /// Build a runtime under `config` that only dials, with this transport.
    ///
    /// # Errors
    ///
    /// A configuration the runtime refuses.
    pub fn client_only<P: ParosProviders>(
        &self,
        providers: P,
        config: RpcConfig,
    ) -> io::Result<(RuntimeFuture, RpcHandle<P>)> {
        match self {
            Self::Plaintext => {
                let (driver, handle) = RpcDriver::client_only(providers, config)?;
                Ok((Box::pin(driver.run()), handle))
            }
            #[cfg(feature = "tls")]
            Self::Tls(tls) => {
                let (driver, handle) = RpcDriver::client_only_with(providers, config, tls.clone())?;
                Ok((Box::pin(driver.run()), handle))
            }
        }
    }
}
