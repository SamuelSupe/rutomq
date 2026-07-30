use crate::config::SecurityConfig;
use anyhow::{Context, Result, anyhow};
use std::fs::File;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{
    ServerConfig, crypto,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};

pub fn acceptor(config: &SecurityConfig) -> Result<Option<TlsAcceptor>> {
    let (cert_path, key_path) = match (&config.tls_cert_file, &config.tls_key_file) {
        (None, None) => return Ok(None),
        (Some(cert_path), Some(key_path)) => (cert_path, key_path),
        _ => {
            return Err(anyhow!(
                "TLS certificate and private key must be configured together"
            ));
        }
    };

    let cert_file = File::open(cert_path)
        .with_context(|| format!("open TLS certificate {}", cert_path.display()))?;
    let certificates = CertificateDer::pem_reader_iter(cert_file)
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("read TLS certificate {}", cert_path.display()))?;
    if certificates.is_empty() {
        return Err(anyhow!(
            "TLS certificate file {} contains no certificates",
            cert_path.display()
        ));
    }

    let key_file =
        File::open(key_path).with_context(|| format!("open TLS key {}", key_path.display()))?;
    let private_key = match PrivateKeyDer::from_pem_reader(key_file) {
        Ok(private_key) => private_key,
        Err(tokio_rustls::rustls::pki_types::pem::Error::NoItemsFound) => {
            return Err(anyhow!(
                "TLS key file {} contains no private key",
                key_path.display()
            ));
        }
        Err(error) => {
            return Err(error).with_context(|| format!("read TLS key {}", key_path.display()));
        }
    };
    let _ = crypto::aws_lc_rs::default_provider().install_default();
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .context("TLS certificate and private key do not match")?;
    Ok(Some(TlsAcceptor::from(Arc::new(server))))
}
