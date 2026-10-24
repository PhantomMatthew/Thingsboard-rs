use std::fs::File;
use std::io::{BufReader, Error, ErrorKind};
use std::sync::Arc;

use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::{WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::{TransportState, session};

#[derive(Debug)]
struct PinnedClientCertVerifier {
    supported: WebPkiSupportedAlgorithms,
    roots: Vec<DistinguishedName>,
}

impl PinnedClientCertVerifier {
    fn new() -> Self {
        Self {
            supported: rustls::crypto::ring::default_provider().signature_verification_algorithms,
            roots: Vec::new(),
        }
    }
}

impl ClientCertVerifier for PinnedClientCertVerifier {
    fn client_auth_mandatory(&self) -> bool {
        false
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &self.roots
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let (_, cert) = x509_parser::parse_x509_certificate(end_entity.as_ref())
            .map_err(|_| rustls::Error::InvalidCertificate(CertificateError::BadEncoding))?;
        let now = now.as_secs() as i64;
        if now < cert.validity().not_before.timestamp()
            || now > cert.validity().not_after.timestamp()
        {
            return Err(rustls::Error::InvalidCertificate(CertificateError::Expired));
        }
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

pub fn acceptor_from_files(cert_path: &str, key_path: &str) -> std::io::Result<TlsAcceptor> {
    let mut cert_reader = BufReader::new(
        File::open(cert_path).map_err(|e| Error::new(e.kind(), format!("open TLS cert: {e}")))?,
    );
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("parse TLS cert: {e}")))?;
    if certs.is_empty() {
        return Err(Error::new(ErrorKind::InvalidData, "TLS cert is empty"));
    }
    let mut key_reader = BufReader::new(
        File::open(key_path).map_err(|e| Error::new(e.kind(), format!("open TLS key: {e}")))?,
    );
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("parse TLS key: {e}")))?
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "TLS private key is missing"))?;
    let config = ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(PinnedClientCertVerifier::new()))
        .with_single_cert(certs, key)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("invalid TLS keypair: {e}")))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub async fn serve_tls(
    listener: TcpListener,
    state: TransportState,
    acceptor: TlsAcceptor,
) -> std::io::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let acceptor = acceptor.clone();
        let state = state.clone();
        tokio::spawn(async move {
            tracing::debug!(%peer, "MQTTS socket accepted");
            match acceptor.accept(socket).await {
                Ok(stream) => {
                    tracing::debug!(%peer, "MQTTS handshake completed");
                    let cert = stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(|certs| certs.first())
                        .map(|cert| cert.as_ref().to_vec());
                    session::run_with_x509(Box::new(stream), peer, state, cert).await;
                }
                Err(e) => tracing::debug!(%peer, error = %e, "MQTTS handshake failed"),
            }
        });
    }
}
