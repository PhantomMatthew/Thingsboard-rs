use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use sha3::{Digest, Sha3_256};

use crate::{TbError, TbResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct X509Credential {
    pub certificate_base64: String,
    pub credentials_id: String,
}

pub fn x509_cert_hash(certificate_base64: &str) -> String {
    format!("{:x}", Sha3_256::digest(certificate_base64.as_bytes()))
}

pub fn parse_x509_credential(input: &str) -> TbResult<X509Credential> {
    let input = input.trim();
    if input.is_empty() {
        return Err(TbError::InvalidInput(
            "X509 certificate must not be empty".into(),
        ));
    }
    let base64 = input
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("-----BEGIN ") && !line.starts_with("-----END "))
        .collect::<String>();
    let der = STANDARD
        .decode(base64)
        .map_err(|_| TbError::InvalidInput("X509 certificate is not valid PEM or Base64".into()))?;
    let (remaining, _) = x509_parser::parse_x509_certificate(&der)
        .map_err(|_| TbError::InvalidInput("invalid X509 DER certificate".into()))?;
    if !remaining.is_empty() {
        return Err(TbError::InvalidInput(
            "exactly one X509 certificate is required".into(),
        ));
    }
    let certificate_base64 = STANDARD.encode(der);
    let credentials_id = x509_cert_hash(&certificate_base64);
    Ok(X509Credential {
        certificate_base64,
        credentials_id,
    })
}

pub fn x509_cert_hash_from_der(der: &[u8]) -> TbResult<String> {
    let credential = parse_x509_credential(&STANDARD.encode(der))?;
    Ok(credential.credentials_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_compatible_hash_vector() {
        assert_eq!(
            x509_cert_hash("AQID"),
            "a055efda602931f78b0a080e1ea91cb81e5c616a080337fbd7507dcb9bbd346b"
        );
    }

    #[test]
    fn pem_base64_and_crlf_have_same_credentials_id() {
        let cert = rcgen::generate_simple_self_signed(["device.example.com".into()]).unwrap();
        let der = cert.cert.der().to_vec();
        let pem = cert.cert.pem();
        let bare = STANDARD.encode(&der);
        let from_pem = parse_x509_credential(&pem).unwrap();
        let from_bare = parse_x509_credential(&bare).unwrap();
        let from_crlf = parse_x509_credential(&pem.replace('\n', "\r\n")).unwrap();
        assert_eq!(from_pem, from_bare);
        assert_eq!(from_pem, from_crlf);
        assert_eq!(from_pem.certificate_base64, bare);
        assert_eq!(
            x509_cert_hash_from_der(&der).unwrap(),
            from_pem.credentials_id
        );
    }

    #[test]
    fn invalid_or_multiple_certificates_are_rejected() {
        assert!(parse_x509_credential("").is_err());
        assert!(parse_x509_credential("not-a-certificate").is_err());
        let cert = rcgen::generate_simple_self_signed(["device.example.com".into()]).unwrap();
        let pem = cert.cert.pem();
        assert!(parse_x509_credential(&format!("{pem}{pem}")).is_err());
    }
}
