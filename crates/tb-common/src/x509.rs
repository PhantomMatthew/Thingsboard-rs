use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use regex::Regex;
use serde_json::Value;
use sha3::{Digest, Sha3_256};

use crate::{TbError, TbResult};

pub const X509_CERTIFICATE_CHAIN: &str = "X509_CERTIFICATE_CHAIN";
pub const MAX_X509_CHAIN_CERTIFICATES: usize = 8;
pub const MAX_X509_CHAIN_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub struct X509ChainProvisionConfiguration {
    pub certificate_der: Vec<u8>,
    pub certificate_regex: Regex,
    pub allow_create_new_devices: bool,
    pub provision_device_key: String,
}

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

pub fn x509_credential_from_der(der: &[u8]) -> TbResult<X509Credential> {
    parse_x509_credential(&STANDARD.encode(der))
}

pub fn x509_cert_hash_from_der(der: &[u8]) -> TbResult<String> {
    Ok(x509_credential_from_der(der)?.credentials_id)
}

pub fn validate_x509_chain_limits(chain: &[Vec<u8>]) -> TbResult<()> {
    if chain.is_empty() || chain.len() > MAX_X509_CHAIN_CERTIFICATES {
        return Err(TbError::InvalidInput(format!(
            "X509 certificate chain must contain 1 to {MAX_X509_CHAIN_CERTIFICATES} certificates"
        )));
    }
    let total_bytes = chain.iter().try_fold(0usize, |total, certificate| {
        if certificate.is_empty() {
            return Err(TbError::InvalidInput(
                "X509 certificate chain contains an empty certificate".into(),
            ));
        }
        total
            .checked_add(certificate.len())
            .ok_or_else(|| TbError::InvalidInput("X509 certificate chain size overflow".into()))
    })?;
    if total_bytes > MAX_X509_CHAIN_BYTES {
        return Err(TbError::InvalidInput(format!(
            "X509 certificate chain exceeds {MAX_X509_CHAIN_BYTES} bytes"
        )));
    }
    Ok(())
}

pub fn x509_chain_provision_configuration(
    profile_data: &Value,
) -> TbResult<X509ChainProvisionConfiguration> {
    let configuration = profile_data
        .get("provisionConfiguration")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            TbError::InvalidInput("X509 chain provisionConfiguration is required".into())
        })?;
    let certificate = configuration
        .get("certificate")
        .or_else(|| profile_data.get("certificate"))
        .and_then(Value::as_str)
        .ok_or_else(|| TbError::InvalidInput("X509 chain certificate is required".into()))?;
    let certificate = parse_x509_credential(certificate)?;
    let pattern = configuration
        .get("certificateRegExPattern")
        .and_then(Value::as_str)
        .ok_or_else(|| TbError::InvalidInput("X509 certificateRegExPattern is required".into()))?;
    let certificate_regex = Regex::new(pattern)
        .map_err(|e| TbError::InvalidInput(format!("invalid X509 certificateRegExPattern: {e}")))?;
    if certificate_regex.captures_len() < 2 {
        return Err(TbError::InvalidInput(
            "X509 certificateRegExPattern must contain capture group 1".into(),
        ));
    }
    let allow_create_new_devices = match configuration.get("allowCreateNewDevicesByX509Certificate")
    {
        None | Some(Value::Null) => false,
        Some(value) => value.as_bool().ok_or_else(|| {
            TbError::InvalidInput("allowCreateNewDevicesByX509Certificate must be a boolean".into())
        })?,
    };
    let certificate_der = STANDARD
        .decode(&certificate.certificate_base64)
        .map_err(|e| TbError::InvalidInput(format!("invalid normalized X509 certificate: {e}")))?;
    Ok(X509ChainProvisionConfiguration {
        provision_device_key: certificate.credentials_id,
        certificate_der,
        certificate_regex,
        allow_create_new_devices,
    })
}

pub fn normalize_x509_chain_profile(
    provision_type: &str,
    profile_data: &mut Value,
) -> TbResult<Option<X509ChainProvisionConfiguration>> {
    if provision_type != X509_CERTIFICATE_CHAIN {
        return Ok(None);
    }
    let configuration = x509_chain_provision_configuration(profile_data)?;
    let provision = profile_data
        .get_mut("provisionConfiguration")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| {
            TbError::InvalidInput("X509 chain provisionConfiguration is required".into())
        })?;
    provision.insert(
        "provisionDeviceKey".into(),
        Value::String(configuration.provision_device_key.clone()),
    );
    provision.insert(
        "allowCreateNewDevicesByX509Certificate".into(),
        Value::Bool(configuration.allow_create_new_devices),
    );
    Ok(Some(configuration))
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

    #[test]
    fn chain_profile_is_validated_and_key_is_derived_from_certificate() {
        let cert = rcgen::generate_simple_self_signed(["ca.example.com".into()]).unwrap();
        let credential = parse_x509_credential(&cert.cert.pem()).unwrap();
        let mut profile = serde_json::json!({
            "provisionConfiguration": {
                "certificate": cert.cert.pem(),
                "certificateRegExPattern": "(.*)"
            }
        });
        let parsed = normalize_x509_chain_profile(X509_CERTIFICATE_CHAIN, &mut profile)
            .unwrap()
            .unwrap();
        assert!(!parsed.allow_create_new_devices);
        assert_eq!(
            profile["provisionConfiguration"]["provisionDeviceKey"],
            credential.credentials_id
        );
        assert_eq!(
            profile["provisionConfiguration"]["allowCreateNewDevicesByX509Certificate"],
            false
        );

        for pattern in ["[", "no-capture"] {
            profile["provisionConfiguration"]["certificateRegExPattern"] = Value::from(pattern);
            assert!(normalize_x509_chain_profile(X509_CERTIFICATE_CHAIN, &mut profile).is_err());
        }
    }

    #[test]
    fn chain_limits_reject_empty_oversized_and_too_many_certificates() {
        assert!(validate_x509_chain_limits(&[]).is_err());
        assert!(validate_x509_chain_limits(&[Vec::new()]).is_err());
        assert!(validate_x509_chain_limits(&vec![vec![1]; 9]).is_err());
        assert!(validate_x509_chain_limits(&[vec![0; MAX_X509_CHAIN_BYTES + 1]]).is_err());
        assert!(validate_x509_chain_limits(&[vec![1; 32], vec![2; 32]]).is_ok());
    }
}
