use tb_common::{TbError, TbResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectVersion {
    pub major: u16,
    pub minor: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Lwm2mPath {
    pub object_id: u16,
    pub version: Option<ObjectVersion>,
    pub instance_id: Option<u16>,
    pub resource_id: Option<u16>,
    pub resource_instance_id: Option<u16>,
}

impl Lwm2mPath {
    pub fn parse(raw: &str) -> TbResult<Self> {
        let path = raw
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .trim_start_matches('<')
            .trim_end_matches('>')
            .trim_matches('/');
        let parts: Vec<_> = path.split('/').collect();
        if parts.is_empty() || parts.len() > 4 || parts.iter().any(|part| part.is_empty()) {
            return Err(invalid_path(raw));
        }
        let (object_id, version) = parse_object(parts[0]).ok_or_else(|| invalid_path(raw))?;
        let number = |part: &str| part.parse::<u16>().ok().filter(|_| part.len() <= 5);
        Ok(Self {
            object_id,
            version,
            instance_id: parts.get(1).and_then(|part| number(part)),
            resource_id: parts.get(2).and_then(|part| number(part)),
            resource_instance_id: parts.get(3).and_then(|part| number(part)),
        })
    }

    pub fn parse_registration_links(input: &[u8]) -> TbResult<Vec<Self>> {
        let input = std::str::from_utf8(input)
            .map_err(|_| TbError::InvalidInput("registration links are not UTF-8".into()))?;
        if input.trim().is_empty() {
            return Ok(Vec::new());
        }
        input
            .split(',')
            .filter(|link| {
                !link
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .trim_start_matches('<')
                    .trim_end_matches('>')
                    .trim_matches('/')
                    .is_empty()
            })
            .map(|link| Self::parse(link.trim()))
            .collect()
    }
}

fn parse_object(raw: &str) -> Option<(u16, Option<ObjectVersion>)> {
    let (object, version) = raw
        .split_once('_')
        .map_or((raw, None), |parts| (parts.0, Some(parts.1)));
    let object_id = object.parse().ok()?;
    let version = match version {
        Some(raw) => {
            let (major, minor) = raw.split_once('.')?;
            Some(ObjectVersion {
                major: major.parse().ok()?,
                minor: minor.parse().ok()?,
            })
        }
        None => None,
    };
    Some((object_id, version))
}

fn invalid_path(raw: &str) -> TbError {
    TbError::InvalidInput(format!("invalid LwM2M path: {raw}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versioned_paths_and_registration_links_parse() {
        let path = Lwm2mPath::parse("</3303_1.1/0/5700/2>;pmin=5").unwrap();
        assert_eq!(path.object_id, 3303);
        assert_eq!(path.version, Some(ObjectVersion { major: 1, minor: 1 }));
        assert_eq!(path.instance_id, Some(0));
        assert_eq!(path.resource_id, Some(5700));
        assert_eq!(path.resource_instance_id, Some(2));
        let links = Lwm2mPath::parse_registration_links(b"</>,</1/0>,</3303_1.1/0/5700>").unwrap();
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].object_id, 1);
    }

    #[test]
    fn leshan_registration_links_parse() {
        let links = Lwm2mPath::parse_registration_links(
            br#"</>;rt="oma.lwm2m";ct="60 110 112 11542 11543",</1/0>,</3>;ver=1.2,</3/0>,</3303/0>"#,
        )
        .unwrap();
        assert_eq!(links.len(), 4);
        assert_eq!(links[3].object_id, 3303);
    }

    #[test]
    fn malformed_paths_are_rejected() {
        for raw in ["", "/a/0", "</1//2>", "</1/2/3/4/5>", "</1_2/0>"] {
            assert!(Lwm2mPath::parse(raw).is_err(), "{raw}");
        }
    }
}
