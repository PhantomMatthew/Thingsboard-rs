use tb_common::{TbError, TbResult};

const MAX_DEPTH: usize = 8;

#[derive(Debug, Clone, PartialEq)]
pub enum Tlv<'a> {
    ObjectInstance { id: u16, children: Vec<Tlv<'a>> },
    ResourceInstance { id: u16, value: &'a [u8] },
    MultipleResource { id: u16, children: Vec<Tlv<'a>> },
    Resource { id: u16, value: &'a [u8] },
}

impl Tlv<'_> {
    pub fn id(&self) -> u16 {
        match self {
            Self::ObjectInstance { id, .. }
            | Self::ResourceInstance { id, .. }
            | Self::MultipleResource { id, .. }
            | Self::Resource { id, .. } => *id,
        }
    }

    fn kind(&self) -> u8 {
        match self {
            Self::ObjectInstance { .. } => 0b00,
            Self::ResourceInstance { .. } => 0b01,
            Self::MultipleResource { .. } => 0b10,
            Self::Resource { .. } => 0b11,
        }
    }

    fn children(&self) -> Option<&[Tlv<'_>]> {
        match self {
            Self::ObjectInstance { children, .. } | Self::MultipleResource { children, .. } => {
                Some(children)
            }
            _ => None,
        }
    }

    fn value(&self) -> Option<&[u8]> {
        match self {
            Self::ResourceInstance { value, .. } | Self::Resource { value, .. } => Some(value),
            _ => None,
        }
    }
}

pub fn parse(input: &[u8]) -> TbResult<Vec<Tlv<'_>>> {
    let (items, consumed) = parse_many(input, 0)?;
    if consumed != input.len() {
        return Err(invalid("trailing TLV bytes"));
    }
    Ok(items)
}

pub fn encode(items: &[Tlv<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    for item in items {
        encode_one(item, &mut out);
    }
    out
}

fn parse_many(input: &[u8], depth: usize) -> TbResult<(Vec<Tlv<'_>>, usize)> {
    if depth > MAX_DEPTH {
        return Err(invalid("TLV nesting is too deep"));
    }
    let mut items = Vec::new();
    let mut offset = 0;
    while offset < input.len() {
        let (item, consumed) = parse_one(&input[offset..], depth)?;
        if consumed == 0 {
            return Err(invalid("empty TLV item"));
        }
        offset += consumed;
        items.push(item);
    }
    Ok((items, offset))
}

fn parse_one(input: &[u8], depth: usize) -> TbResult<(Tlv<'_>, usize)> {
    if input.len() < 2 {
        return Err(invalid("truncated TLV header"));
    }
    let header = input[0];
    let kind = header >> 6;
    let id_len = if header & 0b0010_0000 == 0 { 1 } else { 2 };
    let length_type = (header >> 3) & 0b11;
    let extra = match length_type {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 3,
    };
    let header_len = 1 + id_len + extra;
    if input.len() < header_len {
        return Err(invalid("truncated TLV length"));
    }
    let id = if id_len == 1 {
        u16::from(input[1])
    } else {
        u16::from_be_bytes([input[1], input[2]])
    };
    let length = if extra == 0 {
        usize::from(header & 0b111)
    } else {
        (0..extra).fold(0_usize, |length, index| {
            (length << 8) | usize::from(input[1 + id_len + index])
        })
    };
    let end = header_len
        .checked_add(length)
        .ok_or_else(|| invalid("TLV length overflow"))?;
    if end > input.len() {
        return Err(invalid("truncated TLV value"));
    }
    let value = &input[header_len..end];
    let item = match kind {
        0b00 => Tlv::ObjectInstance {
            id,
            children: parse_many(value, depth + 1)?.0,
        },
        0b01 => Tlv::ResourceInstance { id, value },
        0b10 => Tlv::MultipleResource {
            id,
            children: parse_many(value, depth + 1)?.0,
        },
        0b11 => Tlv::Resource { id, value },
        _ => return Err(invalid("unknown TLV kind")),
    };
    Ok((item, end))
}

fn encode_one(item: &Tlv<'_>, out: &mut Vec<u8>) {
    let mut header = item.kind() << 6;
    if item.id() > u16::from(u8::MAX) {
        header |= 0b0010_0000;
    }
    let body = match item.children() {
        Some(children) => encode(children),
        None => item.value().unwrap_or_default().to_vec(),
    };
    match body.len() {
        0..=7 => header |= body.len() as u8,
        8..=255 => header |= 0b0000_1000,
        256..=65_535 => header |= 0b0001_0000,
        _ => header |= 0b0001_1000,
    }
    out.push(header);
    if header & 0b0010_0000 == 0 {
        out.push(item.id() as u8);
    } else {
        out.extend_from_slice(&item.id().to_be_bytes());
    }
    match body.len() {
        0..=7 => {}
        8..=255 => out.push(body.len() as u8),
        256..=65_535 => out.extend_from_slice(&(body.len() as u16).to_be_bytes()),
        _ => out.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]),
    }
    out.extend_from_slice(&body);
}

fn invalid(message: impl Into<String>) -> TbError {
    TbError::InvalidInput(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_header_forms_round_trip() {
        let long = vec![7; 300];
        let items = vec![
            Tlv::ObjectInstance {
                id: 0,
                children: vec![
                    Tlv::Resource {
                        id: 5700,
                        value: &long,
                    },
                    Tlv::MultipleResource {
                        id: 2,
                        children: vec![Tlv::ResourceInstance {
                            id: 0,
                            value: b"abc",
                        }],
                    },
                ],
            },
            Tlv::Resource {
                id: 3,
                value: &long,
            },
        ];
        let encoded = encode(&items);
        assert_eq!(parse(&encoded).unwrap(), items);
    }

    #[test]
    fn malformed_inputs_are_rejected() {
        assert!(parse(&[0b1100_0000]).is_err());
        assert!(parse(&[0b1100_1000, 1, 4, 1, 2]).is_err());
        assert!(parse(&[0b1100_0001, 1, 0, 0]).is_err());
    }
}
