/// 分区路由：与 ThingsBoard 一致，按 entity_id 的 murmur3_128 hash 取模。
///
/// 保证同一实体的所有消息进入同一分区，单消费者内有序处理。
use bytes::Bytes;

pub fn partition_for(entity_id: &str, partitions: i32) -> i32 {
    debug_assert!(partitions > 0);
    let hash = murmur3_128(entity_id.as_bytes());
    // 使用低 64 位，对齐 Java 端 murmur3_128 的正数取模语义
    ((hash as i64 & 0x7fff_ffff_ffff_ffff) % partitions as i64) as i32
}

/// Kafka 消息 key：直接用 entity_id 字节，让 Kafka 的默认 partitioner
/// （murmur2）做分布；需要精确对齐分区时由调用方显式指定 partition。
pub fn key_of(entity_id: &str) -> Bytes {
    Bytes::copy_from_slice(entity_id.as_bytes())
}

fn murmur3_128(data: &[u8]) -> u64 {
    // MurmurHash3 x64 128-bit 的低 64 位实现
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    let len = data.len();
    let nblocks = len / 16;
    let mut h1: u64 = 0;
    let mut h2: u64 = 0;

    for i in 0..nblocks {
        let k1 = u64::from_le_bytes(data[i * 16..i * 16 + 8].try_into().unwrap());
        let k2 = u64::from_le_bytes(data[i * 16 + 8..i * 16 + 16].try_into().unwrap());

        let mut k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;

        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        let mut k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;

        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let tail = &data[nblocks * 16..];
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;
    for (i, &b) in tail.iter().enumerate() {
        if i < 8 {
            k1 ^= (b as u64) << (i * 8);
        } else {
            k2 ^= (b as u64) << ((i - 8) * 8);
        }
    }
    if k2 != 0 {
        k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
        h2 ^= k2;
    }
    if k1 != 0 {
        k1 = k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= len as u64;
    h2 ^= len as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1.wrapping_add(h2)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_in_range() {
        for id in ["a", "device-1", "0198f1c2-0000-7000-8000-000000000000"] {
            let p = partition_for(id, 10);
            assert!((0..10).contains(&p));
        }
    }

    #[test]
    fn deterministic() {
        assert_eq!(partition_for("x", 16), partition_for("x", 16));
    }
}
