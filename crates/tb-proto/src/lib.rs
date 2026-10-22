//! tb-proto：服务间 protobuf 契约（由 ../../proto 生成）。

pub mod common {
    include!(concat!(env!("OUT_DIR"), "/tb.common.rs"));
}

pub mod transport {
    include!(concat!(env!("OUT_DIR"), "/tb.transport.rs"));
}

pub mod core {
    include!(concat!(env!("OUT_DIR"), "/tb.core.rs"));
}

pub mod ruleengine {
    include!(concat!(env!("OUT_DIR"), "/tb.ruleengine.rs"));
}

pub mod convert;
