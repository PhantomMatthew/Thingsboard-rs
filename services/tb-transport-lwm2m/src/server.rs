use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use coap_lite::option_value::OptionValueString;
use coap_lite::{
    CoapOption, ContentFormat, MessageClass, MessageType, Packet, RequestType, ResponseType,
};
use dashmap::DashMap;
use tb_common::{TbError, TbResult};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;

use crate::integration::{
    EndpointIdentity, EndpointValidator, SessionActivity, UplinkSink, new_session_id,
};
use crate::model::Lwm2mPath;
use crate::profile::{ObservedResource, decode_resource, resources_from_profile};
use tb_proto::core as core_pb;

const DEFAULT_LIFETIME: u64 = 86_400;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub node_id: String,
    pub default_lifetime: Duration,
    pub max_lifetime: Duration,
    pub ack_timeout: Duration,
    pub max_ack_timeout: Duration,
    pub max_retransmissions: u8,
    pub cleanup_interval: Duration,
    pub dedup_ttl: Duration,
    pub max_datagram_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:5683".parse().unwrap(),
            node_id: "lwm2m".into(),
            default_lifetime: Duration::from_secs(DEFAULT_LIFETIME),
            max_lifetime: Duration::from_secs(31_536_000),
            ack_timeout: Duration::from_secs(2),
            max_ack_timeout: Duration::from_secs(32),
            max_retransmissions: 4,
            cleanup_interval: Duration::from_secs(1),
            dedup_ttl: Duration::from_secs(247),
            max_datagram_size: 64 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct Lwm2mServer {
    inner: Arc<Inner>,
}

struct Inner {
    config: ServerConfig,
    validator: Arc<dyn EndpointValidator>,
    uplink: Arc<dyn UplinkSink>,
    activity: Arc<dyn SessionActivity>,
    registrations: DashMap<String, Arc<Registration>>,
    endpoints: DashMap<String, String>,
    registration_commit: Mutex<()>,
    pending: DashMap<Vec<u8>, Arc<PendingExchange>>,
    pending_by_mid: DashMap<(SocketAddr, u16), Arc<PendingExchange>>,
    observations: DashMap<Vec<u8>, Arc<Observation>>,
    notification_dedup: DashMap<(SocketAddr, Vec<u8>, u16), Instant>,
    request_cache: DashMap<(SocketAddr, u16), Arc<CachedResponse>>,
    socket: std::sync::OnceLock<Arc<UdpSocket>>,
    next_mid: AtomicU16,
    next_token: AtomicU64,
}

type DispatchResult = io::Result<(Vec<u8>, Option<Arc<Registration>>)>;

struct CachedResponse {
    result: tokio::sync::OnceCell<DispatchResult>,
    stored_at: Instant,
}

struct Registration {
    id: String,
    endpoint: String,
    peer: StdMutex<SocketAddr>,
    identity: EndpointIdentity,
    session: core_pb::SessionInfoProto,
    resources: Vec<ObservedResource>,
    links: StdMutex<Vec<Lwm2mPath>>,
    lifetime: StdMutex<Duration>,
    expires_at: Mutex<Instant>,
    active: AtomicBool,
}

struct PendingExchange {
    registration_id: String,
    peer: StdMutex<SocketAddr>,
    request_mid: u16,
    response: Mutex<Option<Packet>>,
    acknowledged: AtomicBool,
    reset: AtomicBool,
    notify: Notify,
}

struct Observation {
    registration_id: String,
    peer: StdMutex<SocketAddr>,
    request_mid: u16,
    latest_notification: StdMutex<Option<(u32, Instant)>>,
    resource: ObservedResource,
}

struct ExchangeOutcome {
    packet: Packet,
    timestamp_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExchangeError {
    Timeout,
    Reset,
    Closed,
    Send,
}

impl Lwm2mServer {
    pub fn new(
        config: ServerConfig,
        validator: Arc<dyn EndpointValidator>,
        uplink: Arc<dyn UplinkSink>,
        activity: Arc<dyn SessionActivity>,
    ) -> TbResult<Self> {
        if config.default_lifetime.is_zero()
            || config.max_lifetime < config.default_lifetime
            || config.ack_timeout.is_zero()
            || config.max_ack_timeout < config.ack_timeout
            || config.cleanup_interval.is_zero()
            || config.max_datagram_size < 64
        {
            return Err(TbError::InvalidInput("invalid LwM2M server config".into()));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                validator,
                uplink,
                activity,
                registrations: DashMap::new(),
                endpoints: DashMap::new(),
                registration_commit: Mutex::new(()),
                pending: DashMap::new(),
                pending_by_mid: DashMap::new(),
                observations: DashMap::new(),
                notification_dedup: DashMap::new(),
                request_cache: DashMap::new(),
                socket: std::sync::OnceLock::new(),
                next_mid: AtomicU16::new(1),
                next_token: AtomicU64::new(1),
            }),
        })
    }

    pub async fn run(self) -> io::Result<()> {
        let socket = UdpSocket::bind(self.inner.config.bind_addr).await?;
        self.serve(socket).await
    }

    pub async fn serve(self, socket: UdpSocket) -> io::Result<()> {
        let socket = Arc::new(socket);
        self.inner
            .socket
            .set(socket.clone())
            .map_err(|_| io::Error::other("LwM2M server is already running"))?;
        let mut buffer = vec![0; self.inner.config.max_datagram_size];
        let mut cleanup = tokio::time::interval(self.inner.config.cleanup_interval);
        cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                received = socket.recv_from(&mut buffer) => {
                    let (size, peer) = received?;
                    let Ok(packet) = Packet::from_bytes(&buffer[..size]) else {
                        tracing::debug!(%peer, "undecodable LwM2M CoAP packet");
                        continue;
                    };
                    let server = self.clone();
                    let socket = socket.clone();
                    tokio::spawn(async move {
                        if let Err(error) = server.handle_packet(socket, packet, peer).await {
                            tracing::debug!(%peer, %error, "LwM2M UDP send failed");
                        }
                    });
                }
                _ = cleanup.tick() => self.cleanup().await,
            }
        }
    }

    pub fn registration_count(&self) -> usize {
        self.inner.registrations.len()
    }

    async fn handle_packet(
        &self,
        socket: Arc<UdpSocket>,
        packet: Packet,
        peer: SocketAddr,
    ) -> io::Result<()> {
        let message_type = packet.header.get_type();
        if matches!(message_type, MessageType::Reset)
            || matches!(packet.header.code, MessageClass::Response(_))
            || (message_type == MessageType::Acknowledgement
                && packet.header.code == MessageClass::Empty)
        {
            self.handle_response(&socket, packet, peer).await?;
            return Ok(());
        }
        if !matches!(packet.header.code, MessageClass::Request(_)) {
            return Ok(());
        }
        if message_type != MessageType::Confirmable {
            let (bytes, registered) = self.dispatch(&packet, peer).await?;
            socket.send_to(&bytes, peer).await?;
            if let Some(registration) = registered {
                self.start_initial_requests(registration);
            }
            return Ok(());
        }
        let cache_key = (peer, packet.header.message_id);
        let (cached, owner) = loop {
            match self.inner.request_cache.entry(cache_key) {
                dashmap::mapref::entry::Entry::Occupied(entry) => {
                    if entry.get().stored_at.elapsed() < self.inner.config.dedup_ttl {
                        break (entry.get().clone(), false);
                    }
                    entry.remove();
                }
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    break (
                        entry
                            .insert(Arc::new(CachedResponse {
                                result: tokio::sync::OnceCell::new(),
                                stored_at: Instant::now(),
                            }))
                            .clone(),
                        true,
                    );
                }
            }
        };
        let result = cached
            .result
            .get_or_init(|| self.dispatch(&packet, peer))
            .await;
        let (bytes, registered) = match result {
            Ok(result) => result,
            Err(error) => {
                if owner {
                    self.inner.request_cache.remove(&cache_key);
                }
                return Err(io::Error::other(error.to_string()));
            }
        };
        socket.send_to(bytes, peer).await?;
        if owner && let Some(registration) = registered {
            self.start_initial_requests(registration.clone());
        }
        Ok(())
    }

    async fn dispatch(
        &self,
        request: &Packet,
        peer: SocketAddr,
    ) -> io::Result<(Vec<u8>, Option<Arc<Registration>>)> {
        let segments = uri_path(request)?;
        let queries = uri_query(request)?;
        match (request.header.code, segments.as_slice()) {
            (MessageClass::Request(RequestType::Post), [rd]) if rd == "rd" => {
                self.register(request, peer, &queries).await
            }
            (MessageClass::Request(RequestType::Put), [rd, location]) if rd == "rd" => {
                let result = self
                    .update(location, peer, &queries, &request.payload)
                    .await;
                Ok((self.response(request, result, None)?, None))
            }
            (MessageClass::Request(RequestType::Delete), [rd, location]) if rd == "rd" => {
                let result = self.delete(location, peer).await;
                Ok((self.response(request, result, None)?, None))
            }
            (MessageClass::Request(_), _) => Ok((
                self.response(request, Err(ResponseType::NotFound), None)?,
                None,
            )),
            _ => Ok((
                self.response(request, Err(ResponseType::BadRequest), None)?,
                None,
            )),
        }
    }

    async fn register(
        &self,
        request: &Packet,
        peer: SocketAddr,
        queries: &HashMap<String, String>,
    ) -> io::Result<(Vec<u8>, Option<Arc<Registration>>)> {
        let result = self.register_inner(request, peer, queries).await;
        match result {
            Ok((registration, location)) => {
                let bytes = self.response(request, Ok(ResponseType::Created), Some(&location))?;
                Ok((bytes, Some(registration)))
            }
            Err(status) => Ok((self.response(request, Err(status), None)?, None)),
        }
    }

    async fn register_inner(
        &self,
        request: &Packet,
        peer: SocketAddr,
        queries: &HashMap<String, String>,
    ) -> Result<(Arc<Registration>, String), ResponseType> {
        if queries.get("b").is_some_and(|binding| binding != "U") {
            return Err(ResponseType::BadRequest);
        }
        let endpoint = queries.get("ep").filter(|ep| valid_endpoint(ep));
        let Some(endpoint) = endpoint else {
            return Err(ResponseType::BadRequest);
        };
        let lifetime = self.lifetime(queries)?;
        tracing::debug!(%endpoint, links = %String::from_utf8_lossy(&request.payload), "LwM2M registration links");
        let links = Lwm2mPath::parse_registration_links(&request.payload).map_err(|error| {
            tracing::debug!(%error, endpoint, "invalid LwM2M registration links");
            ResponseType::BadRequest
        })?;
        let identity = match self.inner.validator.validate(endpoint, peer).await {
            Ok(Some(identity)) => identity,
            Ok(None) => return Err(ResponseType::Unauthorized),
            Err(error) => {
                tracing::warn!(%error, endpoint, "LwM2M endpoint validation failed");
                return Err(ResponseType::InternalServerError);
            }
        };
        let resources = resources_from_profile(
            &identity.profile.profile_data_json,
            &identity.resource_types,
        )
        .map_err(|error| {
            tracing::warn!(%error, endpoint, "invalid LwM2M observeAttr profile");
            ResponseType::InternalServerError
        })?;
        let id = new_session_id();
        let session = session_info(&self.inner.config.node_id, &identity, &id);
        self.inner
            .activity
            .open(&session, &identity.device)
            .await
            .map_err(|error| {
                tracing::warn!(%error, endpoint, "LwM2M session open failed");
                ResponseType::InternalServerError
            })?;
        let registration = Arc::new(Registration {
            id: id.clone(),
            endpoint: endpoint.clone(),
            peer: StdMutex::new(peer),
            identity,
            session,
            resources,
            links: StdMutex::new(links),
            lifetime: StdMutex::new(lifetime),
            expires_at: Mutex::new(Instant::now() + lifetime),
            active: AtomicBool::new(true),
        });
        let _commit = self.inner.registration_commit.lock().await;
        if let Some(old_id) = self.inner.endpoints.insert(endpoint.clone(), id.clone())
            && old_id != id
            && let Some((_, old)) = self.inner.registrations.remove(&old_id)
        {
            self.deactivate(&old, "endpoint registered again").await;
        }
        let location = format!("rd/{}", registration.id);
        self.inner.registrations.insert(id, registration.clone());
        Ok((registration, location))
    }

    async fn update(
        &self,
        location: &str,
        peer: SocketAddr,
        queries: &HashMap<String, String>,
        payload: &[u8],
    ) -> Result<ResponseType, ResponseType> {
        let Some(registration) = self
            .inner
            .registrations
            .get(location)
            .filter(|registration| registration.active.load(Ordering::Acquire))
            .map(|registration| registration.clone())
        else {
            return Err(ResponseType::NotFound);
        };
        let lifetime = queries
            .contains_key("lt")
            .then(|| self.lifetime(queries))
            .transpose()?;
        let links = (!payload.is_empty())
            .then(|| {
                Lwm2mPath::parse_registration_links(payload).map_err(|_| ResponseType::BadRequest)
            })
            .transpose()?;
        if let Some(lifetime) = lifetime {
            *registration
                .lifetime
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = lifetime;
        }
        if let Some(links) = links {
            *registration.links.lock().unwrap_or_else(|e| e.into_inner()) = links;
        }
        if *registration.peer.lock().unwrap_or_else(|e| e.into_inner()) != peer {
            *registration.peer.lock().unwrap_or_else(|e| e.into_inner()) = peer;
            for observation in self.inner.observations.iter() {
                if observation.registration_id == registration.id {
                    *observation.peer.lock().unwrap_or_else(|e| e.into_inner()) = peer;
                }
            }
            for pending in self.inner.pending.iter() {
                if pending.registration_id == registration.id {
                    let old_peer = {
                        let mut current = pending.peer.lock().unwrap_or_else(|e| e.into_inner());
                        std::mem::replace(&mut *current, peer)
                    };
                    self.inner
                        .pending_by_mid
                        .remove_if(&(old_peer, pending.request_mid), |_, current| {
                            Arc::ptr_eq(current, &pending)
                        });
                    self.inner
                        .pending_by_mid
                        .insert((peer, pending.request_mid), pending.clone());
                }
            }
        }
        let lifetime = *registration
            .lifetime
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *registration.expires_at.lock().await = Instant::now() + lifetime;
        self.inner
            .activity
            .activity(
                &registration.session,
                &registration.identity.device,
                now_ms(),
            )
            .await
            .map_err(|_| ResponseType::InternalServerError)?;
        Ok(ResponseType::Changed)
    }

    async fn delete(&self, location: &str, peer: SocketAddr) -> Result<ResponseType, ResponseType> {
        let Some(registration) = self.registration(location, peer) else {
            return Err(ResponseType::NotFound);
        };
        if self
            .inner
            .registrations
            .remove(location)
            .map(|(_, found)| found.id == registration.id)
            != Some(true)
        {
            return Err(ResponseType::NotFound);
        }
        self.deactivate(&registration, "registration deleted").await;
        Ok(ResponseType::Deleted)
    }

    fn registration(&self, location: &str, peer: SocketAddr) -> Option<Arc<Registration>> {
        self.inner
            .registrations
            .get(location)
            .filter(|registration| {
                *registration.peer.lock().unwrap_or_else(|e| e.into_inner()) == peer
                    && registration.active.load(Ordering::Acquire)
            })
            .map(|registration| registration.clone())
    }

    fn lifetime(&self, queries: &HashMap<String, String>) -> Result<Duration, ResponseType> {
        let seconds = match queries.get("lt") {
            Some(value) => value.parse::<u64>().map_err(|_| ResponseType::BadRequest)?,
            None => self.inner.config.default_lifetime.as_secs(),
        };
        let lifetime = Duration::from_secs(seconds);
        if lifetime.is_zero() || lifetime > self.inner.config.max_lifetime {
            return Err(ResponseType::BadRequest);
        }
        Ok(lifetime)
    }

    async fn deactivate(&self, registration: &Registration, reason: &str) {
        if !registration.active.swap(false, Ordering::AcqRel) {
            return;
        }
        if self
            .inner
            .endpoints
            .get(&registration.endpoint)
            .is_some_and(|id| *id == registration.id)
        {
            self.inner.endpoints.remove(&registration.endpoint);
        }
        self.inner
            .observations
            .retain(|_, observation| observation.registration_id != registration.id);
        let tokens: Vec<Vec<u8>> = self
            .inner
            .pending
            .iter()
            .filter(|entry| entry.registration_id == registration.id)
            .map(|entry| entry.key().clone())
            .collect();
        for token in tokens {
            if let Some((_, pending)) = self.inner.pending.remove(&token) {
                pending.notify.notify_waiters();
            }
        }
        if let Err(error) = self
            .inner
            .activity
            .close(&registration.session, &registration.identity.device, reason)
            .await
        {
            tracing::warn!(%error, endpoint = registration.endpoint, "LwM2M session close failed");
        }
    }

    async fn cleanup(&self) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .inner
            .registrations
            .iter()
            .filter_map(|entry| {
                let expires = entry.expires_at.try_lock().ok()?;
                (*expires <= now).then(|| entry.key().clone())
            })
            .collect();
        for id in expired {
            if let Some((_, registration)) = self.inner.registrations.remove(&id) {
                self.deactivate(&registration, "registration lifetime expired")
                    .await;
            }
        }
        self.inner
            .notification_dedup
            .retain(|_, stored| stored.elapsed() < self.inner.config.dedup_ttl);
        self.inner
            .request_cache
            .retain(|_, cached| cached.stored_at.elapsed() < self.inner.config.dedup_ttl);
    }

    fn start_initial_requests(&self, registration: Arc<Registration>) {
        for resource in registration.resources.iter().cloned() {
            let server = self.clone();
            let registration = registration.clone();
            tokio::spawn(async move {
                match server.exchange(&registration, &resource, false).await {
                    Ok(outcome) => {
                        server
                            .publish_response(
                                &registration,
                                &resource,
                                &outcome.packet,
                                outcome.timestamp_ms,
                            )
                            .await
                    }
                    Err(error) => {
                        tracing::debug!(?error, endpoint = registration.endpoint, path = %path_string(&resource.path), "initial LwM2M Read failed");
                        return;
                    }
                }
                if !registration.active.load(Ordering::Acquire) {
                    return;
                }
                match server.exchange(&registration, &resource, true).await {
                    Ok(outcome) => {
                        server
                            .publish_response(
                                &registration,
                                &resource,
                                &outcome.packet,
                                outcome.timestamp_ms,
                            )
                            .await;
                    }
                    Err(error) => {
                        tracing::debug!(?error, endpoint = registration.endpoint, path = %path_string(&resource.path), "LwM2M Observe failed");
                    }
                }
            });
        }
    }

    async fn exchange(
        &self,
        registration: &Arc<Registration>,
        resource: &ObservedResource,
        observe: bool,
    ) -> Result<ExchangeOutcome, ExchangeError> {
        let timestamp_ms = now_ms();
        let token = self.allocate_token();
        let mid = self.inner.next_mid.fetch_add(1, Ordering::Relaxed);
        let mut request = Packet::new();
        request.header.set_type(MessageType::Confirmable);
        request.header.code = MessageClass::Request(RequestType::Get);
        request.header.message_id = mid;
        request.set_token(token.clone());
        request.add_option(CoapOption::Accept, 11542_u16.to_be_bytes().to_vec());
        for segment in path_segments(&resource.path) {
            request.add_option(CoapOption::UriPath, segment.into_bytes());
        }
        if observe {
            request.set_observe_value(0);
        }
        let bytes = request.to_bytes().map_err(|_| ExchangeError::Send)?;
        if observe {
            self.inner.observations.insert(
                token.clone(),
                Arc::new(Observation {
                    registration_id: registration.id.clone(),
                    peer: StdMutex::new(
                        *registration.peer.lock().unwrap_or_else(|e| e.into_inner()),
                    ),
                    request_mid: mid,
                    latest_notification: StdMutex::new(None),
                    resource: resource.clone(),
                }),
            );
        }
        let pending = Arc::new(PendingExchange {
            registration_id: registration.id.clone(),
            peer: StdMutex::new(*registration.peer.lock().unwrap_or_else(|e| e.into_inner())),
            request_mid: mid,
            response: Mutex::new(None),
            acknowledged: AtomicBool::new(false),
            reset: AtomicBool::new(false),
            notify: Notify::new(),
        });
        self.inner.pending.insert(token.clone(), pending.clone());
        self.inner.pending_by_mid.insert(
            (*pending.peer.lock().unwrap_or_else(|e| e.into_inner()), mid),
            pending.clone(),
        );
        let result = self.run_exchange(registration, &pending, &bytes).await;
        self.inner.pending.remove(&token);
        self.inner
            .pending_by_mid
            .retain(|_, current| !Arc::ptr_eq(current, &pending));
        if result.is_err() && observe {
            self.inner.observations.remove(&token);
        }
        result.map(|packet| ExchangeOutcome {
            packet,
            timestamp_ms,
        })
    }

    async fn run_exchange(
        &self,
        registration: &Registration,
        pending: &Arc<PendingExchange>,
        bytes: &[u8],
    ) -> Result<Packet, ExchangeError> {
        let socket = self.inner.socket.get().ok_or(ExchangeError::Send)?;
        let mut transmissions = 0_u8;
        loop {
            if let Some(response) = pending.response.lock().await.clone() {
                return Ok(response);
            }
            if !registration.active.load(Ordering::Acquire) {
                return Err(ExchangeError::Closed);
            }
            if pending.reset.load(Ordering::Acquire) {
                return Err(ExchangeError::Reset);
            }
            let acknowledged = pending.acknowledged.load(Ordering::Acquire);
            if !acknowledged && transmissions > self.inner.config.max_retransmissions {
                return Err(ExchangeError::Timeout);
            }
            let timeout = if acknowledged {
                Duration::from_secs(120)
            } else {
                let peer = *pending.peer.lock().unwrap_or_else(|e| e.into_inner());
                socket
                    .send_to(bytes, peer)
                    .await
                    .map_err(|_| ExchangeError::Send)?;
                let timeout = retransmission_timeout(&self.inner.config, transmissions);
                transmissions += 1;
                timeout
            };
            let notified = pending.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(response) = pending.response.lock().await.clone() {
                return Ok(response);
            }
            if tokio::time::timeout(timeout, notified).await.is_err() && acknowledged {
                return Err(ExchangeError::Timeout);
            }
        }
    }

    async fn publish_response(
        &self,
        registration: &Registration,
        resource: &ObservedResource,
        packet: &Packet,
        timestamp: i64,
    ) {
        if packet.header.code != MessageClass::Response(ResponseType::Content)
            || packet
                .get_content_format()
                .is_some_and(|format| format != ContentFormat::ApplicationVndOmaLwm2mTlv)
        {
            return;
        }
        let value = match decode_resource(resource, &packet.payload) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%error, endpoint = registration.endpoint, "invalid LwM2M TLV response");
                return;
            }
        };
        if let Err(error) = self
            .inner
            .uplink
            .publish(&registration.identity.device, resource, value, timestamp)
            .await
        {
            tracing::warn!(%error, endpoint = registration.endpoint, "LwM2M uplink failed");
        }
        if let Err(error) = self
            .inner
            .activity
            .activity(
                &registration.session,
                &registration.identity.device,
                timestamp,
            )
            .await
        {
            tracing::warn!(%error, endpoint = registration.endpoint, "LwM2M activity failed");
        }
    }

    async fn handle_response(
        &self,
        socket: &Arc<UdpSocket>,
        packet: Packet,
        peer: SocketAddr,
    ) -> io::Result<()> {
        if packet.header.get_type() == MessageType::Confirmable {
            socket
                .send_to(
                    &empty_message(MessageType::Acknowledgement, packet.header.message_id)?,
                    peer,
                )
                .await?;
        }
        if packet.header.get_type() == MessageType::Reset {
            self.handle_reset(&packet, peer);
            return Ok(());
        }
        if packet.header.code == MessageClass::Empty {
            if let Some(pending) = self
                .inner
                .pending_by_mid
                .get(&(peer, packet.header.message_id))
                .map(|entry| entry.clone())
            {
                pending.acknowledged.store(true, Ordering::Release);
                pending.notify.notify_waiters();
            }
            return Ok(());
        }
        let token = packet.get_token().to_vec();
        tracing::debug!(?token, message_type = ?packet.header.get_type(), observe = ?packet.get_observe_value(), "handle LwM2M response");
        if packet.header.get_type() == MessageType::Confirmable && !token.is_empty() {
            let key = (peer, token.clone(), packet.header.message_id);
            match self.inner.notification_dedup.entry(key) {
                dashmap::mapref::entry::Entry::Occupied(_) => return Ok(()),
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    entry.insert(Instant::now());
                }
            }
        }
        if let Some(pending) = self.inner.pending.get(&token).map(|entry| entry.clone()) {
            let matches = *pending.peer.lock().unwrap_or_else(|e| e.into_inner()) == peer
                && (packet.header.get_type() != MessageType::Acknowledgement
                    || packet.header.message_id == pending.request_mid);
            if matches {
                let mut response = pending.response.lock().await;
                if response.is_none() {
                    *response = Some(packet);
                    pending.notify.notify_waiters();
                    return Ok(());
                }
            }
        }
        if packet.header.get_type() == MessageType::Acknowledgement {
            return Ok(());
        }
        let Some(observation) = self
            .inner
            .observations
            .get(&token)
            .filter(|observation| {
                *observation.peer.lock().unwrap_or_else(|e| e.into_inner()) == peer
            })
            .map(|entry| entry.clone())
        else {
            tracing::debug!(?token, "LwM2M observation was not found");
            return Ok(());
        };
        let Some(sequence) = packet.get_observe_value().and_then(Result::ok) else {
            return Ok(());
        };
        if packet.header.code != MessageClass::Response(ResponseType::Content) {
            return Ok(());
        }
        let received_at = Instant::now();
        {
            let mut latest = observation
                .latest_notification
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if latest.is_some_and(|(previous, timestamp)| {
                !is_newer_notification(previous, sequence, timestamp, received_at)
            }) {
                return Ok(());
            }
            *latest = Some((sequence, received_at));
        }
        let Some(registration) = self
            .inner
            .registrations
            .get(&observation.registration_id)
            .map(|entry| entry.clone())
            .filter(|registration| registration.active.load(Ordering::Acquire))
        else {
            self.inner.observations.remove(&token);
            return Ok(());
        };
        tracing::debug!(?token, "publish LwM2M observation");
        self.publish_response(&registration, &observation.resource, &packet, now_ms())
            .await;
        Ok(())
    }

    fn handle_reset(&self, packet: &Packet, peer: SocketAddr) {
        let token = packet.get_token();
        let mid = packet.header.message_id;
        self.inner.observations.retain(|candidate, observation| {
            *observation.peer.lock().unwrap_or_else(|e| e.into_inner()) != peer
                || (candidate.as_slice() != token && observation.request_mid != mid)
        });
        let pending: Vec<Arc<PendingExchange>> = self
            .inner
            .pending
            .iter()
            .filter(|entry| {
                *entry.peer.lock().unwrap_or_else(|e| e.into_inner()) == peer
                    && (entry.key().as_slice() == token || entry.request_mid == mid)
            })
            .map(|entry| entry.value().clone())
            .collect();
        for exchange in pending {
            exchange.reset.store(true, Ordering::Release);
            exchange.notify.notify_waiters();
        }
    }

    fn response(
        &self,
        request: &Packet,
        status: Result<ResponseType, ResponseType>,
        location: Option<&str>,
    ) -> io::Result<Vec<u8>> {
        let mut response = Packet::new();
        response
            .header
            .set_type(if request.header.get_type() == MessageType::Confirmable {
                MessageType::Acknowledgement
            } else {
                MessageType::NonConfirmable
            });
        response.header.message_id = if request.header.get_type() == MessageType::Confirmable {
            request.header.message_id
        } else {
            self.inner.next_mid.fetch_add(1, Ordering::Relaxed)
        };
        response.header.code = MessageClass::Response(status.unwrap_or_else(|status| status));
        response.set_token(request.get_token().to_vec());
        if let Some(location) = location {
            for segment in location.split('/') {
                response.add_option(CoapOption::LocationPath, segment.as_bytes().to_vec());
            }
        }
        response.to_bytes().map_err(io::Error::other)
    }

    fn allocate_token(&self) -> Vec<u8> {
        loop {
            let token = self
                .inner
                .next_token
                .fetch_add(1, Ordering::Relaxed)
                .to_be_bytes()
                .to_vec();
            if !self.inner.pending.contains_key(&token)
                && !self.inner.observations.contains_key(&token)
            {
                return token;
            }
        }
    }
}

fn retransmission_timeout(config: &ServerConfig, attempt: u8) -> Duration {
    let factor = 1_u32.checked_shl(u32::from(attempt)).unwrap_or(u32::MAX);
    config
        .ack_timeout
        .checked_mul(factor)
        .unwrap_or(config.max_ack_timeout)
        .min(config.max_ack_timeout)
}

fn is_newer_notification(previous: u32, current: u32, received_at: Instant, now: Instant) -> bool {
    const OBSERVE_MASK: u32 = 0x00ff_ffff;
    const HALF_SEQUENCE_SPACE: u32 = 0x0080_0000;
    now.duration_since(received_at) >= Duration::from_secs(128)
        || (current != previous
            && current.wrapping_sub(previous) & OBSERVE_MASK < HALF_SEQUENCE_SPACE)
}

fn session_info(
    node_id: &str,
    identity: &EndpointIdentity,
    session_id: &str,
) -> core_pb::SessionInfoProto {
    let device = &identity.device;
    core_pb::SessionInfoProto {
        node_id: node_id.into(),
        session_id: session_id.into(),
        tenant_id: device.tenant_id.clone(),
        device_id: device.device_id.clone(),
        device_profile_id: device.device_profile_id.clone(),
        device_name: device.device_name.clone(),
        device_type: device.device_type.clone(),
        is_gateway: false,
        gateway_session_id: String::new(),
    }
}

fn valid_endpoint(endpoint: &str) -> bool {
    !endpoint.is_empty() && endpoint.len() <= 255 && !endpoint.chars().any(char::is_control)
}

fn uri_path(packet: &Packet) -> io::Result<Vec<String>> {
    packet
        .get_options_as::<OptionValueString>(CoapOption::UriPath)
        .unwrap_or_default()
        .into_iter()
        .map(|value| value.map(|value| value.0).map_err(io::Error::other))
        .collect()
}

fn uri_query(packet: &Packet) -> io::Result<HashMap<String, String>> {
    let mut result = HashMap::new();
    for query in packet
        .get_options_as::<OptionValueString>(CoapOption::UriQuery)
        .unwrap_or_default()
    {
        let query = query.map_err(io::Error::other)?.0;
        let (key, value) = query.split_once('=').unwrap_or((&query, ""));
        result.insert(key.to_string(), value.to_string());
    }
    Ok(result)
}

fn path_segments(path: &Lwm2mPath) -> Vec<String> {
    let mut segments = vec![path.object_id.to_string()];
    if let Some(instance) = path.instance_id {
        segments.push(instance.to_string());
    }
    if let Some(resource) = path.resource_id {
        segments.push(resource.to_string());
    }
    if let Some(instance) = path.resource_instance_id {
        segments.push(instance.to_string());
    }
    segments
}

fn path_string(path: &Lwm2mPath) -> String {
    format!("/{}", path_segments(path).join("/"))
}

fn empty_message(message_type: MessageType, mid: u16) -> io::Result<Vec<u8>> {
    let mut packet = Packet::new();
    packet.header.set_type(message_type);
    packet.header.code = MessageClass::Empty;
    packet.header.message_id = mid;
    packet.to_bytes().map_err(io::Error::other)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retransmission_backoff_is_exponential_and_bounded() {
        let config = ServerConfig {
            ack_timeout: Duration::from_millis(20),
            max_ack_timeout: Duration::from_millis(70),
            ..ServerConfig::default()
        };
        assert_eq!(
            retransmission_timeout(&config, 0),
            Duration::from_millis(20)
        );
        assert_eq!(
            retransmission_timeout(&config, 1),
            Duration::from_millis(40)
        );
        assert_eq!(
            retransmission_timeout(&config, 2),
            Duration::from_millis(70)
        );
        assert_eq!(
            retransmission_timeout(&config, 8),
            Duration::from_millis(70)
        );
    }

    #[test]
    fn observe_sequence_uses_serial_numbers_and_the_128_second_rule() {
        let now = Instant::now();
        assert!(!is_newer_notification(1, 1, now, now));
        assert!(!is_newer_notification(2, 1, now, now));
        assert!(is_newer_notification(1, 2, now, now));
        assert!(is_newer_notification(0x00ff_ffff, 0, now, now));
        assert!(!is_newer_notification(0, 0x00ff_ffff, now, now));
        assert!(is_newer_notification(
            100,
            1,
            now - Duration::from_secs(128),
            now
        ));
    }

    #[test]
    fn request_paths_include_resource_instance() {
        let path = Lwm2mPath::parse("/3303_1.1/0/5700/2").unwrap();
        assert_eq!(path_segments(&path), ["3303", "0", "5700", "2"]);
    }
}
