use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use bytes::Bytes;

use crate::{GossipError, Result, connection_pool::LockFreeStreamHandle, framing};

#[derive(Clone)]
enum AskResponseSink {
    StreamHandle(Arc<LockFreeStreamHandle>),
    DeferredWriter(Arc<ResponseWriter>),
}

impl AskResponseSink {
    async fn send_response_bytes(&self, correlation_id: u16, payload: Bytes) -> Result<()> {
        match self {
            Self::StreamHandle(stream_handle) => {
                stream_handle
                    .send_response_auto_bytes(correlation_id, payload)
                    .await
            }
            Self::DeferredWriter(writer) => {
                writer.send_response_bytes(correlation_id, payload).await
            }
        }
    }

    fn try_send_response_bytes(&self, correlation_id: u16, payload: Bytes) -> Result<()> {
        match self {
            Self::StreamHandle(stream_handle) => {
                let header = framing::write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    payload.len(),
                );
                stream_handle
                    .write_header_and_payload_control_inline_nonblocking(header, 16, payload)
            }
            Self::DeferredWriter(writer) => writer.try_send_response_bytes(correlation_id, payload),
        }
    }

    async fn send_response_pooled(
        &self,
        correlation_id: u16,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        match self {
            Self::StreamHandle(stream_handle) => {
                let header = framing::write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    payload_len,
                );
                let prefix_len = prefix.as_ref().map(|bytes| bytes.len()).unwrap_or(0) as u8;
                stream_handle
                    .write_pooled_ask_inline(header, 16, prefix, prefix_len, payload)
                    .await
            }
            Self::DeferredWriter(writer) => {
                writer
                    .send_response_pooled(correlation_id, payload, prefix, payload_len)
                    .await
            }
        }
    }
}

enum AskContextSink<'a> {
    StreamHandle(&'a Arc<LockFreeStreamHandle>),
    DeferredWriter(&'a Arc<ResponseWriter>),
}

pub struct AskContext<'a> {
    correlation_id: u16,
    sink: AskContextSink<'a>,
}

impl<'a> AskContext<'a> {
    pub(crate) fn from_stream_handle(
        correlation_id: u16,
        stream_handle: &'a Arc<LockFreeStreamHandle>,
    ) -> Self {
        Self {
            correlation_id,
            sink: AskContextSink::StreamHandle(stream_handle),
        }
    }

    pub(crate) fn from_writer(correlation_id: u16, writer: &'a Arc<ResponseWriter>) -> Self {
        Self {
            correlation_id,
            sink: AskContextSink::DeferredWriter(writer),
        }
    }

    pub fn correlation_id(&self) -> u16 {
        self.correlation_id
    }

    pub fn responder(&self) -> AskResponder {
        match &self.sink {
            AskContextSink::StreamHandle(stream_handle) => {
                AskResponder::from_stream_handle(self.correlation_id, (*stream_handle).clone())
            }
            AskContextSink::DeferredWriter(writer) => {
                AskResponder::from_writer(self.correlation_id, (*writer).clone())
            }
        }
    }
}

#[derive(Clone)]
pub struct AskResponder {
    correlation_id: u16,
    sink: AskResponseSink,
}

impl AskResponder {
    pub(crate) fn from_stream_handle(
        correlation_id: u16,
        stream_handle: Arc<LockFreeStreamHandle>,
    ) -> Self {
        Self {
            correlation_id,
            sink: AskResponseSink::StreamHandle(stream_handle),
        }
    }

    pub(crate) fn from_writer(correlation_id: u16, writer: Arc<ResponseWriter>) -> Self {
        Self {
            correlation_id,
            sink: AskResponseSink::DeferredWriter(writer),
        }
    }

    pub fn correlation_id(&self) -> u16 {
        self.correlation_id
    }

    pub async fn reply(self, response: Bytes) -> Result<()> {
        self.sink
            .send_response_bytes(self.correlation_id, response)
            .await
    }

    pub async fn reply_bytes(self, response: Bytes) -> Result<()> {
        self.reply(response).await
    }

    pub fn try_reply_bytes(self, response: Bytes) -> Result<()> {
        self.sink
            .try_send_response_bytes(self.correlation_id, response)
    }

    pub async fn reply_typed<M>(self, value: &M) -> Result<()>
    where
        M: crate::typed::WireEncode,
    {
        let payload = crate::typed::encode_typed_pooled(value)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<M>(payload);
        self.sink
            .send_response_pooled(self.correlation_id, payload, prefix, payload_len)
            .await
    }
}

impl std::fmt::Debug for AskResponder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AskResponder")
            .field("correlation_id", &self.correlation_id)
            .finish()
    }
}

pub(crate) struct ResponseWriter {
    addr: SocketAddr,
    stream_handle: ArcSwapOption<LockFreeStreamHandle>,
}

impl ResponseWriter {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream_handle: ArcSwapOption::empty(),
        }
    }

    pub(crate) fn bind_stream_handle(&self, stream_handle: Arc<LockFreeStreamHandle>) {
        self.stream_handle.store(Some(stream_handle));
    }

    fn stream_handle(&self) -> Result<Arc<LockFreeStreamHandle>> {
        self.stream_handle.load_full().ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!("response stream handle is not available for {}", self.addr),
            ))
        })
    }

    async fn send_response_bytes(&self, correlation_id: u16, payload: Bytes) -> Result<()> {
        self.stream_handle()?
            .send_response_auto_bytes(correlation_id, payload)
            .await
    }

    fn try_send_response_bytes(&self, correlation_id: u16, payload: Bytes) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        );
        self.stream_handle()?
            .write_header_and_payload_control_inline_nonblocking(header, 16, payload)
    }

    async fn send_response_pooled(
        &self,
        correlation_id: u16,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        let stream_handle = self.stream_handle()?;
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload_len,
        );
        let prefix_len = prefix.as_ref().map(|bytes| bytes.len()).unwrap_or(0) as u8;
        stream_handle
            .write_pooled_ask_inline(header, 16, prefix, prefix_len, payload)
            .await
    }
}
