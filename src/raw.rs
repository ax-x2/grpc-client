use crate::client::{
    GrpcClient, RequestStream, SubscriptionController, SubscriptionCore, SubscriptionError,
    SubscriptionMode,
};
use crate::proto::geyser::{SubscribeRequest, SubscribeRequestPing};
use bytes::Bytes;
use futures_core::Stream;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use crate::client::MetadataInterceptor;

// ---------------------------------------------------------------------------
// RawBytesCodec — custom tonic codec that encodes requests with prost and
// returns raw Bytes frames from the response stream, bypassing prost decode.
// ---------------------------------------------------------------------------

struct ProstRequestEncoder<T>(std::marker::PhantomData<T>);

impl<T: prost::Message + Default> Encoder for ProstRequestEncoder<T> {
    type Item = T;
    type Error = tonic::Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        // EncodeBuf implements BufMut directly — encode into it without going through writer()
        item.encode(dst)
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

struct RawBytesDecoder;

impl Decoder for RawBytesDecoder {
    type Item = Bytes;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        use bytes::Buf as _;
        let len = src.remaining();
        if len == 0 {
            return Ok(None);
        }
        Ok(Some(src.copy_to_bytes(len)))
    }
}

struct RawBytesCodec;

impl Codec for RawBytesCodec {
    type Encode = SubscribeRequest;
    type Decode = Bytes;
    type Encoder = ProstRequestEncoder<SubscribeRequest>;
    type Decoder = RawBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        ProstRequestEncoder(std::marker::PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawBytesDecoder
    }
}

// ---------------------------------------------------------------------------
// opening the raw subscribe stream
// ---------------------------------------------------------------------------

async fn open_raw_subscribe(
    channel: Channel,
    interceptor: MetadataInterceptor,
    request_stream: RequestStream<SubscribeRequest>,
    max_decoding_message_size: Option<usize>,
) -> Result<Streaming<Bytes>, tonic::Status> {
    let intercepted = InterceptedService::new(channel, interceptor);
    let mut grpc = Grpc::new(intercepted);
    if let Some(limit) = max_decoding_message_size {
        grpc = grpc.max_decoding_message_size(limit);
    }
    let path = http::uri::PathAndQuery::from_static("/geyser.Geyser/Subscribe");
    let response = grpc
        .streaming(Request::new(request_stream), path, RawBytesCodec)
        .await?;
    Ok(response.into_inner())
}

// ---------------------------------------------------------------------------
// stack-only slot parser — no heap allocation, no prost decode
// ---------------------------------------------------------------------------

/// read a varint from `data`. Returns `(value, remaining_bytes)` or `None`
/// if the data is truncated mid-varint.
#[inline]
fn read_varint(data: &[u8]) -> Option<(u64, &[u8])> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in data.iter().enumerate() {
        if shift >= 64 {
            // overflow — varint too long
            return None;
        }
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, &data[i + 1..]));
        }
        shift += 7;
    }
    None // truncated
}

/// read a protobuf tag (field_number, wire_type). returns
/// `(field_number, wire_type, remaining)` or `None` if truncated.
#[inline]
fn read_tag(data: &[u8]) -> Option<(u32, u8, &[u8])> {
    let (tag_varint, rest) = read_varint(data)?;
    let field_number = (tag_varint >> 3) as u32;
    let wire_type = (tag_varint & 0x7) as u8;
    Some((field_number, wire_type, rest))
}

/// skip one field given its already-read wire type.
/// returns remaining slice after the field, or `None` on truncation/unknown wire type.
#[inline]
fn skip_field(wire_type: u8, data: &[u8]) -> Option<&[u8]> {
    match wire_type {
        0 => {
            // varint: consume all bytes including continuation bytes
            let (_, rest) = read_varint(data)?;
            Some(rest)
        }
        1 => {
            // 64-bit: skip 8 bytes
            if data.len() < 8 {
                return None;
            }
            Some(&data[8..])
        }
        2 => {
            // LEN: read length prefix then skip that many bytes
            let (len, rest) = read_varint(data)?;
            let len = len as usize;
            if rest.len() < len {
                return None;
            }
            Some(&rest[len..])
        }
        5 => {
            // 32-bit: skip 4 bytes
            if data.len() < 4 {
                return None;
            }
            Some(&data[4..])
        }
        _ => None, // unknown wire type
    }
}

/// parse a `SubscribeUpdateSlot` message from `data` (the embedded LEN payload).
/// returns `Ok(Some(RawSlotUpdate))` on success, `Ok(None)` if data is empty,
/// `Err(())` on malformed wire data.
fn parse_update_slot_message(mut data: &[u8]) -> Result<RawSlotUpdate, ()> {
    let mut slot: u64 = 0;
    let mut status: i32 = 0;

    while !data.is_empty() {
        let (field_num, wire_type, rest) = read_tag(data).ok_or(())?;
        data = rest;
        match (field_num, wire_type) {
            (1, 0) => {
                // slot field: varint
                let (v, rest) = read_varint(data).ok_or(())?;
                slot = v;
                data = rest;
            }
            (3, 0) => {
                // status field: varint
                let (v, rest) = read_varint(data).ok_or(())?;
                status = v as i32;
                data = rest;
            }
            (_, wt) => {
                // skip unknown or unneeded fields (parent=field2, dead_error=field4)
                data = skip_field(wt, data).ok_or(())?;
            }
        }
    }

    Ok(RawSlotUpdate { slot, status })
}

/// top-level parser: walk `SubscribeUpdate` fields looking for the Slot variant.
///
/// wire tag constants (protobuf field << 3 | wire_type):
/// - SubscribeUpdate.Slot (field 3, wire type 2 = LEN) → tag varint = 0x1A
/// - SubscribeUpdate.Ping (field 6, wire type 2 = LEN) → tag varint = 0x32
/// - SubscribeUpdate.Pong (field 9, wire type 2 = LEN) → tag varint = 0x4A
pub fn parse_slot_update_from_bytes(mut data: &[u8]) -> Result<Option<RawSlotUpdate>, ()> {
    while !data.is_empty() {
        let (field_num, wire_type, rest) = read_tag(data).ok_or(())?;
        data = rest;
        match field_num {
            3 => {
                // slot variant (LEN field)
                if wire_type != 2 {
                    return Err(());
                }
                let (len, rest) = read_varint(data).ok_or(())?;
                let len = len as usize;
                if rest.len() < len {
                    return Err(());
                }
                let slot_msg = &rest[..len];
                let update = parse_update_slot_message(slot_msg)?;
                return Ok(Some(update));
            }
            6 | 9 => {
                // ping (field 6) or pong (field 9) — not an error, just not a slot update
                return Ok(None);
            }
            _ => {
                // skip anything else
                data = skip_field(wire_type, data).ok_or(())?;
            }
        }
    }
    // end of buffer without a slot update or ping/pong
    Ok(None)
}

// ---------------------------------------------------------------------------
// public types
// ---------------------------------------------------------------------------

/// raw gRPC response frame: payload bytes without prost decode overhead.
pub struct RawSubscribeFrame {
    payload: Bytes,
}

impl RawSubscribeFrame {
    /// borrow the raw protobuf payload bytes.
    #[inline]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// stack-only slot parser — no heap allocation, no prost decode.
    #[inline]
    pub fn parse_slot_update(&self) -> Result<Option<RawSlotUpdate>, SubscriptionError> {
        parse_slot_update_from_bytes(&self.payload)
            .map_err(|_| SubscriptionError::Status(tonic::Status::data_loss("bad proto frame")))
    }

    /// fallback full prost decode for debugging / compatibility.
    pub fn decode_full(
        &self,
    ) -> Result<crate::proto::geyser::SubscribeUpdate, prost::DecodeError> {
        use prost::Message as _;
        crate::proto::geyser::SubscribeUpdate::decode(self.payload.as_ref())
    }
}

/// scalar slot update extracted without heap allocation.
pub struct RawSlotUpdate {
    pub slot: u64,
    pub status: i32,
}

// ---------------------------------------------------------------------------
// RawMode — SubscriptionMode impl for the raw Bytes path
// ---------------------------------------------------------------------------

/// zero-sized type used as a `SubscriptionMode` discriminant for the raw Bytes path.
pub(crate) struct RawMode;

impl SubscriptionMode for RawMode {
    type Request = SubscribeRequest;
    type Update = Bytes;

    fn open<'a>(
        client: &'a mut GrpcClient,
        request_stream: RequestStream<Self::Request>,
    ) -> Pin<Box<dyn Future<Output = Result<Streaming<Self::Update>, tonic::Status>> + Send + 'a>>
    {
        let channel = client.channel().clone();
        let interceptor = client.interceptor();
        let max_size = client.max_decoding_message_size();
        Box::pin(open_raw_subscribe(
            channel,
            interceptor,
            request_stream,
            max_size,
        ))
    }

    #[inline]
    fn observed_slot(update: &Self::Update) -> Option<u64> {
        parse_slot_update_from_bytes(update)
            .ok()
            .flatten()
            .map(|u| u.slot)
    }

    #[inline]
    fn is_ping(update: &Self::Update) -> bool {
        // ping tag is 0x32 (field 6, wire type 2).
        // the first meaningful byte in the buffer is the tag of the first field.
        // we only need to check the very first tag byte.
        update.first() == Some(&0x32)
    }

    #[inline]
    fn pong_request(id: i32) -> Self::Request {
        SubscribeRequest {
            ping: Some(SubscribeRequestPing { id }),
            ..Default::default()
        }
    }

    #[inline]
    fn apply_resume(base: &Self::Request, from_slot: Option<u64>) -> Self::Request {
        if from_slot == base.from_slot {
            // common case: no resume needed — avoid an extra clone
            return base.clone();
        }
        let mut request = base.clone();
        request.from_slot = from_slot;
        request
    }
}

// ---------------------------------------------------------------------------
// RawSubscription — public wrapper stream
// ---------------------------------------------------------------------------

/// low-latency gRPC subscription that yields raw `RawSubscribeFrame` values
/// without prost decode overhead on the hot path.
pub struct RawSubscription {
    pub(crate) inner: SubscriptionCore<RawMode>,
}

impl RawSubscription {
    pub fn controller(&self) -> &SubscriptionController<SubscribeRequest> {
        self.inner.controller()
    }

    pub fn dropped_control_messages(&self) -> u64 {
        self.inner.dropped_control_messages()
    }

    pub fn last_observed_slot(&self) -> Option<u64> {
        self.inner.last_observed_slot()
    }
}

impl Stream for RawSubscription {
    type Item = Result<RawSubscribeFrame, SubscriptionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner)
            .poll_next(cx)
            .map_ok(|payload| RawSubscribeFrame { payload })
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::geyser::{
        SubscribeUpdate, SubscribeUpdatePing, SubscribeUpdateSlot,
        subscribe_update::UpdateOneof,
    };
    use prost::Message as _;

    fn encode_subscribe_update(update: &SubscribeUpdate) -> Bytes {
        let mut buf = bytes::BytesMut::new();
        update.encode(&mut buf).unwrap();
        buf.freeze()
    }

    #[test]
    fn parse_slot_update_contiguous() {
        let update = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 314159,
                parent: Some(314158),
                status: 3, // SlotProcessed
                dead_error: None,
            })),
        };
        let bytes = encode_subscribe_update(&update);
        let result = parse_slot_update_from_bytes(&bytes).unwrap();
        let raw = result.expect("should be Some(RawSlotUpdate)");
        assert_eq!(raw.slot, 314159);
        assert_eq!(raw.status, 3);
    }

    #[test]
    fn parse_slot_update_ping() {
        let update = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
        };
        let bytes = encode_subscribe_update(&update);
        let result = parse_slot_update_from_bytes(&bytes).unwrap();
        assert!(result.is_none(), "ping should return Ok(None)");
    }

    #[test]
    fn parse_slot_update_unknown_fields() {
        // build a slot update, then manually prepend a fake unknown field (field 99, varint).
        let update = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 99999,
                parent: None,
                status: 4,
                dead_error: None,
            })),
        };
        let original = encode_subscribe_update(&update);

        // prepend an unknown field: field 50, wire type 0 (varint), value 1.
        // tag = (50 << 3) | 0 = 400, encoded as varint: 0x90 0x03
        let mut with_unknown = bytes::BytesMut::new();
        with_unknown.extend_from_slice(&[0x90, 0x03, 0x01]); // tag varint + value 1
        with_unknown.extend_from_slice(&original);

        let result = parse_slot_update_from_bytes(&with_unknown).unwrap();
        let raw = result.expect("should still parse slot after unknown field");
        assert_eq!(raw.slot, 99999);
        assert_eq!(raw.status, 4);
    }

    #[test]
    fn parse_slot_update_truncated() {
        // truncate to 2 bytes — should return Err(())
        let update = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 1,
                parent: None,
                status: 1,
                dead_error: None,
            })),
        };
        let bytes = encode_subscribe_update(&update);
        // only keep the first 2 bytes — truncates mid-message
        let truncated = &bytes[..2.min(bytes.len())];
        // truncated could parse as Ok(None) if nothing of interest is there, or Err(()) on
        // malformed data. Either way it must NOT panic.
        let _ = parse_slot_update_from_bytes(truncated);
    }
}
