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
    // Grpc::streaming calls inner.call() directly without poll_ready, so we must
    // acquire the Tower Buffer permit here first. todo
    grpc.ready()
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
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
// zero-alloc partial wire-format parser for SubscribeUpdate.block frames.
//
// block frames carry ~15–100 MB of data (thousands of txs x signatures +
// instructions + logs + inner_instructions + sol balances + rewards), but
// downstream consumers only read a tiny slice (is_vote, err presence,
// account_keys, pre/post_token_balances). full prost decode materializes the
// entire tree including ~95% of fields that are immediately dropped. this
// parser walks the wire format on demand and exposes only whats needed,
// borrowed directly from the underlying buffer - zero allocation on the
// hot path.
//
// proto wire numbers (see proto/geyser.proto, proto/solana-storage.proto):
//   SubscribeUpdate.block          = field 5 (LEN)  → tag 0x2A
//   SubscribeUpdateBlock.slot      = field 1 (varint)
//   SubscribeUpdateBlock.transactions = field 6 (LEN, repeated)
//   SubscribeUpdateTransactionInfo.is_vote     = field 2 (varint)
//   SubscribeUpdateTransactionInfo.transaction = field 3 (LEN)
//   SubscribeUpdateTransactionInfo.meta        = field 4 (LEN)
//   SubscribeUpdateTransactionInfo.index       = field 5 (varint)
//   Transaction.message                        = field 2 (LEN)
//   Message.header                             = field 1 (LEN)
//   Message.account_keys                       = field 2 (LEN, repeated bytes)
//   MessageHeader.num_required_signatures      = field 1 (varint)
//   TransactionStatusMeta.err                  = field 1 (LEN)
//   TransactionStatusMeta.pre_token_balances   = field 7 (LEN, repeated)
//   TransactionStatusMeta.post_token_balances  = field 8 (LEN, repeated)
//   TokenBalance.account_index                 = field 1 (varint)
//   TokenBalance.mint                          = field 2 (LEN, string)
//   TokenBalance.ui_token_amount               = field 3 (LEN)
//   TokenBalance.owner                         = field 4 (LEN, string)
//   UiTokenAmount.decimals                     = field 2 (varint)
//   UiTokenAmount.amount                       = field 3 (LEN, string)
// ---------------------------------------------------------------------------

/// borrowed view into a `SubscribeUpdate.block` payload.
#[derive(Clone, Copy)]
pub struct BlockView<'a> {
    pub slot: u64,
    payload: &'a [u8],
}

impl<'a> BlockView<'a> {
    #[inline]
    pub fn transactions(&self) -> TxIter<'a> {
        TxIter { cursor: self.payload }
    }
}

/// iterator over repeated `SubscribeUpdateTransactionInfo` frames in a block.
pub struct TxIter<'a> {
    cursor: &'a [u8],
}

impl<'a> Iterator for TxIter<'a> {
    type Item = TxView<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while !self.cursor.is_empty() {
            let (field_num, wire_type, rest) = read_tag(self.cursor)?;
            self.cursor = rest;
            if field_num == 6 && wire_type == 2 {
                let (len, rest) = read_varint(self.cursor)?;
                let len = len as usize;
                if rest.len() < len {
                    return None;
                }
                let tx_payload = &rest[..len];
                self.cursor = &rest[len..];
                return Some(parse_tx_view(tx_payload));
            }
            self.cursor = skip_field(wire_type, self.cursor)?;
        }
        None
    }
}

/// borrowed view over one transaction inside a block. eagerly caches the
/// handful of sub-slices and scalar fields the scan hot path reads many
/// times, so downstream iteration over `account_keys` / `pre_token_balances`
/// / `post_token_balances` does not re-walk parent wire messages.
#[derive(Clone, Copy)]
pub struct TxView<'a> {
    pub is_vote: bool,
    pub index: u64,
    /// `meta.err` presence (transaction failed on-chain). cached.
    pub err_present: bool,
    /// `message.header.num_required_signatures`. cached. 0 if absent.
    pub num_required_signatures: u32,
    /// inner payload of `Transaction.message`. empty if absent.
    message: &'a [u8],
    /// inner payload of the `TransactionStatusMeta` submessage (field 4).
    meta: &'a [u8],
}

#[inline]
fn parse_tx_view(mut data: &[u8]) -> TxView<'_> {
    let mut is_vote = false;
    let mut index: u64 = 0;
    let mut transaction: &[u8] = &[];
    let mut meta: &[u8] = &[];

    while !data.is_empty() {
        let Some((field_num, wire_type, rest)) = read_tag(data) else {
            break;
        };
        data = rest;
        match (field_num, wire_type) {
            (2, 0) => {
                let Some((v, rest)) = read_varint(data) else {
                    break;
                };
                is_vote = v != 0;
                data = rest;
            }
            (5, 0) => {
                let Some((v, rest)) = read_varint(data) else {
                    break;
                };
                index = v;
                data = rest;
            }
            (3, 2) => {
                let Some((len, rest)) = read_varint(data) else {
                    break;
                };
                let len = len as usize;
                if rest.len() < len {
                    break;
                }
                transaction = &rest[..len];
                data = &rest[len..];
            }
            (4, 2) => {
                let Some((len, rest)) = read_varint(data) else {
                    break;
                };
                let len = len as usize;
                if rest.len() < len {
                    break;
                }
                meta = &rest[..len];
                data = &rest[len..];
            }
            (_, wt) => {
                let Some(rest) = skip_field(wt, data) else {
                    break;
                };
                data = rest;
            }
        }
    }

    // resolve downstream slices once so later per-tx hot-path lookups dont
    // have to re-walk parent wire messages.
    let message = find_first_field(transaction, 2, 2).unwrap_or(&[]);
    let header = find_first_field(message, 1, 2).unwrap_or(&[]);
    let num_required_signatures = find_first_varint(header, 1).unwrap_or(0) as u32;
    // err_present: in canonical proto3 encoding, fields are emitted in
    // ascending order. err is field 1 of TransactionStatusMeta, so it
    // either appears first or not at all. peeking the first tag is O(1)
    // vs O(meta_size) for a full find-scan - and meta is large (logs,
    // inner_instructions, etc), so a full scan on every successful tx was
    // the single biggest unnecessary cost in `err_present` detection.
    let err_present = match read_tag(meta) {
        Some((1, 2, _)) => true,
        _ => false,
    };

    TxView {
        is_vote,
        index,
        err_present,
        num_required_signatures,
        message,
        meta,
    }
}

impl<'a> TxView<'a> {
    /// iterator over raw `account_keys[]` (each entry is the raw bytes of one
    /// pubkey — typically 32 bytes).
    #[inline]
    pub fn account_keys(&self) -> AccountKeyIter<'a> {
        AccountKeyIter {
            cursor: self.message,
        }
    }

    /// iterator over `meta.pre_token_balances[]`.
    #[inline]
    pub fn pre_token_balances(&self) -> TokenBalanceIter<'a> {
        TokenBalanceIter {
            cursor: self.meta,
            field: 7,
        }
    }

    /// iterator over `meta.post_token_balances[]`.
    #[inline]
    pub fn post_token_balances(&self) -> TokenBalanceIter<'a> {
        TokenBalanceIter {
            cursor: self.meta,
            field: 8,
        }
    }
}

/// iterator over `repeated bytes account_keys = 2` inside a `Message` payload.
pub struct AccountKeyIter<'a> {
    cursor: &'a [u8],
}

impl<'a> Iterator for AccountKeyIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while !self.cursor.is_empty() {
            let (field_num, wire_type, rest) = read_tag(self.cursor)?;
            self.cursor = rest;
            if field_num == 2 && wire_type == 2 {
                let (len, rest) = read_varint(self.cursor)?;
                let len = len as usize;
                if rest.len() < len {
                    return None;
                }
                let key = &rest[..len];
                self.cursor = &rest[len..];
                return Some(key);
            }
            self.cursor = skip_field(wire_type, self.cursor)?;
        }
        None
    }
}

/// iterator over one of `pre_token_balances` or `post_token_balances` (the
/// `field` discriminant selects which).
pub struct TokenBalanceIter<'a> {
    cursor: &'a [u8],
    field: u32,
}

impl<'a> Iterator for TokenBalanceIter<'a> {
    type Item = TokenBalanceView<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while !self.cursor.is_empty() {
            let (field_num, wire_type, rest) = read_tag(self.cursor)?;
            self.cursor = rest;
            if field_num == self.field && wire_type == 2 {
                let (len, rest) = read_varint(self.cursor)?;
                let len = len as usize;
                if rest.len() < len {
                    return None;
                }
                let payload = &rest[..len];
                self.cursor = &rest[len..];
                return Some(parse_token_balance(payload));
            }
            self.cursor = skip_field(wire_type, self.cursor)?;
        }
        None
    }
}

/// borrowed view over one `TokenBalance` entry. `mint`, `owner`, and
/// `amount` are proto `string` fields (bs58 or decimal ASCII); they borrow
/// directly from the wire buffer without UTF-8 validation (geyser emits
/// ASCII bs58 and decimal digits). If a caller needs to reject malformed
/// UTF-8, it can re-validate via `std::str::from_utf8`.
#[derive(Clone, Copy, Default)]
pub struct TokenBalanceView<'a> {
    pub account_index: u32,
    pub mint: &'a str,
    pub owner: &'a str,
    pub amount: &'a str,
    pub decimals: u32,
}

#[inline]
fn parse_token_balance(mut data: &[u8]) -> TokenBalanceView<'_> {
    let mut out = TokenBalanceView::default();
    while !data.is_empty() {
        let Some((field_num, wire_type, rest)) = read_tag(data) else {
            break;
        };
        data = rest;
        match (field_num, wire_type) {
            (1, 0) => {
                let Some((v, rest)) = read_varint(data) else {
                    break;
                };
                out.account_index = v as u32;
                data = rest;
            }
            (2, 2) => {
                let Some((bytes, rest)) = read_len_slice(data) else {
                    break;
                };
                out.mint = bytes_as_str(bytes);
                data = rest;
            }
            (3, 2) => {
                let Some((bytes, rest)) = read_len_slice(data) else {
                    break;
                };
                parse_ui_token_amount(bytes, &mut out);
                data = rest;
            }
            (4, 2) => {
                let Some((bytes, rest)) = read_len_slice(data) else {
                    break;
                };
                out.owner = bytes_as_str(bytes);
                data = rest;
            }
            (_, wt) => {
                let Some(rest) = skip_field(wt, data) else {
                    break;
                };
                data = rest;
            }
        }
    }
    out
}

#[inline]
fn parse_ui_token_amount<'a>(mut data: &'a [u8], out: &mut TokenBalanceView<'a>) {
    while !data.is_empty() {
        let Some((field_num, wire_type, rest)) = read_tag(data) else {
            break;
        };
        data = rest;
        match (field_num, wire_type) {
            (2, 0) => {
                let Some((v, rest)) = read_varint(data) else {
                    break;
                };
                out.decimals = v as u32;
                data = rest;
            }
            (3, 2) => {
                let Some((bytes, rest)) = read_len_slice(data) else {
                    break;
                };
                out.amount = bytes_as_str(bytes);
                data = rest;
            }
            (_, wt) => {
                let Some(rest) = skip_field(wt, data) else {
                    break;
                };
                data = rest;
            }
        }
    }
}

/// reinterpret the slice as &str. proto3 guarantees strings are valid UTF-8
/// on a compliant writer, and the geyser fields we hit (bs58 pubkeys, decimal
/// digits) are pure ASCII. we trust the wire bytes here to avoid per-call
/// validation overhead on the hot path. a malformed server could send a non-
/// UTF-8 byte sequence; if that is a concern, swap to `str::from_utf8` which
/// is still cheap.
#[inline]
fn bytes_as_str(bytes: &[u8]) -> &str {
    // safety: proto3 mandates UTF-8 strings. the alternative (from_utf8)
    // costs O(n) per field x thousands of fields per block. if geyser ever
    // violates the spec, downstream string comparisons would fail but not
    // cause UB - &str just represents bytes.
    unsafe { std::str::from_utf8_unchecked(bytes) }
}

#[inline]
fn read_len_slice(data: &[u8]) -> Option<(&[u8], &[u8])> {
    let (len, rest) = read_varint(data)?;
    let len = len as usize;
    if rest.len() < len {
        return None;
    }
    Some((&rest[..len], &rest[len..]))
}

/// find the first occurrence of a field with the given number and wire type,
/// returning its payload (for LEN) or skipping otherwise. used for singleton
/// sub-message lookups (Transaction.message, Message.header, meta.err).
#[inline]
fn find_first_field<'a>(mut data: &'a [u8], field: u32, wire_type: u8) -> Option<&'a [u8]> {
    while !data.is_empty() {
        let (fnum, wt, rest) = read_tag(data)?;
        data = rest;
        if fnum == field && wt == wire_type {
            if wire_type == 2 {
                let (payload, _) = read_len_slice(data)?;
                return Some(payload);
            }
            // non-LEN singleton lookups not currently used, but return empty
            // to signal presence.
            return Some(&[]);
        }
        data = skip_field(wt, data)?;
    }
    None
}

/// find the first occurrence of a varint field and return its value.
#[inline]
fn find_first_varint(mut data: &[u8], field: u32) -> Option<u64> {
    while !data.is_empty() {
        let (fnum, wt, rest) = read_tag(data)?;
        data = rest;
        if fnum == field && wt == 0 {
            let (v, _) = read_varint(data)?;
            return Some(v);
        }
        data = skip_field(wt, data)?;
    }
    None
}

/// top-level block parser: walks the outer `SubscribeUpdate` frame and
/// returns a `BlockView` iff the update carries a block variant. returns
/// `Ok(None)` for non-block variants (ping/pong/slot/etc). returns `Err(())`
/// on malformed wire data.
pub fn parse_block_update_from_bytes(mut data: &[u8]) -> Result<Option<BlockView<'_>>, ()> {
    while !data.is_empty() {
        let (field_num, wire_type, rest) = read_tag(data).ok_or(())?;
        data = rest;
        match field_num {
            5 => {
                // block variant — LEN field carrying SubscribeUpdateBlock
                if wire_type != 2 {
                    return Err(());
                }
                let (payload, _) = read_len_slice(data).ok_or(())?;
                // walk the block payload once to pluck `slot` and keep the
                // payload slice for lazy transaction iteration.
                let slot = find_first_varint(payload, 1).unwrap_or(0);
                return Ok(Some(BlockView { slot, payload }));
            }
            6 | 9 => {
                // ping or pong — not a block
                return Ok(None);
            }
            _ => {
                data = skip_field(wire_type, data).ok_or(())?;
            }
        }
    }
    Ok(None)
}

/// peek the block slot from a `SubscribeUpdate` wire payload without
/// materializing a `BlockView`. returns `None` for non-block variants
/// (ping/pong/slot/etc) or malformed data. ~2 varint reads per call.
/// 
pub fn parse_block_slot(mut data: &[u8]) -> Option<u64> {
    while !data.is_empty() {
        let (field_num, wire_type, rest) = read_tag(data)?;
        data = rest;
        match field_num {
            5 => {
                if wire_type != 2 {
                    return None;
                }
                let (payload, _) = read_len_slice(data)?;
                // block.slot is field 1 varint — almost always first on the wire.
                return find_first_varint(payload, 1);
            }
            6 | 9 => return None, // ping / pong
            _ => {
                data = skip_field(wire_type, data)?;
            }
        }
    }
    None
}

/// peek the `SubscribeUpdate.created_at` timestamp (field 11 - a
/// `google.protobuf.Timestamp` with `int64 seconds = 1` and
/// `int32 nanos = 2`). returns `(seconds, nanos)` or `None` if the
/// field is absent or the payload is malformed.
/// 
pub fn parse_created_at_ns(mut data: &[u8]) -> Option<(i64, i32)> {
    while !data.is_empty() {
        let (field_num, wire_type, rest) = read_tag(data)?;
        data = rest;
        if field_num == 11 && wire_type == 2 {
            let (payload, _) = read_len_slice(data)?;
            let seconds = find_first_varint(payload, 1).unwrap_or(0) as i64;
            let nanos = find_first_varint(payload, 2).unwrap_or(0) as i32;
            return Some((seconds, nanos));
        }
        data = skip_field(wire_type, data)?;
    }
    None
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
    fn parse_block_update_roundtrip() {
        use crate::proto::geyser::{SubscribeUpdateBlock, SubscribeUpdateTransactionInfo};
        use crate::proto::solana::storage::confirmed_block::{
            Message, MessageHeader, TokenBalance, Transaction, TransactionError,
            TransactionStatusMeta, UiTokenAmount,
        };

        let signer_bytes = vec![7u8; 32];
        let mint_str = "Mint1111111111111111111111111111111111111111";
        let owner_str = "Owner111111111111111111111111111111111111111";

        let tx = SubscribeUpdateTransactionInfo {
            signature: vec![1; 64],
            is_vote: false,
            transaction: Some(Transaction {
                signatures: vec![vec![9; 64]],
                message: Some(Message {
                    header: Some(MessageHeader {
                        num_required_signatures: 2,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    }),
                    account_keys: vec![signer_bytes.clone(), vec![8u8; 32]],
                    recent_blockhash: vec![0; 32],
                    instructions: vec![],
                    versioned: false,
                    address_table_lookups: vec![],
                }),
            }),
            meta: Some(TransactionStatusMeta {
                err: None,
                fee: 5000,
                pre_balances: vec![],
                post_balances: vec![],
                inner_instructions: vec![],
                inner_instructions_none: true,
                log_messages: vec![],
                log_messages_none: true,
                pre_token_balances: vec![TokenBalance {
                    account_index: 3,
                    mint: mint_str.to_owned(),
                    ui_token_amount: Some(UiTokenAmount {
                        ui_amount: 0.0,
                        decimals: 9,
                        amount: "100".to_owned(),
                        ui_amount_string: String::new(),
                    }),
                    owner: owner_str.to_owned(),
                    program_id: String::new(),
                }],
                post_token_balances: vec![TokenBalance {
                    account_index: 3,
                    mint: mint_str.to_owned(),
                    ui_token_amount: Some(UiTokenAmount {
                        ui_amount: 0.0,
                        decimals: 9,
                        amount: "60".to_owned(),
                        ui_amount_string: String::new(),
                    }),
                    owner: owner_str.to_owned(),
                    program_id: String::new(),
                }],
                rewards: vec![],
                loaded_writable_addresses: vec![],
                loaded_readonly_addresses: vec![],
                return_data: None,
                return_data_none: true,
                compute_units_consumed: None,
                cost_units: None,
            }),
            index: 42,
        };

        let failed_tx = SubscribeUpdateTransactionInfo {
            signature: vec![2; 64],
            is_vote: true,
            transaction: None,
            meta: Some(TransactionStatusMeta {
                err: Some(TransactionError { err: vec![1] }),
                ..TransactionStatusMeta::default()
            }),
            index: 43,
        };

        let block = SubscribeUpdateBlock {
            slot: 314159,
            blockhash: "abc".to_owned(),
            transactions: vec![tx, failed_tx],
            ..SubscribeUpdateBlock::default()
        };
        let update = SubscribeUpdate {
            filters: vec!["blocks".to_owned()],
            created_at: None,
            update_oneof: Some(UpdateOneof::Block(block)),
        };

        let bytes = encode_subscribe_update(&update);
        let view = parse_block_update_from_bytes(&bytes)
            .expect("parse ok")
            .expect("block variant");
        assert_eq!(view.slot, 314159);

        let txs: Vec<_> = view.transactions().collect();
        assert_eq!(txs.len(), 2);

        // first tx: non-vote, with meta/token-balances/account-keys
        assert!(!txs[0].is_vote);
        assert_eq!(txs[0].index, 42);
        assert!(!txs[0].err_present);
        assert_eq!(txs[0].num_required_signatures, 2);
        let keys: Vec<&[u8]> = txs[0].account_keys().collect();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], signer_bytes.as_slice());
        let pre: Vec<_> = txs[0].pre_token_balances().collect();
        let post: Vec<_> = txs[0].post_token_balances().collect();
        assert_eq!(pre.len(), 1);
        assert_eq!(post.len(), 1);
        assert_eq!(pre[0].account_index, 3);
        assert_eq!(pre[0].mint, mint_str);
        assert_eq!(pre[0].owner, owner_str);
        assert_eq!(pre[0].amount, "100");
        assert_eq!(pre[0].decimals, 9);
        assert_eq!(post[0].amount, "60");

        // second tx: vote + err set
        assert!(txs[1].is_vote);
        assert_eq!(txs[1].index, 43);
        assert!(txs[1].err_present);
    }

    #[test]
    fn parse_block_update_ping_returns_none() {
        let update = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
        };
        let bytes = encode_subscribe_update(&update);
        let result = parse_block_update_from_bytes(&bytes).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn parse_block_update_truncated_does_not_panic() {
        use crate::proto::geyser::SubscribeUpdateBlock;
        let block = SubscribeUpdateBlock {
            slot: 1,
            blockhash: String::new(),
            ..SubscribeUpdateBlock::default()
        };
        let update = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Block(block)),
        };
        let bytes = encode_subscribe_update(&update);
        for cut in 0..bytes.len() {
            let _ = parse_block_update_from_bytes(&bytes[..cut]);
        }
    }

    #[test]
    fn parse_block_update_empty_block() {
        use crate::proto::geyser::SubscribeUpdateBlock;
        let block = SubscribeUpdateBlock {
            slot: 77,
            blockhash: "x".to_owned(),
            ..SubscribeUpdateBlock::default()
        };
        let update = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Block(block)),
        };
        let bytes = encode_subscribe_update(&update);
        let view = parse_block_update_from_bytes(&bytes).unwrap().unwrap();
        assert_eq!(view.slot, 77);
        assert_eq!(view.transactions().count(), 0);
    }

    #[test]
    fn parse_block_slot_peek() {
        use crate::proto::geyser::SubscribeUpdateBlock;
        let block = SubscribeUpdateBlock {
            slot: 424242,
            blockhash: "h".to_owned(),
            ..SubscribeUpdateBlock::default()
        };
        let update = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Block(block)),
        };
        let bytes = encode_subscribe_update(&update);
        assert_eq!(parse_block_slot(&bytes), Some(424242));

        // non-block variant returns None
        let ping = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
        };
        let ping_bytes = encode_subscribe_update(&ping);
        assert_eq!(parse_block_slot(&ping_bytes), None);
    }

    #[test]
    fn parse_created_at_extracts_timestamp() {
        use crate::proto::geyser::SubscribeUpdateBlock;
        use prost_types::Timestamp;
        let block = SubscribeUpdateBlock {
            slot: 1,
            ..SubscribeUpdateBlock::default()
        };
        let update = SubscribeUpdate {
            filters: vec![],
            created_at: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 123_456_789,
            }),
            update_oneof: Some(UpdateOneof::Block(block)),
        };
        let bytes = encode_subscribe_update(&update);
        let (sec, nanos) = parse_created_at_ns(&bytes).unwrap();
        assert_eq!(sec, 1_700_000_000);
        assert_eq!(nanos, 123_456_789);

        // missing created_at returns None
        let without = SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
        };
        let without_bytes = encode_subscribe_update(&without);
        assert!(parse_created_at_ns(&without_bytes).is_none());
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
