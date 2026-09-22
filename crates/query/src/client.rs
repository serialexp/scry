//! Bounded client for the public query TCP protocol.
//!
//! The client deliberately buffers only up to [`QueryLimits::max_bytes`] and
//! [`QueryLimits::max_rows`]. All counters are cumulative across superseded
//! attempts, even though provisional result batches are discarded on reset.

use std::fmt;
use std::time::Duration;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use scry_proto::constants::QUERY_ERR_RESOURCES;
use scry_proto::framing::{write_frame, FrameError, MAX_FRAME_BYTES};
use scry_proto::{QueryFrame, QueryFrameMsg, QueryStatsOutput};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::time::{timeout, Instant};

use crate::QueryRequest;

macro_rules! number {
    ($array:expr, $ty:ty) => {
        QueryScalar::Number(
            $array
                .as_any()
                .downcast_ref::<$ty>()
                .expect("Arrow datatype/downcast invariant")
                .value(0) as f64,
        )
    };
}

/// Deadlines for one connection and query. The total deadline includes DNS,
/// connect, request write, response transfer, and Arrow decoding.
#[derive(Debug, Clone, Copy)]
pub struct QueryDeadlines {
    pub connect: Duration,
    pub write: Duration,
    pub total: Duration,
}

impl Default for QueryDeadlines {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(5),
            write: Duration::from_secs(5),
            total: Duration::from_secs(30),
        }
    }
}

/// Cumulative response limits. Prefix bytes are included in `max_bytes`.
#[derive(Debug, Clone, Copy)]
pub struct QueryLimits {
    pub max_frames: usize,
    pub max_bytes: usize,
    pub max_rows: usize,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_frames: 4_096,
            max_bytes: 64 * 1024 * 1024,
            max_rows: 1_000_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutPhase {
    Connect,
    Write,
    Total,
}

impl fmt::Display for TimeoutPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect => f.write_str("connect"),
            Self::Write => f.write_str("write"),
            Self::Total => f.write_str("total"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundKind {
    Frames,
    Bytes,
    Rows,
}

impl fmt::Display for BoundKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frames => f.write_str("frames"),
            Self::Bytes => f.write_str("bytes"),
            Self::Rows => f.write_str("rows"),
        }
    }
}

/// Failures are separated by retry/operational meaning for alert evaluators.
#[derive(Debug, Error)]
pub enum QueryClientError {
    #[error("query stream failed with code {code:#06x}: {message}")]
    Stream { code: u16, message: String },
    #[error("query service resource limit: {message}")]
    Resource { message: String },
    #[error("query transport: {0}")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("query {phase} deadline exceeded")]
    Timeout { phase: TimeoutPhase },
    #[error("query protocol violation: {0}")]
    Protocol(String),
    #[error("query {kind} bound exceeded: observed {observed}, limit {limit}")]
    Bounds {
        kind: BoundKind,
        observed: usize,
        limit: usize,
    },
}

impl QueryClientError {
    fn transport(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Transport(Box::new(error))
    }
}

/// Fully drained final attempt. Provisional batches and stats are removed when
/// `ResponseSuperseded` is received.
#[derive(Debug)]
pub struct QueryResponse {
    pub batches: Vec<RecordBatch>,
    pub total_rows: u64,
    pub stats: Option<QueryStatsOutput>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QueryScalar {
    Number(f64),
    Boolean(bool),
}

/// Scalar aggregate outcome. SQL aggregate nulls and empty results are not
/// errors: alert evaluators generally need to distinguish them as no data.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScalarOutcome {
    Value(QueryScalar),
    NoData,
}

#[derive(Debug, Clone)]
pub struct QueryWireClient {
    address: String,
    deadlines: QueryDeadlines,
    limits: QueryLimits,
}

impl QueryWireClient {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            deadlines: QueryDeadlines::default(),
            limits: QueryLimits::default(),
        }
    }

    pub fn with_deadlines(mut self, deadlines: QueryDeadlines) -> Self {
        self.deadlines = deadlines;
        self
    }

    pub fn with_limits(mut self, limits: QueryLimits) -> Self {
        self.limits = limits;
        self
    }

    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse, QueryClientError> {
        let deadline = Instant::now() + self.deadlines.total;
        match timeout(self.deadlines.total, self.query_inner(request, deadline)).await {
            Ok(result) => result,
            Err(_) => Err(QueryClientError::Timeout {
                phase: TimeoutPhase::Total,
            }),
        }
    }

    pub async fn scalar(&self, request: QueryRequest) -> Result<ScalarOutcome, QueryClientError> {
        let response = self.query(request).await?;
        scalar_from_batches(&response.batches)
    }

    async fn query_inner(
        &self,
        request: QueryRequest,
        deadline: Instant,
    ) -> Result<QueryResponse, QueryClientError> {
        let socket = timeout(self.deadlines.connect, TcpStream::connect(&self.address))
            .await
            .map_err(|_| QueryClientError::Timeout {
                phase: TimeoutPhase::Connect,
            })?
            .map_err(QueryClientError::transport)?;
        let (read, write) = socket.into_split();
        let mut read = BufReader::new(read);
        let mut write = BufWriter::new(write);
        let request = QueryFrame {
            msg: QueryFrameMsg::QueryRequest(request.to_wire().into()),
        };
        timeout(self.deadlines.write, async {
            write_frame(&mut write, &request).await?;
            write.flush().await.map_err(FrameError::Io)
        })
        .await
        .map_err(|_| QueryClientError::Timeout {
            phase: TimeoutPhase::Write,
        })?
        .map_err(QueryClientError::transport)?;

        let mut decoder = StreamDecoder::new();
        let mut batches = Vec::new();
        let mut final_rows = 0usize;
        let mut cumulative_rows = 0usize;
        let mut frames = 0usize;
        let mut bytes = 0usize;
        let mut active_attempt = 0u32;
        let mut awaiting_schema = true;
        let mut stats = None;
        // Reuse transport scratch across frames; generated decoding necessarily
        // takes ownership of variable-sized fields after this buffer is read.
        let mut frame_body = Vec::new();

        loop {
            let frame = read_bounded_frame(
                &mut read,
                &mut frame_body,
                &mut frames,
                &mut bytes,
                self.limits,
                deadline,
            )
            .await?;
            match frame.msg {
                QueryFrameMsg::SchemaMsg(schema) => {
                    if !awaiting_schema {
                        return Err(QueryClientError::Protocol(
                            "duplicate schema in one attempt".into(),
                        ));
                    }
                    decode_ipc(
                        &mut decoder,
                        schema.ipc_bytes,
                        &mut batches,
                        &mut final_rows,
                        &mut cumulative_rows,
                        self.limits.max_rows,
                    )?;
                    awaiting_schema = false;
                }
                QueryFrameMsg::BatchMsg(batch) => {
                    if awaiting_schema {
                        return Err(QueryClientError::Protocol("batch before schema".into()));
                    }
                    decode_ipc(
                        &mut decoder,
                        batch.ipc_bytes,
                        &mut batches,
                        &mut final_rows,
                        &mut cumulative_rows,
                        self.limits.max_rows,
                    )?;
                }
                QueryFrameMsg::ResponseSuperseded(reset) => {
                    if awaiting_schema
                        || reset.superseded_attempt != active_attempt
                        || reset.next_attempt != active_attempt.saturating_add(1)
                    {
                        return Err(QueryClientError::Protocol(
                            "invalid ResponseSuperseded attempt transition".into(),
                        ));
                    }
                    active_attempt = reset.next_attempt;
                    decoder = StreamDecoder::new();
                    batches.clear();
                    final_rows = 0;
                    stats = None;
                    awaiting_schema = true;
                }
                QueryFrameMsg::QueryStats(value) => stats = Some(value),
                QueryFrameMsg::EndOfStream(end) => {
                    if awaiting_schema {
                        return Err(QueryClientError::Protocol(
                            "EndOfStream before schema".into(),
                        ));
                    }
                    if end.total_rows != final_rows as u64 {
                        return Err(QueryClientError::Protocol(format!(
                            "row-count mismatch: decoded {final_rows}, server reported {}",
                            end.total_rows
                        )));
                    }
                    return Ok(QueryResponse {
                        batches,
                        total_rows: end.total_rows,
                        stats,
                    });
                }
                QueryFrameMsg::StreamError(error) if error.code == QUERY_ERR_RESOURCES => {
                    return Err(QueryClientError::Resource {
                        message: error.message,
                    });
                }
                QueryFrameMsg::StreamError(error) => {
                    return Err(QueryClientError::Stream {
                        code: error.code,
                        message: error.message,
                    });
                }
                _ => {
                    return Err(QueryClientError::Protocol(
                        "unexpected frame in data-query response".into(),
                    ));
                }
            }
        }
    }
}

async fn read_bounded_frame<R: tokio::io::AsyncRead + Unpin>(
    read: &mut R,
    body: &mut Vec<u8>,
    frames: &mut usize,
    bytes: &mut usize,
    limits: QueryLimits,
    deadline: Instant,
) -> Result<QueryFrame, QueryClientError> {
    let len = timeout_at(deadline, read.read_u32()).await? as usize;
    let next_frames = frames.saturating_add(1);
    if next_frames > limits.max_frames {
        return Err(QueryClientError::Bounds {
            kind: BoundKind::Frames,
            observed: next_frames,
            limit: limits.max_frames,
        });
    }
    if len > MAX_FRAME_BYTES {
        return Err(QueryClientError::Protocol(format!(
            "frame is {len} bytes, maximum is {MAX_FRAME_BYTES}"
        )));
    }
    let next_bytes = bytes.saturating_add(4).saturating_add(len);
    if next_bytes > limits.max_bytes {
        return Err(QueryClientError::Bounds {
            kind: BoundKind::Bytes,
            observed: next_bytes,
            limit: limits.max_bytes,
        });
    }
    body.resize(len, 0);
    timeout_at(deadline, read.read_exact(body)).await?;
    let frame = QueryFrame::decode(body)
        .map_err(|error| QueryClientError::Protocol(format!("frame decode: {error}")))?;
    *frames = next_frames;
    *bytes = next_bytes;
    Ok(frame)
}

async fn timeout_at<F, T>(deadline: Instant, future: F) -> Result<T, QueryClientError>
where
    F: std::future::Future<Output = std::io::Result<T>>,
{
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| QueryClientError::Timeout {
            phase: TimeoutPhase::Total,
        })?
        .map_err(QueryClientError::transport)
}

fn decode_ipc(
    decoder: &mut StreamDecoder,
    bytes: Vec<u8>,
    batches: &mut Vec<RecordBatch>,
    final_rows: &mut usize,
    cumulative_rows: &mut usize,
    max_rows: usize,
) -> Result<(), QueryClientError> {
    let mut buffer = Buffer::from(bytes);
    while !buffer.is_empty() {
        let before = buffer.len();
        let decoded = decoder
            .decode(&mut buffer)
            .map_err(|error| QueryClientError::Protocol(format!("Arrow IPC decode: {error}")))?;
        if buffer.len() == before {
            return Err(QueryClientError::Protocol(
                "Arrow IPC decoder made no progress".into(),
            ));
        }
        if let Some(batch) = decoded {
            let observed = cumulative_rows.saturating_add(batch.num_rows());
            if observed > max_rows {
                return Err(QueryClientError::Bounds {
                    kind: BoundKind::Rows,
                    observed,
                    limit: max_rows,
                });
            }
            *cumulative_rows = observed;
            *final_rows = final_rows.saturating_add(batch.num_rows());
            batches.push(batch);
        }
    }
    Ok(())
}

pub fn scalar_from_batches(batches: &[RecordBatch]) -> Result<ScalarOutcome, QueryClientError> {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if rows == 0 {
        return Ok(ScalarOutcome::NoData);
    }
    if rows != 1 || batches.iter().any(|batch| batch.num_columns() != 1) {
        return Err(QueryClientError::Protocol(format!(
            "scalar query requires exactly one row and one column, got {rows} row(s)"
        )));
    }
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() == 1)
        .expect("row count was checked");
    let array = batch.column(0);
    if array.is_null(0) {
        return Ok(ScalarOutcome::NoData);
    }
    let value = match array.data_type() {
        DataType::Boolean => QueryScalar::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Arrow datatype/downcast invariant")
                .value(0),
        ),
        DataType::Int8 => number!(array, Int8Array),
        DataType::Int16 => number!(array, Int16Array),
        DataType::Int32 => number!(array, Int32Array),
        DataType::Int64 => number!(array, Int64Array),
        DataType::UInt8 => number!(array, UInt8Array),
        DataType::UInt16 => number!(array, UInt16Array),
        DataType::UInt32 => number!(array, UInt32Array),
        DataType::UInt64 => number!(array, UInt64Array),
        DataType::Float32 => number!(array, Float32Array),
        DataType::Float64 => number!(array, Float64Array),
        other => {
            return Err(QueryClientError::Protocol(format!(
                "scalar column must be numeric or boolean, got {other}"
            )));
        }
    };
    if let QueryScalar::Number(number) = value {
        if !number.is_finite() {
            return Err(QueryClientError::Protocol(
                "scalar numeric value is not finite".into(),
            ));
        }
    }
    Ok(ScalarOutcome::Value(value))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int32Array, StringDictionaryBuilder};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow_ipc::writer::{write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
    use scry_proto::framing::{read_frame, write_frame};
    use scry_proto::{
        BatchMsgInput, EndOfStreamInput, QueryFrame, QueryFrameMsg, QueryStatsInput,
        ResponseSupersededInput, SchemaMsgInput, StreamErrorInput,
    };
    use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
    use tokio::net::TcpListener;

    use super::*;

    fn ipc_frames(batch: &RecordBatch) -> Vec<QueryFrame> {
        let generator = IpcDataGenerator::default();
        let options = IpcWriteOptions::default();
        let mut tracker = DictionaryTracker::new(false);
        let schema = generator.schema_to_bytes_with_dictionary_tracker(
            &batch.schema(),
            &mut tracker,
            &options,
        );
        let mut schema_bytes = Vec::new();
        write_message(&mut schema_bytes, schema, &options).unwrap();
        let mut frames = vec![QueryFrame {
            msg: QueryFrameMsg::SchemaMsg(
                SchemaMsgInput {
                    ipc_bytes: schema_bytes,
                }
                .into(),
            ),
        }];
        #[allow(deprecated)]
        let (dictionaries, batch) = generator
            .encoded_batch(batch, &mut tracker, &options)
            .unwrap();
        for encoded in dictionaries.into_iter().chain(std::iter::once(batch)) {
            let mut ipc_bytes = Vec::new();
            write_message(&mut ipc_bytes, encoded, &options).unwrap();
            frames.push(QueryFrame {
                msg: QueryFrameMsg::BatchMsg(BatchMsgInput { ipc_bytes }.into()),
            });
        }
        frames
    }

    fn number_batch(value: f64) -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Float64Array::from(vec![value])) as ArrayRef,
        )])
        .unwrap()
    }

    async fn fake_server(frames: Vec<QueryFrame>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (read, write) = socket.into_split();
            let mut read = BufReader::new(read);
            let mut write = BufWriter::new(write);
            let request: QueryFrame = read_frame(&mut read).await.unwrap();
            assert!(matches!(request.msg, QueryFrameMsg::QueryRequest(_)));
            for frame in frames {
                write_frame(&mut write, &frame).await.unwrap();
            }
            write.flush().await.unwrap();
        });
        address.to_string()
    }

    fn end(rows: u64) -> QueryFrame {
        QueryFrame {
            msg: QueryFrameMsg::EndOfStream(EndOfStreamInput { total_rows: rows }.into()),
        }
    }

    fn stats(server_total_us: u64) -> QueryFrame {
        QueryFrame {
            msg: QueryFrameMsg::QueryStats(
                QueryStatsInput {
                    server_total_us,
                    admission_wait_us: 0,
                    catalog_us: 0,
                    cache_lookup_us: 0,
                    live_fetch_us: 0,
                    register_us: 0,
                    plan_us: 0,
                    execute_us: 0,
                    serialize_us: 0,
                    write_us: 0,
                    postings_fetch_us: 0,
                    bloom_fetch_us: 0,
                    df_opening_us: 0,
                    df_scanning_us: 0,
                    df_compute_us: 0,
                    cache_hit: 0,
                    attempts: 1,
                    blocks_considered: 0,
                    blocks_scanned: 0,
                    bytes_scanned: 0,
                    node_id: String::new(),
                    live_nodes: Vec::new(),
                }
                .into(),
            ),
        }
    }

    #[tokio::test]
    async fn supersession_discards_provisional_result_and_stats() {
        let mut frames = ipc_frames(&number_batch(1.0));
        frames.push(stats(11));
        frames.push(QueryFrame {
            msg: QueryFrameMsg::ResponseSuperseded(
                ResponseSupersededInput {
                    superseded_attempt: 0,
                    next_attempt: 1,
                    reason: 1,
                }
                .into(),
            ),
        });
        frames.extend(ipc_frames(&number_batch(2.0)));
        frames.push(stats(22));
        frames.push(end(1));
        let address = fake_server(frames).await;

        let response = QueryWireClient::new(address)
            .query(QueryRequest::default())
            .await
            .unwrap();
        assert_eq!(response.stats.unwrap().server_total_us, 22);
        assert_eq!(
            scalar_from_batches(&response.batches).unwrap(),
            ScalarOutcome::Value(QueryScalar::Number(2.0))
        );
    }

    #[tokio::test]
    async fn row_limit_is_cumulative_across_supersession() {
        let mut frames = ipc_frames(&number_batch(1.0));
        frames.push(QueryFrame {
            msg: QueryFrameMsg::ResponseSuperseded(
                ResponseSupersededInput {
                    superseded_attempt: 0,
                    next_attempt: 1,
                    reason: 1,
                }
                .into(),
            ),
        });
        frames.extend(ipc_frames(&number_batch(2.0)));
        frames.push(end(1));
        let address = fake_server(frames).await;
        let limits = QueryLimits {
            max_rows: 1,
            ..QueryLimits::default()
        };

        let error = QueryWireClient::new(address)
            .with_limits(limits)
            .query(QueryRequest::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            QueryClientError::Bounds {
                kind: BoundKind::Rows,
                observed: 2,
                limit: 1
            }
        ));
    }

    #[tokio::test]
    async fn stream_and_resource_errors_are_typed() {
        for (code, resource) in [(QUERY_ERR_RESOURCES, true), (0x00ff, false)] {
            let address = fake_server(vec![QueryFrame {
                msg: QueryFrameMsg::StreamError(
                    StreamErrorInput {
                        code,
                        message: "rejected".into(),
                    }
                    .into(),
                ),
            }])
            .await;
            let error = QueryWireClient::new(address)
                .query(QueryRequest::default())
                .await
                .unwrap_err();
            assert_eq!(matches!(error, QueryClientError::Resource { .. }), resource);
            assert_eq!(matches!(error, QueryClientError::Stream { .. }), !resource);
        }
    }

    #[tokio::test]
    async fn total_deadline_is_typed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let error = QueryWireClient::new(address)
            .with_deadlines(QueryDeadlines {
                connect: Duration::from_secs(1),
                write: Duration::from_secs(1),
                total: Duration::from_millis(10),
            })
            .query(QueryRequest::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            QueryClientError::Timeout {
                phase: TimeoutPhase::Total
            }
        ));
    }

    #[tokio::test]
    async fn frame_and_byte_limits_are_enforced_before_body_allocation() {
        let address = fake_server(ipc_frames(&number_batch(1.0))).await;
        let error = QueryWireClient::new(address)
            .with_limits(QueryLimits {
                max_frames: 0,
                ..QueryLimits::default()
            })
            .query(QueryRequest::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            QueryClientError::Bounds {
                kind: BoundKind::Frames,
                ..
            }
        ));

        let address = fake_server(ipc_frames(&number_batch(1.0))).await;
        let error = QueryWireClient::new(address)
            .with_limits(QueryLimits {
                max_bytes: 4,
                ..QueryLimits::default()
            })
            .query(QueryRequest::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            QueryClientError::Bounds {
                kind: BoundKind::Bytes,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn decodes_dictionary_messages_and_record_batch() {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append("alpha").unwrap();
        let dictionary = Arc::new(builder.finish()) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![dictionary]).unwrap();
        let mut frames = ipc_frames(&batch);
        frames.push(end(1));
        let address = fake_server(frames).await;

        let response = QueryWireClient::new(address)
            .query(QueryRequest::default())
            .await
            .unwrap();
        assert_eq!(response.batches.len(), 1);
        assert_eq!(response.batches[0].num_rows(), 1);
    }

    #[test]
    fn scalar_contract_handles_no_data_and_rejects_non_finite() {
        assert_eq!(scalar_from_batches(&[]).unwrap(), ScalarOutcome::NoData);
        let null = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Float64Array::from(vec![None])) as ArrayRef,
        )])
        .unwrap();
        assert_eq!(scalar_from_batches(&[null]).unwrap(), ScalarOutcome::NoData);
        let nan = number_batch(f64::NAN);
        assert!(matches!(
            scalar_from_batches(&[nan]),
            Err(QueryClientError::Protocol(_))
        ));
        let two_rows = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )])
        .unwrap();
        assert!(matches!(
            scalar_from_batches(&[two_rows]),
            Err(QueryClientError::Protocol(_))
        ));
    }
}
