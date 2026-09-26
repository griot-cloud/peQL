//! Flight SQL over tonic (feature `flight`), on a listener the caller supplies.
//!
//! | Flight SQL | peQL |
//! | --- | --- |
//! | `GetFlightInfo` (statement or prepared statement), `CreatePreparedStatement` | [`Engine::check`]: the statement guard, visibility, `decide`, `guarantee`; a refusal is returned here, before any scan |
//! | `DoGet` | [`Engine::query`]; the envelope (and its signature, when the engine has a signer) is the JSON `app_metadata` of the first message |
//! | `DoPut` (bulk ingest) | [`Engine::write`] under the contract named by the ingest's `table`, by its owner only |
//!
//! Every request names its caller. By default the caller is the `x-peql-caller` header: a
//! [`parcel_runtime::Caller`] as base64 JSON ([`caller_header`] makes one). The header is
//! believed, so the listener must be reachable only by whoever authenticated the caller (the
//! host of a guest, over a Unix socket or vsock). [`FlightSql::for_caller`] instead answers
//! every request for one caller fixed when the service is built, and ignores the header.
//!
//! A ticket is the SQL itself: `DoGet` checks everything again for the caller of the `DoGet`,
//! so a ticket grants nothing on its own.
//!
//! ```no_run
//! # async fn f(engine: std::sync::Arc<peql::Engine>) -> Result<(), Box<dyn std::error::Error>> {
//! let listener = tokio::net::UnixListener::bind("/run/peql.sock")?;
//! peql::flight::serve_unix(peql::flight::FlightSql::new(engine), listener).await?;
//! # Ok(()) }
//! ```

use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandPreparedStatementQuery, CommandStatementIngest,
    CommandStatementQuery, ProstMessageExt, SqlInfo, TableExistsOption, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, IpcMessage, SchemaAsIpc, Ticket,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::error::DataFusionError;
use futures::{Stream, TryStreamExt};
use parcel_runtime::Caller;
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::transport::server::Connected;
use tonic::{Request, Response, Status};

use crate::engine::{Engine, WriteMode};
use crate::error::PeqlError;

type DoGetStream = <FlightSql as FlightService>::DoGetStream;

/// The request header that names the caller: a [`Caller`] as base64 JSON.
pub const CALLER_HEADER: &str = "x-peql-caller";

/// The value of [`CALLER_HEADER`] for a caller.
pub fn caller_header(caller: &Caller) -> String {
    BASE64.encode(serde_json::to_vec(caller).expect("a caller serialises"))
}

enum Callers {
    Header,
    Fixed(Box<Caller>),
}

/// peQL as a Flight SQL service.
pub struct FlightSql {
    engine: Arc<Engine>,
    callers: Callers,
}

impl FlightSql {
    /// Serve `engine`, taking each request's caller from its [`CALLER_HEADER`].
    pub fn new(engine: Arc<Engine>) -> FlightSql {
        FlightSql {
            engine,
            callers: Callers::Header,
        }
    }

    /// Serve `engine` to one caller, fixed now: every request is answered for them.
    pub fn for_caller(engine: Arc<Engine>, caller: Caller) -> FlightSql {
        FlightSql {
            engine,
            callers: Callers::Fixed(Box::new(caller)),
        }
    }

    /// The tonic service, to add to a server of one's own.
    pub fn into_service(self) -> FlightServiceServer<FlightSql> {
        FlightServiceServer::new(self)
    }

    fn caller(&self, metadata: &MetadataMap) -> Result<Caller, Status> {
        match &self.callers {
            Callers::Fixed(c) => Ok(c.as_ref().clone()),
            Callers::Header => {
                let value = metadata.get(CALLER_HEADER).ok_or_else(|| {
                    Status::unauthenticated(format!(
                        "no `{CALLER_HEADER}` header: every request names its caller"
                    ))
                })?;
                let bytes = BASE64.decode(value.as_bytes()).map_err(|e| {
                    Status::unauthenticated(format!("`{CALLER_HEADER}` is not base64: {e}"))
                })?;
                serde_json::from_slice(&bytes).map_err(|e| {
                    Status::unauthenticated(format!("`{CALLER_HEADER}` is not a caller: {e}"))
                })
            }
        }
    }

    /// Check the SQL for the caller and describe the answer; the ticket carries `command`.
    async fn info(
        &self,
        sql: &str,
        command: Vec<u8>,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let caller = self.caller(request.metadata())?;
        let checked = self.engine.check(sql, &caller).await.map_err(status)?;
        let info = FlightInfo::new()
            .try_with_schema(&checked.schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(command)))
            .with_descriptor(request.into_inner())
            .with_ordered(true);
        Ok(Response::new(info))
    }

    /// Run the SQL for the caller and stream the answer, the envelope first.
    async fn answer(
        &self,
        sql: &[u8],
        metadata: &MetadataMap,
    ) -> Result<Response<DoGetStream>, Status> {
        let caller = self.caller(metadata)?;
        let sql = std::str::from_utf8(sql)
            .map_err(|_| Status::invalid_argument("the ticket is not UTF-8 SQL"))?;
        let res = self.engine.query(sql, &caller).await.map_err(status)?;
        let app_metadata = serde_json::to_vec(&serde_json::json!({
            "envelope": res.envelope,
            "signature": res.signature,
        }))
        .map_err(|e| Status::internal(e.to_string()))?;
        let batches = futures::stream::iter(res.batches.into_iter().map(Ok));
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(res.schema)
            .with_metadata(Bytes::from(app_metadata))
            .build(batches)
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }
}

/// A peQL error as a gRPC status; the message is the error's own words.
pub fn status(e: PeqlError) -> Status {
    let msg = e.to_string();
    match e {
        PeqlError::UnknownContract(_) => Status::not_found(msg),
        PeqlError::Denied { .. }
        | PeqlError::Refused { .. }
        | PeqlError::GuaranteeFailed { .. }
        | PeqlError::NotServable { .. }
        | PeqlError::Ungated(_) => Status::permission_denied(msg),
        PeqlError::BudgetExhausted { .. } => Status::resource_exhausted(msg),
        PeqlError::NotWritten { .. } => Status::failed_precondition(msg),
        PeqlError::Compile(_) | PeqlError::Invalid(_) => Status::invalid_argument(msg),
        PeqlError::Signing(_) => Status::unavailable(msg),
        PeqlError::DataFusion(
            DataFusionError::Plan(_) | DataFusionError::SQL(..) | DataFusionError::SchemaError(..),
        ) => Status::invalid_argument(msg),
        PeqlError::DataFusion(_) | PeqlError::Io(_) => Status::internal(msg),
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSql {
    type FlightService = FlightSql;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = TicketStatementQuery {
            statement_handle: Bytes::from(query.query.clone().into_bytes()),
        };
        self.info(&query.query, ticket.as_any().encode_to_vec(), request)
            .await
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("the statement handle is not UTF-8 SQL"))?;
        self.info(&sql, query.as_any().encode_to_vec(), request)
            .await
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        self.answer(&ticket.statement_handle, request.metadata())
            .await
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        self.answer(&query.prepared_statement_handle, request.metadata())
            .await
    }

    /// A prepared statement is its SQL, checked now for the caller and again at each `DoGet`.
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let caller = self.caller(request.metadata())?;
        let checked = self
            .engine
            .check(&query.query, &caller)
            .await
            .map_err(status)?;
        let options = IpcWriteOptions::default();
        let IpcMessage(dataset_schema) = SchemaAsIpc::new(&checked.schema, &options)
            .try_into()
            .map_err(|e: datafusion::arrow::error::ArrowError| Status::internal(e.to_string()))?;
        let IpcMessage(parameter_schema) = SchemaAsIpc::new(&Schema::empty(), &options)
            .try_into()
            .map_err(|e: datafusion::arrow::error::ArrowError| Status::internal(e.to_string()))?;
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: Bytes::from(query.query.into_bytes()),
            dataset_schema,
            parameter_schema,
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Ok(())
    }

    /// Bulk ingest into the contract named by `table`: its owner's write, under its write plan.
    async fn do_put_statement_ingest(
        &self,
        command: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let caller = self.caller(request.metadata())?;
        if command.catalog.is_some() || command.schema.is_some() {
            return Err(Status::not_found(format!(
                "only contracts exist: `{}` takes no catalog or schema",
                command.table
            )));
        }
        let name = command.table;
        self.engine
            .authorize_write(&name, &caller)
            .map_err(status)?;
        let if_exists = command
            .table_definition_options
            .map(|o| o.if_exists())
            .unwrap_or(TableExistsOption::Unspecified);
        let mode = match if_exists {
            TableExistsOption::Replace => WriteMode::Overwrite,
            TableExistsOption::Append | TableExistsOption::Unspecified => WriteMode::Append,
            TableExistsOption::Fail => {
                return Err(Status::already_exists(format!(
                    "`{name}` is a contract, which exists before its data: ingest with append or replace"
                )));
            }
        };
        let batches: Vec<_> = FlightRecordBatchStream::new_from_flight_data(
            request.into_inner().map_err(FlightError::from),
        )
        .try_collect()
        .await
        .map_err(Status::from)?;
        let report = self
            .engine
            .write(&name, batches, mode)
            .await
            .map_err(status)?;
        Ok(report.rows_written as i64)
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Serve `service` on every connection `incoming` yields: a Unix socket, TCP, a vsock, or
/// anything else tonic can accept.
pub async fn serve<I, IO, IE>(
    service: FlightSql,
    incoming: I,
) -> Result<(), tonic::transport::Error>
where
    I: Stream<Item = Result<IO, IE>> + Send + 'static,
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Connected + Unpin + Send + 'static,
    IE: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    serve_until(service, incoming, std::future::pending()).await
}

/// [`serve`] until `shutdown` completes, then finish the requests in flight.
pub async fn serve_until<I, IO, IE, F>(
    service: FlightSql,
    incoming: I,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    I: Stream<Item = Result<IO, IE>> + Send + 'static,
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Connected + Unpin + Send + 'static,
    IE: Into<Box<dyn std::error::Error + Send + Sync>>,
    F: std::future::Future<Output = ()>,
{
    tonic::transport::Server::builder()
        .add_service(service.into_service())
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
}

/// Serve on a Unix socket.
#[cfg(unix)]
pub async fn serve_unix(
    service: FlightSql,
    listener: tokio::net::UnixListener,
) -> Result<(), tonic::transport::Error> {
    serve(
        service,
        tokio_stream::wrappers::UnixListenerStream::new(listener),
    )
    .await
}

/// Serve on TCP.
pub async fn serve_tcp(
    service: FlightSql,
    listener: tokio::net::TcpListener,
) -> Result<(), tonic::transport::Error> {
    serve(
        service,
        tokio_stream::wrappers::TcpListenerStream::new(listener),
    )
    .await
}
