use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use session_protocol::SessionWireError;

pub fn session_error(error: SessionWireError) -> Response {
    let status =
        StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, Json(error)).into_response()
}
