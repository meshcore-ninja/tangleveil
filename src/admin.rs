use axum::{
    Json, Router,
    extract::{FromRequestParts, State},
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::Serialize;

use crate::{config, reload, state::AppState};

pub struct AdminAuth;

impl FromRequestParts<AppState> for AdminAuth {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let expected = state
            .admin_token
            .read()
            .expect("admin token lock poisoned");
        if !config::admin_enabled(&expected) {
            return Err(admin_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "admin token not configured",
            ));
        }

        let provided = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));

        match provided {
            Some(token) if token == expected.as_str() => Ok(AdminAuth),
            _ => Err(admin_error(
                StatusCode::UNAUTHORIZED,
                "invalid admin token",
            )),
        }
    }
}

#[derive(Debug, Serialize)]
struct AdminResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct AdminError {
    error: &'static str,
}

pub fn router() -> Router<AppState> {
    Router::new().route("/reload", post(reload_config))
}

async fn reload_config(_auth: AdminAuth, State(state): State<AppState>) -> Response {
    match reload::trigger_reload(&state).await {
        Ok(()) => Json(AdminResponse { status: "reloaded" }).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

fn admin_error(status: StatusCode, message: &'static str) -> Response {
    (status, Json(AdminError { error: message })).into_response()
}
