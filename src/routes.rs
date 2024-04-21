use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};

use crate::types::{RgAppState, RgJob, RgMoveJob};

pub(crate) async fn create_job_route(
    State(state): State<Arc<RgAppState>>,
    Json(job): Json<RgJob>,
) -> impl IntoResponse {
    match state.job_service.write().await.create_job(job).await {
        Ok(new_job) => Ok((StatusCode::OK, Json(new_job))),
        Err(msg) => Err((StatusCode::BAD_REQUEST, msg)),
    }
}

pub(crate) async fn get_jobs(State(state): State<Arc<RgAppState>>) -> impl IntoResponse {
    Json(state.job_service.read().await.get_jobs().await)
}

pub(crate) async fn move_job(
    State(state): State<Arc<RgAppState>>,
    Json(job): Json<RgMoveJob>,
) -> impl IntoResponse {
    let result = state.job_service.write().await.move_job(job).await;
    match result {
        Ok(job) => Ok((StatusCode::OK, Json(job))),
        Err(msg) => Err((StatusCode::BAD_REQUEST, msg)),
    }
}
