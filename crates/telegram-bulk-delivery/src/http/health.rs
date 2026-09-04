use crate::store::ReadPool;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Default)]
pub struct Readiness {
    writer_alive: AtomicBool,
    disk_ok: AtomicBool,
    telegram_configured: AtomicBool,
    ingest_available: AtomicBool,
}
impl Readiness {
    pub fn ready() -> Self {
        Self {
            writer_alive: AtomicBool::new(true),
            disk_ok: AtomicBool::new(true),
            telegram_configured: AtomicBool::new(true),
            ingest_available: AtomicBool::new(true),
        }
    }
    pub fn set_writer_alive(&self, value: bool) {
        self.writer_alive.store(value, Ordering::Release);
    }
    pub fn set_disk_ok(&self, value: bool) {
        self.disk_ok.store(value, Ordering::Release);
    }
    pub fn set_telegram_configured(&self, value: bool) {
        self.telegram_configured.store(value, Ordering::Release);
    }
    pub fn set_ingest_available(&self, value: bool) {
        self.ingest_available.store(value, Ordering::Release);
    }
    fn static_ready(&self) -> bool {
        self.writer_alive.load(Ordering::Acquire)
            && self.disk_ok.load(Ordering::Acquire)
            && self.telegram_configured.load(Ordering::Acquire)
            && self.ingest_available.load(Ordering::Acquire)
    }
}
#[derive(Clone)]
pub struct HealthState {
    pub readiness: Arc<Readiness>,
    pub readers: Arc<ReadPool>,
}
pub fn router(state: HealthState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .with_state(state)
}
async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}
async fn ready(State(state): State<HealthState>) -> impl IntoResponse {
    if state.readiness.static_ready() && state.readers.ping().is_ok() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}
