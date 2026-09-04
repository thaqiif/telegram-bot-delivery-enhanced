pub mod api_model;
pub mod auth;
pub mod config;
pub mod domain;
pub mod http;
pub mod observe;
pub mod scheduler;
pub mod store;
pub mod telegram;
pub mod webhook;

pub const MIN_SQLITE_VERSION_NUMBER: i32 = 3_051_003;
const _: () = assert!(rusqlite::ffi::SQLITE_VERSION_NUMBER >= MIN_SQLITE_VERSION_NUMBER);
