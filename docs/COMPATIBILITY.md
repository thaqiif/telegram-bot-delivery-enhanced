# Telegram Bot API Compatibility

The service accepts every outbound method represented in the committed metadata at `crates/telegram-api-meta/src/lib.rs`. The current fixture is `crates/telegram-api-meta/fixtures/bot-api-10.3.json`; `BOT_API_VERSION` identifies the reviewed Telegram Bot API release.

## Update workflow

1. Read the official Telegram Bot API changelog and method documentation for the target version.
2. Update the committed fixture and `BOT_API_VERSION` together.
3. Update `METHODS` with exact official method/parameter spelling, type, required/file flags, family, and success projection. Add request/response-only methods to `NON_BULK_METHODS` rather than pretending they are outbound fan-out methods.
4. Review each new file parameter. `is_multipart` controls the 70-second lease/request budget; missing a file flag can recover a live upload too early.
5. Review success projection (`Message`, `FirstMessage`, `None`), automatic chat migration safety, and rate-limit method family.
6. Generate a normalized snapshot to a temporary path:

   ```sh
   cargo run -p telegram-bot-api-gen -- /tmp/bot-api-normalized.json
   diff -u crates/telegram-api-meta/fixtures/bot-api-10.3.json /tmp/bot-api-normalized.json
   ```

   The generator is intentionally deterministic. Do not overwrite the canonical fixture until the diff has been reviewed.
7. Run the metadata coverage test plus the representative API fixtures (text, media, album, copy/forward, location/poll/contact, edit/delete, admin, non-chat-axis).
8. Run capped workspace verification (adjust downward on shared hosts):

   ```sh
   CARGO_BUILD_JOBS=1 cargo fmt --all --check
   CARGO_BUILD_JOBS=1 cargo clippy --workspace --all-targets --all-features -- -D warnings
   CARGO_BUILD_JOBS=1 cargo test --workspace --all-features -- --test-threads=1
   ```
9. Record the upstream Bot API version, source URL/date, generated diff, and reviewer in the release notes.

## Compatibility boundary

Metadata preserves official parameter names and determines dispatch classification/lease behavior. Telegram can add or change methods independently; unknown methods are rejected until reviewed metadata is shipped. This is deliberate: silently proxying unknown methods would bypass request validation, timeout/file classification, and rate controls.

## Known gap: late-10.3 methods awaiting official-table reconciliation

`BOT_API_VERSION = "10.3"` tracks the release dated 2026-08-24. The committed `METHODS` table carries the full 87-method surface reviewed for that release, but the late-10.3 changelog also announces new methods whose official parameter tables were not yet available to this project at review time and are therefore **not** present:

- `sendLivePhoto`, `sendMessageDraft`, `sendRichMessage`, `sendRichMessageDraft`
- `editEphemeralMessageText`, `editEphemeralMessageMedia`, `editEphemeralMessageCaption`, `editEphemeralMessageReplyMarkup`, `deleteEphemeralMessage`
- 10.3 also replaced `receiver_user_id`/`callback_query_id` with `ephemeral_message_parameters` on `sendMessage` and related send methods.

Per the boundary above, these names are rejected (`unknown bulk method`) until they are added with exact official spelling, type, requiredness, file flags, family, and success projection. Add them via the Update workflow rather than guessing, because their file/media classification directly controls the lease and request budget.
